from flask import Blueprint, render_template, request, redirect, url_for, session, current_app, make_response, flash
import sqlite3, csv, io, json, zipfile
import os
import re
import logging
import unicodedata
from functools import wraps
from datetime import datetime, date
import calendar
from flask import jsonify, send_from_directory, abort
from .core.database import get_connection
from .core.logging_config import log_financial_event, log_system_event, log_user_action_event
from .db import import_excel_if_needed
from .utils.security import hash_password, is_password_hashed, verify_password
from .utils.totp import build_totp_uri, generate_totp_secret, verify_totp_code
from .utils.uploads import build_static_upload_url
from .utils.validation import safe_float, safe_int

bp = Blueprint('main', __name__)


@bp.route('/health')
def health():
    return api_response(
        {
            'ok': True,
            'status': 'healthy',
            'app': 'JAWILVIO_V10',
            'database_recovered': bool(current_app.config.get('DATABASE_RECOVERED')),
        }
    )


@bp.route('/uploads/<path:asset_path>')
def uploaded_asset(asset_path):
    uploads_root = os.path.abspath(current_app.config.get('UPLOADS_ROOT') or '')
    if not uploads_root:
        abort(404)
    normalized = asset_path.replace('\\', '/').lstrip('/')
    full_path = os.path.abspath(os.path.join(uploads_root, normalized))
    if not full_path.startswith(uploads_root + os.sep) and full_path != uploads_root:
        abort(404)
    if not os.path.exists(full_path):
        abort(404)
    return send_from_directory(uploads_root, normalized, conditional=True)

REUNION_TIPOS_VIA = [
    ('Jr.', 'Jirón'),
    ('Psje.', 'Pasaje'),
    ('Av.', 'Avenida'),
]
PERMISO_MOTIVOS = ['Personal', 'Familiar', 'Salud', 'Trabajo', 'Otros']
MULTA_ASISTENCIA_ESTADOS = {'Faltó', 'Tardanza'}
MULTA_COBRO_ESTADOS = ('Pendiente', 'Cobrada')

MONTH_LABELS = {
    '01': 'ENERO',
    '02': 'FEBRERO',
    '03': 'MARZO',
    '04': 'ABRIL',
    '05': 'MAYO',
    '06': 'JUNIO',
    '07': 'JULIO',
    '08': 'AGOSTO',
    '09': 'SETIEMBRE',
    '10': 'OCTUBRE',
    '11': 'NOVIEMBRE',
    '12': 'DICIEMBRE',
}

ROLE_VIEW_ACCESS = {
    'Administrador': {'*'},
    'Tesorero': {
        'main.dashboard', 'main.socios', 'main.socio_detalle', 'main.prestamo_excel_detalle',
        'main.historial_prestamo_detalle', 'main.historial_prestamo_comparar', 'main.prestamos',
        'main.aportaciones_mensuales', 'main.aportaciones_mensuales_imprimible', 'main.cierre_mensual', 'main.nuevos_prestamos',
        'main.estado_cuenta', 'main.estado_cuenta_imprimible', 'main.reuniones', 'main.multas_asistencia', 'main.reportes',
        'main.saldo_actual', 'main.fondo_total', 'main.reporte_mensual_csv', 'main.graficos',
        'main.pdf_estado_general', 'main.reporte_imprimible', 'main.reporte_morosos_csv',
        'main.api_socios_collection', 'main.api_socio_detail',
        'main.api_reuniones_collection', 'main.api_reunion_detail',
        'main.api_permisos_collection', 'main.api_permiso_detail',
        'main.api_caja_collection', 'main.api_caja_detail', 'main.api_caja_detail_items',
        'main.mi_cuenta',
    },
    'Secretario': {
        'main.dashboard', 'main.socios', 'main.socio_detalle', 'main.prestamo_excel_detalle',
        'main.historial_prestamo_detalle', 'main.historial_prestamo_comparar', 'main.prestamos',
        'main.estado_cuenta', 'main.estado_cuenta_imprimible', 'main.reuniones', 'main.asistencia', 'main.multas_asistencia',
        'main.reportes', 'main.saldo_actual', 'main.graficos', 'main.pdf_estado_general',
        'main.api_socios_collection', 'main.api_socio_detail',
        'main.api_reuniones_collection', 'main.api_reunion_detail',
        'main.api_permisos_collection', 'main.api_permiso_detail',
        'main.api_caja_collection', 'main.api_caja_detail', 'main.api_caja_detail_items',
        'main.reporte_imprimible', 'main.reporte_morosos_csv', 'main.mi_cuenta',
    },
    'Consulta': {
        'main.dashboard', 'main.socios', 'main.socio_detalle', 'main.prestamo_excel_detalle',
        'main.historial_prestamo_detalle', 'main.historial_prestamo_comparar', 'main.prestamos',
        'main.aportaciones_mensuales', 'main.aportaciones_mensuales_imprimible', 'main.cierre_mensual', 'main.nuevos_prestamos',
        'main.estado_cuenta', 'main.estado_cuenta_imprimible', 'main.reuniones', 'main.asistencia', 'main.multas_asistencia',
        'main.reportes', 'main.saldo_actual', 'main.fondo_total', 'main.reporte_mensual_csv',
        'main.graficos', 'main.pdf_estado_general', 'main.reporte_imprimible', 'main.reporte_morosos_csv',
        'main.api_socios_collection', 'main.api_socio_detail',
        'main.api_reuniones_collection', 'main.api_reunion_detail',
        'main.api_permisos_collection', 'main.api_permiso_detail',
        'main.api_caja_collection', 'main.api_caja_detail', 'main.api_caja_detail_items',
        'main.mi_cuenta',
    },
}

ROLE_WRITE_ACCESS = {
    'Administrador': {'*'},
    'Tesorero': {'main.cierre_mensual', 'main.nuevos_prestamos', 'main.multas_asistencia'},
    'Secretario': {
        'main.asistencia', 'main.reuniones',
        'main.api_reuniones_collection', 'main.api_reunion_detail',
        'main.api_permisos_collection', 'main.api_permiso_detail',
    },
    'Consulta': set(),
}


def connect_db():
    try:
        return get_connection()
    except Exception:
        try:
            database_name = current_app.config.get('DATABASE')
        except Exception:
            database_name = None
        log_system_event(
            'No se pudo abrir la conexión a la base de datos',
            level=logging.ERROR,
            exc_info=True,
            database=database_name,
        )
        raise


def build_branding_logo_url(documento):
    """Return the configured logo URL, falling back to the bundled default."""
    return build_static_upload_url(documento) or url_for('static', filename='img/logo_jawilvio_light.svg')


def is_api_request(endpoint=None):
    endpoint = endpoint or request.endpoint or ''
    return request.path.startswith('/api/') or endpoint.startswith('main.api_')


def api_response(payload, status=200):
    response = make_response(jsonify(payload), status)
    response.headers['Cache-Control'] = 'no-store'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


def api_error(message, status=400, **extra):
    payload = {'ok': False, 'message': message}
    payload.update(extra)
    return api_response(payload, status=status)


def api_request_data():
    if request.is_json:
        data = request.get_json(silent=True)
        return data if isinstance(data, dict) else {}
    return request.form.to_dict()


def clear_pending_two_factor_session():
    """Remove any incomplete 2FA login challenge from the session."""
    for key in (
        'pending_2fa_user_id',
        'pending_2fa_username',
        'pending_2fa_role',
        'pending_2fa_started_at',
    ):
        session.pop(key, None)


def finalize_login_session(user_row):
    """Persist the authenticated session after password and 2FA checks."""
    session['user_id'] = user_row['id']
    session['username'] = user_row['username']
    session['role'] = user_row['role']
    clear_pending_two_factor_session()

def serialize_socio_api(row):
    row_dict = dict(row)
    return {
        'id': row_dict.get('id'),
        'numero': row_dict.get('numero'),
        'nombre': row_dict.get('nombre'),
        'dni': row_dict.get('dni'),
        'foto': row_dict.get('foto'),
        'foto_url': build_static_upload_url(row_dict.get('foto')),
        'meses': row_dict.get('meses'),
        'plazo_balance': row_dict.get('plazo_balance'),
        'cuotas': row_dict.get('plazo_real') or row_dict.get('meses'),
        'fecha_prestamo': row_dict.get('fecha_prestamo'),
        'saldo_base': round(float(row_dict.get('saldo') or 0), 1),
        'saldo_actual': round(float(row_dict.get('saldo_periodo') or row_dict.get('saldo') or 0), 1),
        'mes_2026': row_dict.get('mes_2026'),
        'reunion': row_dict.get('reunion'),
        'permisos': row_dict.get('permisos'),
    }


def serialize_reunion_api(row):
    if not row:
        return None
    lugar = ' '.join(
        [parte.strip() for parte in [(row['tipo_via'] or ''), (row['direccion_reunion'] or '')] if parte and parte.strip()]
    ).strip() or '-'
    return {
        'periodo': row['periodo'],
        'socio_numero': row['socio_numero'],
        'socio_nombre': row['socio_nombre'],
        'estado': row['estado'],
        'fecha_programada': row['fecha_programada'],
        'fecha_realizada': row['fecha_realizada'],
        'tipo_via': row['tipo_via'],
        'direccion_reunion': row['direccion_reunion'],
        'lugar_reunion': lugar,
        'observacion': row['observacion'],
        'actualizado_por': row['actualizado_por'],
        'creado_en': row['creado_en'],
        'actualizado_en': row['actualizado_en'],
    }


def serialize_permiso_api(row):
    if not row:
        return None
    return {
        'id': row['id'],
        'periodo': row['periodo'],
        'socio_numero': row['socio_numero'],
        'socio_nombre': row['socio_nombre'],
        'fecha_permiso': row['fecha_permiso'],
        'motivo': row['motivo'],
        'documento': row['documento'],
        'documento_url': build_static_upload_url(row['documento']),
        'observacion': row['observacion'],
        'registrado_por': row['registrado_por'],
        'creado_en': row['creado_en'],
    }


def serialize_caja_item_api(row):
    return {
        'socio_numero': row['socio_numero'],
        'socio_nombre': row['socio_nombre'],
        'cuotas': row['cuotas'],
        'fecha_prestamo': row['fecha_prestamo'],
        'cuota_plazo': row['cuota_plazo'],
        'cuota_fecha': row['cuota_fecha'],
        'cuota_prestamo': round(float(row['cuota_prestamo'] or 0), 1),
        'cuota_interes': round(float(row['cuota_interes'] or 0), 1),
        'cuota_capital': round(float(row['cuota_capital'] or 0), 1),
        'aporte_mensual': round(float(row['aporte_mensual'] or 0), 1),
        'total_mes': round(float(row['total_mes'] or 0), 1),
        'saldo_actual': round(float(row['saldo_actual'] or 0), 1),
        'fuente_saldo': row['fuente_saldo'],
    }


def role_can_access_endpoint(role, endpoint, write=False):
    if not endpoint:
        return True
    if endpoint.startswith('static'):
        return True
    if endpoint in {'main.login', 'main.logout', 'main.index'}:
        return True
    access_map = ROLE_WRITE_ACCESS if write else ROLE_VIEW_ACCESS
    allowed = access_map.get(role or '', set())
    return '*' in allowed or endpoint in allowed


@bp.before_request
def enforce_role_permissions():
    endpoint = request.endpoint
    if not endpoint or endpoint.startswith('static'):
        return None
    if endpoint in {'main.login', 'main.logout', 'main.index'}:
        return None
    if not session.get('user_id'):
        return None

    role = session.get('role')
    try:
        with connect_db() as conn:
            user_row = conn.execute(
                "SELECT estado FROM users WHERE id=?",
                (session.get('user_id'),),
            ).fetchone()
        if user_row and user_row['estado'] == 'Suspendido':
            usuario = session.get('username', 'desconocido')
            session.clear()
            log_system_event(
                'Intento de acceso bloqueado por usuario suspendido',
                level=logging.WARNING,
                usuario=usuario,
                endpoint=endpoint,
            )
            if is_api_request(endpoint):
                return api_error('Tu usuario está suspendido. Contacta al administrador.', status=403)
            flash('Tu usuario está suspendido. Contacta al administrador.')
            try:
                log_action(usuario, 'Sesión cerrada por suspensión', 'El usuario intentó acceder estando suspendido.')
            except Exception:
                pass
            return redirect(url_for('main.login'))
    except Exception:
        pass
    write = request.method not in ('GET', 'HEAD', 'OPTIONS')
    if role_can_access_endpoint(role, endpoint, write=write):
        return None

    if write:
        log_system_event(
            'Acción bloqueada por permisos insuficientes',
            level=logging.WARNING,
            usuario=session.get('username'),
            rol=role,
            endpoint=endpoint,
            metodo=request.method,
        )
        if is_api_request(endpoint):
            return api_error('Tu rol no tiene permiso para realizar esta acción.', status=403)
        flash('Tu rol no tiene permiso para realizar esta acción.')
    else:
        log_system_event(
            'Acceso de lectura bloqueado por permisos insuficientes',
            level=logging.WARNING,
            usuario=session.get('username'),
            rol=role,
            endpoint=endpoint,
            metodo=request.method,
        )
        if is_api_request(endpoint):
            return api_error('Tu rol no tiene acceso a este módulo.', status=403)
        flash('Tu rol no tiene acceso a este módulo.')
    return redirect(url_for('main.dashboard'))


def loan_title_from_date(iso_date, fallback='-'):
    if not iso_date or len(str(iso_date)) < 7:
        return fallback
    year = str(iso_date)[:4]
    month = str(iso_date)[5:7]
    if month not in MONTH_LABELS:
        return fallback
    return f"{MONTH_LABELS[month]} {year} - PRÉSTAMO"


def loan_title_visible(iso_date, explicit_title=None, fallback='-'):
    if explicit_title and str(explicit_title).strip():
        return str(explicit_title).strip()
    return loan_title_from_date(iso_date, fallback)


def update_excel_loan_admin_metadata(
    conn,
    socio_numero,
    prestamo_excel_id,
    titulo_visible=None,
    saldo_base_ref=None,
    prestamo_adicional_ref=None,
    fecha_inicio_manual=None,
):
    prestamo = conn.execute(
        """
        SELECT *
        FROM prestamos_excel_historial
        WHERE id=? AND socio_numero=? AND COALESCE(oculto_manual, 0)=0
        """,
        (prestamo_excel_id, socio_numero),
    ).fetchone()
    if not prestamo:
        return None

    titulo_visible = (titulo_visible or '').strip() or (prestamo['titulo_manual'] or prestamo['titulo'] or None)
    fecha_inicio_resuelta = (fecha_inicio_manual or prestamo['fecha_inicio_manual'] or prestamo['fecha_inicio'] or '').strip() or None
    max_plazo = conn.execute(
        """
        SELECT COALESCE(MAX(plazo), 0)
        FROM prestamos_excel_historial_cuotas
        WHERE prestamo_excel_id=?
        """,
        (prestamo_excel_id,),
    ).fetchone()[0]
    fecha_fin_resuelta = add_months_iso(fecha_inicio_resuelta, int(max_plazo or 0)) if fecha_inicio_resuelta else prestamo['fecha_fin']
    conn.execute(
        """
        UPDATE prestamos_excel_historial
        SET titulo_manual=?,
            saldo_base_ref=COALESCE(?, saldo_base_ref),
            prestamo_adicional_ref=COALESCE(?, prestamo_adicional_ref),
            fecha_inicio_manual=?,
            fecha_fin_manual=?,
            fecha_inicio=?,
            fecha_fin=?
        WHERE id=?
        """,
        (
            titulo_visible,
            saldo_base_ref,
            prestamo_adicional_ref,
            fecha_inicio_resuelta,
            fecha_fin_resuelta,
            fecha_inicio_resuelta,
            fecha_fin_resuelta,
            prestamo_excel_id,
        ),
    )
    if fecha_inicio_resuelta:
        cuota_rows = conn.execute(
            """
            SELECT id, plazo
            FROM prestamos_excel_historial_cuotas
            WHERE prestamo_excel_id=?
            ORDER BY plazo, id
            """,
            (prestamo_excel_id,),
        ).fetchall()
        for cuota_row in cuota_rows:
            conn.execute(
                """
                UPDATE prestamos_excel_historial_cuotas
                SET fecha=?
                WHERE id=?
                """,
                (add_months_iso(fecha_inicio_resuelta, int(cuota_row['plazo'] or 0)), cuota_row['id']),
            )
    return conn.execute(
        """
        SELECT *
        FROM prestamos_excel_historial
        WHERE id=? AND socio_numero=?
        """,
        (prestamo_excel_id, socio_numero),
    ).fetchone()


def sync_saldo_history_for_socio(conn, socio_numero):
    historial_periodos = conn.execute(
        """
        SELECT periodo
        FROM saldo_historico_mensual
        WHERE socio_numero=?
        ORDER BY periodo
        """,
        (socio_numero,),
    ).fetchall()
    for row in historial_periodos:
        periodo = row['periodo']
        saldo_row = conn.execute(
            """
            SELECT saldo
            FROM cuotas
            WHERE socio_numero=?
              AND fecha IS NOT NULL
              AND fecha <= ?
              AND plazo BETWEEN 0 AND 240
            ORDER BY fecha DESC, plazo DESC
            LIMIT 1
            """,
            (socio_numero, f'{periodo}-31'),
        ).fetchone()
        if saldo_row and saldo_row['saldo'] is not None:
            conn.execute(
                """
                UPDATE saldo_historico_mensual
                SET saldo=?
                WHERE socio_numero=? AND periodo=?
                """,
                (saldo_row['saldo'], socio_numero, periodo),
            )


def recalculate_periods_from(conn, start_period=None):
    periodos = [row['periodo'] for row in conn.execute("SELECT periodo FROM periodos ORDER BY periodo").fetchall()]
    for periodo in periodos:
        if start_period and periodo < start_period:
            continue
        ensure_monthly_collections(conn, periodo)


def apply_excel_loan_as_active(
    conn,
    socio_numero,
    prestamo_excel_id,
    titulo_visible=None,
    saldo_base_ref=None,
    prestamo_adicional_ref=None,
    fecha_inicio_manual=None,
):
    prestamo = update_excel_loan_admin_metadata(
        conn,
        socio_numero,
        prestamo_excel_id,
        titulo_visible=titulo_visible,
        saldo_base_ref=saldo_base_ref,
        prestamo_adicional_ref=prestamo_adicional_ref,
        fecha_inicio_manual=fecha_inicio_manual,
    )
    if not prestamo:
        return None

    start_period_candidates = [value[:7] for value in (
        prestamo['fecha_inicio_manual'],
        prestamo['fecha_inicio'],
        fecha_inicio_manual,
    ) if value]
    conn.execute(
        "UPDATE prestamos_excel_historial SET es_activo=CASE WHEN id=? THEN 1 ELSE 0 END WHERE socio_numero=?",
        (prestamo_excel_id, socio_numero),
    )
    conn.execute("DELETE FROM cuotas WHERE socio_numero=?", (socio_numero,))
    cronograma = conn.execute(
        """
        SELECT plazo, fecha, prestamo, interes, abono_capital, cuota, saldo, hoja_origen
        FROM prestamos_excel_historial_cuotas
        WHERE prestamo_excel_id=?
        ORDER BY plazo, fecha
        """,
        (prestamo_excel_id,),
    ).fetchall()
    for row in cronograma:
        conn.execute(
            """
            INSERT INTO cuotas(socio_numero, plazo, fecha, prestamo, interes, abono_capital, cuota, saldo, hoja_origen)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                socio_numero,
                row['plazo'],
                row['fecha'],
                row['prestamo'],
                row['interes'],
                row['abono_capital'],
                row['cuota'],
                row['saldo'],
                row['hoja_origen'],
            ),
        )
    conn.execute(
        """
        UPDATE socios
        SET fecha_prestamo=?,
            plazo_balance=?,
            meses=COALESCE(meses, ?)
        WHERE numero=?
        """,
        (
            prestamo['fecha_inicio_manual'] or prestamo['fecha_inicio'],
            prestamo['plazo_total'],
            prestamo['plazo_total'],
            socio_numero,
        ),
    )
    sync_saldo_history_for_socio(conn, socio_numero)
    recalculate_periods_from(conn, min(start_period_candidates) if start_period_candidates else None)
    log_financial_event(
        'Préstamo histórico aplicado como vigente',
        socio_numero=socio_numero,
        prestamo_excel_id=prestamo_excel_id,
        titulo_visible=titulo_visible or prestamo['titulo_manual'] or prestamo['titulo'],
        fecha_inicio=prestamo['fecha_inicio_manual'] or prestamo['fecha_inicio'],
        fecha_fin=prestamo['fecha_fin_manual'] or prestamo['fecha_fin'],
        saldo_base_ref=saldo_base_ref,
        prestamo_adicional_ref=prestamo_adicional_ref,
    )
    return prestamo


def process_excel_loan_admin_update(conn, socio_numero, loan_block_id, redirect_endpoint, **redirect_values):
    """Handle the admin correction form for a historical Excel loan block."""
    titulo_visible = request.form.get('titulo_visible', '').strip()
    fecha_inicio_manual = request.form.get('fecha_inicio_manual', '').strip()
    saldo_base_ref_raw = request.form.get('saldo_base_ref', '').strip()
    prestamo_adicional_ref_raw = request.form.get('prestamo_adicional_ref', '').strip()
    submit_mode = (request.form.get('submit_mode', 'apply') or 'apply').strip()
    try:
        saldo_base_ref = round(float(saldo_base_ref_raw), 1) if saldo_base_ref_raw else None
    except Exception:
        saldo_base_ref = None
    try:
        prestamo_adicional_ref = round(float(prestamo_adicional_ref_raw), 1) if prestamo_adicional_ref_raw else None
    except Exception:
        prestamo_adicional_ref = None

    prestamo_actualizado = update_excel_loan_admin_metadata(
        conn,
        socio_numero,
        loan_block_id,
        titulo_visible=titulo_visible,
        saldo_base_ref=saldo_base_ref,
        prestamo_adicional_ref=prestamo_adicional_ref,
        fecha_inicio_manual=fecha_inicio_manual,
    )
    if not prestamo_actualizado:
        flash('No se encontró el préstamo histórico solicitado.')
        return redirect(url_for(redirect_endpoint, **redirect_values))

    if submit_mode == 'apply' or prestamo_actualizado['es_activo']:
        prestamo_aplicado = apply_excel_loan_as_active(
            conn,
            socio_numero,
            loan_block_id,
            titulo_visible=titulo_visible,
            saldo_base_ref=saldo_base_ref,
            prestamo_adicional_ref=prestamo_adicional_ref,
            fecha_inicio_manual=fecha_inicio_manual,
        )
        accion_log = 'Corrección de préstamo histórico vigente'
        detalle_log = (
            f'Socio {socio_numero}: bloque {loan_block_id} reaplicado con fecha '
            f'{fecha_inicio_manual or prestamo_aplicado["fecha_inicio_manual"] or prestamo_aplicado["fecha_inicio"]} '
            f'y título visible "{titulo_visible or prestamo_aplicado["titulo"]}".'
        )
        mensaje_flash = 'Corrección administrativa guardada y préstamo vigente recalculado correctamente.'
    else:
        prestamo_aplicado = prestamo_actualizado
        accion_log = 'Corrección administrativa de préstamo histórico'
        detalle_log = (
            f'Socio {socio_numero}: bloque {loan_block_id} actualizado con fecha '
            f'{fecha_inicio_manual or prestamo_actualizado["fecha_inicio_manual"] or prestamo_actualizado["fecha_inicio"]} '
            f'y título visible "{titulo_visible or prestamo_actualizado["titulo"]}".'
        )
        mensaje_flash = 'Corrección administrativa guardada correctamente.'
    conn.commit()
    log_action(
        session.get('username', 'admin'),
        accion_log,
        detalle_log,
    )
    flash(mensaje_flash)
    return redirect(url_for(redirect_endpoint, **redirect_values))


def create_manual_excel_loan(conn, socio_numero, socio_nombre, titulo_visible, fecha_inicio, monto, cuotas, tasa_mensual, aplicar_como_vigente=False, saldo_base_ref=None, prestamo_adicional_ref=None):
    schedule = build_new_loan_schedule(monto, cuotas, tasa_mensual, fecha_inicio)
    total_interes = round(sum((row['interes'] or 0) for row in schedule if (row['plazo'] or 0) > 0), 1)
    total_capital = round(sum((row['abono_capital'] or 0) for row in schedule if (row['plazo'] or 0) > 0), 1)
    total_pagable = round(sum((row['cuota'] or 0) for row in schedule if (row['plazo'] or 0) > 0), 1)
    bloque_orden = conn.execute(
        "SELECT COALESCE(MAX(bloque_orden), 0) + 1 FROM prestamos_excel_historial WHERE socio_numero=?",
        (socio_numero,),
    ).fetchone()[0]
    cur = conn.execute(
        """
        INSERT INTO prestamos_excel_historial(
            socio_numero, socio_nombre, bloque_orden, titulo, titulo_manual, saldo_base_ref, prestamo_adicional_ref, fecha_inicio, fecha_fin,
            monto_inicial, plazo_total, interes_total, capital_total, cuota_total,
            saldo_inicial, saldo_final, es_activo, hoja_origen
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            socio_numero,
            socio_nombre,
            bloque_orden,
            titulo_visible,
            titulo_visible,
            saldo_base_ref,
            prestamo_adicional_ref,
            fecha_inicio,
            schedule[-1]['fecha'] if schedule else fecha_inicio,
            round(monto, 1),
            cuotas,
            total_interes,
            total_capital,
            total_pagable,
            round(monto, 1),
            0.0,
            1 if aplicar_como_vigente else 0,
            'MANUAL',
        ),
    )
    prestamo_excel_id = cur.lastrowid
    for row in schedule:
        conn.execute(
            """
            INSERT INTO prestamos_excel_historial_cuotas(
                prestamo_excel_id, plazo, fecha, prestamo, interes, abono_capital, cuota, saldo, hoja_origen
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                prestamo_excel_id,
                row['plazo'],
                row['fecha'],
                row['prestamo'],
                row['interes'],
                row['abono_capital'],
                row['cuota'],
                row['saldo'],
                'MANUAL',
            ),
        )
    if aplicar_como_vigente:
        apply_excel_loan_as_active(conn, socio_numero, prestamo_excel_id, titulo_visible, saldo_base_ref, prestamo_adicional_ref)
    log_financial_event(
        'Préstamo histórico manual creado',
        socio_numero=socio_numero,
        socio_nombre=socio_nombre,
        prestamo_excel_id=prestamo_excel_id,
        titulo_visible=titulo_visible,
        fecha_inicio=fecha_inicio,
        monto=round(monto, 1),
        cuotas=cuotas,
        tasa_mensual=tasa_mensual,
        aplicar_como_vigente=aplicar_como_vigente,
        saldo_base_ref=saldo_base_ref,
        prestamo_adicional_ref=prestamo_adicional_ref,
    )
    return prestamo_excel_id


def format_period_label(periodo, fallback='-'):
    if not periodo or len(str(periodo)) < 7 or '-' not in str(periodo):
        return fallback
    year, month = str(periodo).split('-', 1)
    return f"{MONTH_LABELS.get(month, month)} {year}"


def get_period_excel_placements(conn, periodo):
    return conn.execute(
        """
        SELECT h.socio_numero,
               COALESCE(h.socio_nombre, s.nombre) as socio_nombre,
               h.fecha_inicio,
               h.monto_inicial,
               h.plazo_total,
               h.titulo
        FROM prestamos_excel_historial h
        LEFT JOIN socios s ON s.numero = h.socio_numero
        WHERE h.es_activo=1
          AND COALESCE(h.oculto_manual, 0)=0
          AND substr(h.fecha_inicio, 1, 7)=?
          AND EXISTS (
              SELECT 1
              FROM prestamos_excel_historial hx
              WHERE hx.socio_numero = h.socio_numero
                AND hx.id <> h.id
                AND COALESCE(hx.oculto_manual, 0)=0
          )
        ORDER BY h.socio_numero
        """,
        (periodo,),
    ).fetchall()


def get_period_manual_placements(conn, periodo):
    return conn.execute(
        """
        SELECT socio_numero,
               socio_nombre,
               fecha_desembolso as fecha_inicio,
               monto as monto_inicial,
               cuotas as plazo_total,
               estado
        FROM prestamos_nuevos
        WHERE periodo=?
          AND estado IN ('Reservado', 'Aprobado')
        ORDER BY socio_numero, id
        """,
        (periodo,),
    ).fetchall()


def get_period_placement_status(conn, periodo, fondo_total):
    manual_colocado = conn.execute(
        """
        SELECT COALESCE(SUM(monto), 0)
        FROM prestamos_nuevos
        WHERE periodo=? AND estado IN ('Reservado', 'Aprobado')
        """,
        (periodo,),
    ).fetchone()[0] or 0
    excel_rows = get_period_excel_placements(conn, periodo)
    manual_rows = get_period_manual_placements(conn, periodo)
    if manual_rows:
        total_colocado = round(manual_colocado, 1)
        fuente = 'prestamos_nuevos'
        detalle_rows = [
            {
                'socio_numero': row['socio_numero'],
                'socio_nombre': row['socio_nombre'],
                'fecha_inicio': row['fecha_inicio'],
                'monto_inicial': row['monto_inicial'],
                'plazo_total': row['plazo_total'],
                'origen': f"App ({row['estado']})",
            }
            for row in manual_rows
        ]
    elif excel_rows:
        total_colocado = round(fondo_total or 0, 1)
        fuente = 'excel_activo'
        detalle_rows = [
            {
                'socio_numero': row['socio_numero'],
                'socio_nombre': row['socio_nombre'],
                'fecha_inicio': row['fecha_inicio'],
                'monto_inicial': row['monto_inicial'],
                'plazo_total': row['plazo_total'],
                'origen': 'Excel',
            }
            for row in excel_rows
        ]
    else:
        total_colocado = 0
        fuente = 'ninguno'
        detalle_rows = []
    detalle_rows = sorted(detalle_rows, key=lambda row: (row['fecha_inicio'] or '', row['socio_numero'] or 0, row['origen']))
    origin_counts = {
        'excel': sum(1 for row in detalle_rows if row['origen'] == 'Excel'),
        'app_aprobado': sum(1 for row in detalle_rows if row['origen'] == 'App (Aprobado)'),
        'app_reservado': sum(1 for row in detalle_rows if row['origen'] == 'App (Reservado)'),
    }
    return {
        'total_colocado': total_colocado,
        'saldo_por_colocar': max(round((fondo_total or 0) - total_colocado, 1), 0),
        'fuente': fuente,
        'excel_rows': excel_rows,
        'manual_rows': manual_rows,
        'detalle_rows': detalle_rows,
        'origin_counts': origin_counts,
        'manual_colocado': round(manual_colocado, 1),
        'excel_detectado': round(fondo_total or 0, 1) if excel_rows else 0,
    }


def _normalize_log_text(value):
    text = str(value or '').strip().lower()
    if not text:
        return ''
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')


def _is_financial_action(action_name):
    normalized_action = _normalize_log_text(action_name)
    finance_keywords = (
        'prestamo',
        'multa',
        'cierre',
        'apertura de periodo',
        'aplicacion de sugerencia',
        'colocacion',
        'fondo',
        'caja',
    )
    return any(keyword in normalized_action for keyword in finance_keywords)


def _serialize_audit_payload(value):
    if value is None:
        return None
    if isinstance(value, sqlite3.Row):
        value = dict(value)
    elif hasattr(value, 'to_dict') and callable(getattr(value, 'to_dict')):
        value = value.to_dict()
    return json.dumps(value, ensure_ascii=False, default=str)


def log_action(
    usuario,
    accion,
    detalle='',
    categoria='usuario',
    modulo=None,
    entidad=None,
    entidad_id=None,
    periodo=None,
    antes=None,
    despues=None,
    nivel='INFO',
    metadata=None,
):
    metadata_payload = dict(metadata or {})
    try:
        metadata_payload.setdefault('endpoint', request.endpoint)
        metadata_payload.setdefault('method', request.method)
        metadata_payload.setdefault('ip', request.headers.get('X-Forwarded-For', request.remote_addr))
        metadata_payload.setdefault('user_agent', request.user_agent.string)
    except RuntimeError:
        pass

    try:
        with connect_db() as conn:
            conn.execute(
                """
                INSERT INTO auditoria(
                    usuario, accion, detalle, categoria, modulo, entidad, entidad_id, periodo,
                    nivel, antes_json, despues_json, metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    usuario,
                    accion,
                    str(detalle or ''),
                    categoria,
                    modulo,
                    entidad,
                    str(entidad_id) if entidad_id is not None else None,
                    periodo,
                    str(nivel or 'INFO').upper(),
                    _serialize_audit_payload(antes),
                    _serialize_audit_payload(despues),
                    _serialize_audit_payload(metadata_payload) if metadata_payload else None,
                ),
            )
            conn.commit()
    except Exception:
        log_system_event(
            'No se pudo registrar la auditoría en base de datos',
            level=logging.ERROR,
            exc_info=True,
            usuario=usuario,
            accion=accion,
            detalle=detalle,
            categoria=categoria,
            modulo=modulo,
            entidad=entidad,
            entidad_id=entidad_id,
            periodo=periodo,
        )

    log_user_action_event(
        accion,
        usuario=usuario,
        detalle=detalle,
        categoria=categoria,
        modulo=modulo,
        entidad=entidad,
        entidad_id=entidad_id,
        periodo=periodo,
    )

    if categoria == 'finanzas' or _is_financial_action(accion):
        log_financial_event(
            accion,
            usuario=usuario,
            detalle=detalle,
            categoria=categoria,
            modulo=modulo,
            entidad=entidad,
            entidad_id=entidad_id,
            periodo=periodo,
        )


def get_default_period(conn):
    return datetime.now().strftime('%Y-%m')


def get_period_row(conn, periodo):
    return conn.execute("SELECT * FROM periodos WHERE periodo=?", (periodo,)).fetchone()


def is_period_closed(period_row):
    return bool(period_row and str(period_row['estado'] or '').strip() == 'Cerrado')


def get_period_write_lock_message(periodo):
    return f'El período {periodo} ya está cerrado y no admite cambios operativos.'


def ensure_period_is_writable(conn, periodo):
    period_row = get_period_row(conn, periodo)
    if is_period_closed(period_row):
        return False, get_period_write_lock_message(periodo), period_row
    return True, None, period_row


def get_next_period(periodo):
    year, month = map(int, periodo.split('-'))
    month += 1
    if month > 12:
        month = 1
        year += 1
    return f'{year:04d}-{month:02d}'


def get_aportaciones_period_context(conn, periodo):
    ensure_monthly_collections(conn, periodo)
    rows = conn.execute(
        """
        SELECT *
        FROM obligaciones_mensuales
        WHERE periodo=?
        ORDER BY socio_numero
        """,
        (periodo,),
    ).fetchall()
    cierre = conn.execute(
        "SELECT * FROM periodos WHERE periodo=?",
        (periodo,),
    ).fetchone()
    periodos = conn.execute(
        """
        SELECT periodo FROM periodos
        ORDER BY periodo DESC
        """
    ).fetchall()
    periodo_historico_oficial = str(get_config_value(conn, 'resumen_financiero_periodo_oficial', '2026-03') or '2026-03')
    siguiente_periodo_operativo = get_next_period(periodo_historico_oficial)
    total_mensual = float(cierre['total_recaudado'] or 0) if cierre else 0
    total_prestamos = float(cierre['total_prestamos'] or 0) if cierre else 0
    total_aportes = float(cierre['total_aportes'] or 0) if cierre else 0
    resumen_aportaciones = {
        'socios_con_aporte': len(rows),
        'aporte_base': float(cierre['aporte_mensual_base'] or 0) if cierre else 0,
        'porcentaje_prestamo': round((total_prestamos / total_mensual) * 100, 1) if total_mensual > 0 else 0,
        'porcentaje_aporte': round((total_aportes / total_mensual) * 100, 1) if total_mensual > 0 else 0,
        'promedio_total_socio': round((total_mensual / len(rows)), 1) if rows else 0,
    }
    return {
        'periodo': periodo,
        'filas': rows,
        'cierre': cierre,
        'periodos': periodos,
        'resumen_aportaciones': resumen_aportaciones,
        'periodo_historico_oficial': periodo_historico_oficial,
        'siguiente_periodo_operativo': siguiente_periodo_operativo,
        'es_periodo_historico_oficial': periodo == periodo_historico_oficial,
    }


def ensure_monthly_collections(conn, periodo):
    cfg = {r['clave']: r['valor'] for r in conn.execute("SELECT clave, valor FROM configuracion").fetchall()}
    try:
        aporte_mensual = float(cfg.get('aporte_mensual', 150) or 150)
    except Exception:
        aporte_mensual = 150.0

    periodo_existente = get_period_row(conn, periodo)
    obligaciones_existentes = conn.execute(
        "SELECT COUNT(*) FROM obligaciones_mensuales WHERE periodo=?",
        (periodo,),
    ).fetchone()[0]
    if is_period_closed(periodo_existente) and obligaciones_existentes > 0:
        return

    conn.execute(
        """
        INSERT OR IGNORE INTO periodos(periodo, estado, origen, aporte_mensual_base, fecha_apertura, fecha_calculo)
        VALUES(?, 'Abierto', 'app', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (periodo, aporte_mensual),
    )

    socios = conn.execute(
        """
        SELECT s.*,
               COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real,
               MIN(CASE WHEN c.plazo = 0 THEN c.fecha END) as fecha_prestamo_activa
        FROM socios s
        LEFT JOIN cuotas c ON c.socio_numero = s.numero
        GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
        ORDER BY s.numero
        """
    ).fetchall()

    conn.execute("DELETE FROM obligaciones_mensuales WHERE periodo=?", (periodo,))
    conn.execute("DELETE FROM aportaciones_mensuales WHERE periodo=?", (periodo,))

    for socio in socios:
        cuotas_valor = socio['plazo_real'] or socio['plazo_balance'] or socio['meses'] or 0
        fecha_prestamo_operativa = socio['fecha_prestamo_activa'] or socio['fecha_prestamo']
        cuota_row = conn.execute(
            """
            SELECT plazo, fecha, cuota, interes, abono_capital, saldo
            FROM cuotas
            WHERE socio_numero=? AND substr(fecha, 1, 7)=?
              AND plazo BETWEEN 1 AND 240
            ORDER BY fecha ASC, plazo ASC
            LIMIT 1
            """,
            (socio['numero'], periodo),
        ).fetchone()
        if not cuota_row:
            cuota_row = conn.execute(
                """
                SELECT hc.plazo, hc.fecha, hc.cuota, hc.interes, hc.abono_capital, hc.saldo
                FROM historial_prestamos_socios_cuotas hc
                JOIN historial_prestamos_socios h ON h.id = hc.historial_id
                WHERE h.socio_numero=?
                  AND substr(hc.fecha, 1, 7)=?
                  AND hc.plazo BETWEEN 1 AND 240
                ORDER BY h.id DESC, hc.fecha ASC, hc.plazo ASC
                LIMIT 1
                """,
                (socio['numero'], periodo),
            ).fetchone()
        if not cuota_row:
            cuota_row = conn.execute(
                """
                SELECT ec.plazo, ec.fecha, ec.cuota, ec.interes, ec.abono_capital, ec.saldo
                FROM prestamos_excel_historial_cuotas ec
                JOIN prestamos_excel_historial eh ON eh.id = ec.prestamo_excel_id
                WHERE eh.socio_numero=?
                  AND eh.es_activo=1
                  AND COALESCE(eh.oculto_manual, 0)=0
                  AND substr(ec.fecha, 1, 7)=?
                  AND ec.plazo BETWEEN 1 AND 240
                ORDER BY date(eh.fecha_inicio) DESC, eh.bloque_orden DESC, ec.fecha ASC, ec.plazo ASC
                LIMIT 1
                """,
                (socio['numero'], periodo),
            ).fetchone()
        if not cuota_row:
            rollover_row = conn.execute(
                """
                SELECT ec.plazo,
                       ec.fecha,
                       ec.cuota,
                       ec.interes,
                       ec.abono_capital,
                       ec.saldo,
                       eh.fecha_inicio as fecha_prestamo_anterior,
                       eh.plazo_total as cuotas_anteriores
                FROM prestamos_excel_historial_cuotas ec
                JOIN prestamos_excel_historial eh ON eh.id = ec.prestamo_excel_id
                WHERE eh.socio_numero=?
                  AND eh.es_activo=0
                  AND COALESCE(eh.oculto_manual, 0)=0
                  AND substr(ec.fecha, 1, 7)=?
                  AND ec.plazo BETWEEN 1 AND 240
                  AND EXISTS (
                      SELECT 1
                      FROM prestamos_excel_historial act
                      WHERE act.socio_numero=eh.socio_numero
                        AND act.es_activo=1
                        AND COALESCE(act.oculto_manual, 0)=0
                        AND substr(act.fecha_inicio, 1, 7)=?
                  )
                ORDER BY date(eh.fecha_inicio) DESC, eh.bloque_orden DESC, ec.fecha ASC, ec.plazo ASC
                LIMIT 1
                """,
                (socio['numero'], periodo, periodo),
            ).fetchone()
            if rollover_row:
                cuota_row = rollover_row
                fecha_prestamo_operativa = rollover_row['fecha_prestamo_anterior'] or fecha_prestamo_operativa
                cuotas_valor = rollover_row['cuotas_anteriores'] or cuotas_valor
        saldo_row = conn.execute(
            """
            SELECT saldo
            FROM cuotas
            WHERE socio_numero=?
              AND fecha IS NOT NULL
              AND fecha <= ?
              AND plazo BETWEEN 0 AND 240
            ORDER BY fecha DESC, plazo DESC
            LIMIT 1
            """,
            (socio['numero'], f'{periodo}-31'),
        ).fetchone()

        cuota_prestamo = cuota_row['cuota'] if cuota_row and cuota_row['cuota'] is not None else 0
        cuota_interes = cuota_row['interes'] if cuota_row and cuota_row['interes'] is not None else 0
        cuota_capital = cuota_row['abono_capital'] if cuota_row and cuota_row['abono_capital'] is not None else 0
        saldo_actual = saldo_row['saldo'] if saldo_row and saldo_row['saldo'] is not None else (socio['saldo'] or 0)
        total_mes = cuota_prestamo + aporte_mensual

        conn.execute(
            """
            INSERT OR REPLACE INTO obligaciones_mensuales(
                periodo, socio_numero, socio_nombre, cuotas, fecha_prestamo, cuota_plazo, cuota_fecha,
                cuota_prestamo, cuota_interes, cuota_capital, aporte_mensual, total_mes, saldo_actual, fuente_saldo
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                periodo,
                socio['numero'],
                socio['nombre'],
                cuotas_valor,
                fecha_prestamo_operativa,
                cuota_row['plazo'] if cuota_row else None,
                cuota_row['fecha'] if cuota_row else None,
                cuota_prestamo,
                cuota_interes,
                cuota_capital,
                aporte_mensual,
                total_mes,
                saldo_actual,
                'base_datos',
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO aportaciones_mensuales(
                periodo, socio_numero, socio_nombre, cuotas, fecha_prestamo, cuota_plazo, cuota_fecha,
                cuota_prestamo, cuota_interes, cuota_capital, aporte_mensual, total_mes, saldo_actual, fuente_saldo
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                periodo,
                socio['numero'],
                socio['nombre'],
                cuotas_valor,
                fecha_prestamo_operativa,
                cuota_row['plazo'] if cuota_row else None,
                cuota_row['fecha'] if cuota_row else None,
                cuota_prestamo,
                cuota_interes,
                cuota_capital,
                aporte_mensual,
                total_mes,
                saldo_actual,
                'base_datos',
            ),
        )

    overrides = conn.execute(
        """
        SELECT socio_numero, cuota_prestamo, fecha_prestamo
        FROM obligaciones_mensuales_override
        WHERE periodo=?
        """,
        (periodo,),
    ).fetchall()
    for override in overrides:
        row = conn.execute(
            """
            SELECT aporte_mensual
            FROM obligaciones_mensuales
            WHERE periodo=? AND socio_numero=?
            """,
            (periodo, override['socio_numero']),
        ).fetchone()
        aporte_override = row['aporte_mensual'] if row else aporte_mensual
        total_override = round((override['cuota_prestamo'] or 0) + (aporte_override or 0), 1)
        conn.execute(
            """
            UPDATE obligaciones_mensuales
            SET cuota_prestamo=?,
                total_mes=?,
                fecha_prestamo=COALESCE(?, fecha_prestamo)
            WHERE periodo=? AND socio_numero=?
            """,
            (
                override['cuota_prestamo'] or 0,
                total_override,
                override['fecha_prestamo'],
                periodo,
                override['socio_numero'],
            ),
        )
        conn.execute(
            """
            UPDATE aportaciones_mensuales
            SET cuota_prestamo=?,
                total_mes=?,
                fecha_prestamo=COALESCE(?, fecha_prestamo)
            WHERE periodo=? AND socio_numero=?
            """,
            (
                override['cuota_prestamo'] or 0,
                total_override,
                override['fecha_prestamo'],
                periodo,
                override['socio_numero'],
            ),
        )

    cierre = conn.execute(
        """
        SELECT COUNT(*) as total_socios,
               COALESCE(SUM(cuota_prestamo), 0) as total_prestamos,
               COALESCE(SUM(aporte_mensual), 0) as total_aportes,
               COALESCE(SUM(total_mes), 0) as total_recaudado,
               COALESCE(SUM(cuota_interes), 0) as total_intereses,
               COALESCE(SUM(cuota_capital), 0) as total_capital
        FROM obligaciones_mensuales
        WHERE periodo=?
        """,
        (periodo,),
    ).fetchone()
    colocacion = get_period_placement_status(conn, periodo, cierre['total_recaudado'] if cierre else 0)
    periodo_actual = conn.execute(
        "SELECT estado FROM periodos WHERE periodo=?",
        (periodo,),
    ).fetchone()
    if periodo_actual and periodo_actual['estado'] == 'Cerrado':
        estado_periodo = 'Cerrado'
    else:
        estado_periodo = 'Colocado' if (colocacion['saldo_por_colocar'] or 0) <= 0.0001 else 'Abierto'
    conn.execute(
        """
        UPDATE periodos
        SET estado=?,
            origen='app',
            total_socios=?,
            total_prestamos=?,
            total_aportes=?,
            total_recaudado=?,
            total_intereses=?,
            total_capital=?,
            total_colocado=?,
            saldo_por_colocar=?,
            aporte_mensual_base=?,
            fecha_calculo=CURRENT_TIMESTAMP
        WHERE periodo=?
        """,
        (
            estado_periodo,
            cierre['total_socios'] or 0,
            cierre['total_prestamos'] or 0,
            cierre['total_aportes'] or 0,
            cierre['total_recaudado'] or 0,
            cierre['total_intereses'] or 0,
            cierre['total_capital'] or 0,
            colocacion['total_colocado'] or 0,
            colocacion['saldo_por_colocar'] or 0,
            aporte_mensual,
            periodo,
        ),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO cierre_mensual(
            periodo, total_socios, total_prestamos, total_aportes, total_recaudado,
            total_intereses, total_capital, aporte_mensual_base, fecha_calculo
        ) VALUES(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        """,
        (
            periodo,
            cierre['total_socios'] or 0,
            cierre['total_prestamos'] or 0,
            cierre['total_aportes'] or 0,
            cierre['total_recaudado'] or 0,
            cierre['total_intereses'] or 0,
            cierre['total_capital'] or 0,
            aporte_mensual,
        ),
    )
    if estado_periodo in ('Colocado', 'Cerrado'):
        sync_dashboard_financial_snapshot(conn, periodo)
    conn.commit()


def get_config_value(conn, key, default_value):
    row = conn.execute("SELECT valor FROM configuracion WHERE clave=?", (key,)).fetchone()
    return row['valor'] if row and row['valor'] not in (None, '') else default_value


def get_float_config_value(conn, key, default_value=0.0):
    try:
        return float(get_config_value(conn, key, default_value) or default_value)
    except Exception:
        return float(default_value or 0)


def normalize_attendance_state(value):
    text = (value or '').strip()
    replacements = {
        'AsistiÃ³': 'Asistió',
        'FaltÃ³': 'Faltó',
    }
    text = replacements.get(text, text)
    text_upper = text.upper()
    if text_upper.startswith('ASIST'):
        return 'Asistió'
    if text_upper.startswith('FALT'):
        return 'Faltó'
    if text_upper.startswith('TARD'):
        return 'Tardanza'
    if text_upper.startswith('PERMI'):
        return 'Permiso'
    return text


def get_latest_attendance_rows_for_period(conn, periodo):
    return conn.execute(
        """
        WITH asistencia_ultima AS (
            SELECT a.*
            FROM asistencia a
            JOIN (
                SELECT socio_numero, fecha, MAX(id) AS max_id
                FROM asistencia
                WHERE fecha IS NOT NULL
                  AND substr(fecha, 1, 7)=?
                GROUP BY socio_numero, fecha
            ) ult
              ON ult.max_id = a.id
        )
        SELECT *
        FROM asistencia_ultima
        ORDER BY fecha DESC, socio_numero ASC
        """,
        (periodo,),
    ).fetchall()


def sync_attendance_fines_for_period(conn, periodo):
    multa_inasistencia = round(get_float_config_value(conn, 'multa_inasistencia', 10), 1)
    multa_tardanza = round(get_float_config_value(conn, 'multa_tardanza', 5), 1)
    period_row = get_period_row(conn, periodo)
    if is_period_closed(period_row):
        return {
            'multa_inasistencia': multa_inasistencia,
            'multa_tardanza': multa_tardanza,
            'locked': True,
        }
    socios_rows = conn.execute("SELECT numero, nombre FROM socios ORDER BY numero").fetchall()
    latest_rows = get_latest_attendance_rows_for_period(conn, periodo)
    counters = {
        row['numero']: {
            'socio_nombre': row['nombre'],
            'inasistencias': 0,
            'tardanzas': 0,
        }
        for row in socios_rows
    }
    for row in latest_rows:
        socio_numero = row['socio_numero']
        if socio_numero not in counters:
            continue
        state = normalize_attendance_state(row['estado'])
        if state == 'Faltó':
            counters[socio_numero]['inasistencias'] += 1
        elif state == 'Tardanza':
            counters[socio_numero]['tardanzas'] += 1

    for socio_numero, row in counters.items():
        inasistencias = int(row['inasistencias'] or 0)
        tardanzas = int(row['tardanzas'] or 0)
        total_multa = round((inasistencias * multa_inasistencia) + (tardanzas * multa_tardanza), 1)
        existente = conn.execute(
            """
            SELECT id, estado_cobro, fecha_cobro, observacion, edicion_manual, oculto_manual
            FROM multas_asistencia
            WHERE periodo=? AND socio_numero=?
            """,
            (periodo, socio_numero),
        ).fetchone()
        if existente and int(existente['oculto_manual'] or 0) == 1:
            continue
        if existente and int(existente['edicion_manual'] or 0) == 1:
            conn.execute(
                """
                UPDATE multas_asistencia
                SET socio_nombre=?,
                    calculado_en=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (row['socio_nombre'], existente['id']),
            )
            continue
        if total_multa <= 0:
            if existente:
                conn.execute("DELETE FROM multas_asistencia WHERE id=?", (existente['id'],))
            continue
        if existente:
            conn.execute(
                """
                UPDATE multas_asistencia
                SET socio_nombre=?,
                    inasistencias=?,
                    tardanzas=?,
                    monto_multa_inasistencia=?,
                    monto_multa_tardanza=?,
                    total_multa=?,
                    edicion_manual=0,
                    oculto_manual=0,
                    calculado_en=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (
                    row['socio_nombre'],
                    inasistencias,
                    tardanzas,
                    multa_inasistencia,
                    multa_tardanza,
                    total_multa,
                    existente['id'],
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO multas_asistencia(
                    periodo, socio_numero, socio_nombre, inasistencias, tardanzas,
                    monto_multa_inasistencia, monto_multa_tardanza, total_multa,
                    estado_cobro, fecha_cobro, observacion, edicion_manual, oculto_manual
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    periodo,
                    socio_numero,
                    row['socio_nombre'],
                    inasistencias,
                    tardanzas,
                    multa_inasistencia,
                    multa_tardanza,
                    total_multa,
                    'Pendiente',
                    None,
                    None,
                    0,
                    0,
                ),
            )
    conn.commit()
    return {
        'multa_inasistencia': multa_inasistencia,
        'multa_tardanza': multa_tardanza,
        'locked': False,
    }


def get_latest_financial_period(conn):
    row = conn.execute(
        """
        SELECT periodo
        FROM periodos
        WHERE estado IN ('Colocado', 'Cerrado')
        ORDER BY periodo DESC
        LIMIT 1
        """
    ).fetchone()
    return row['periodo'] if row else None


def get_financial_snapshot_for_period(conn, periodo):
    if not periodo:
        return None
    saldo_row = conn.execute(
        """
        SELECT COUNT(*) as total, ROUND(COALESCE(SUM(saldo), 0), 1) as saldo_total
        FROM saldo_historico_mensual
        WHERE periodo=?
        """,
        (periodo,),
    ).fetchone()
    if saldo_row and (saldo_row['total'] or 0) > 0:
        saldo_total = float(saldo_row['saldo_total'] or 0)
        total_socios = int(saldo_row['total'] or 0)
        fuente = 'saldo_historico_mensual'
    else:
        saldo_total = float(
            conn.execute(
                """
                SELECT ROUND(COALESCE(SUM(saldo_actual), 0), 1)
                FROM obligaciones_mensuales
                WHERE periodo=?
                """,
                (periodo,),
            ).fetchone()[0]
            or 0
        )
        total_socios = int(
            conn.execute(
                "SELECT COUNT(*) FROM obligaciones_mensuales WHERE periodo=?",
                (periodo,),
            ).fetchone()[0]
            or 0
        )
        fuente = 'obligaciones_mensuales'
    acciones_por_socio = round((saldo_total / total_socios), 1) if total_socios else 0
    return {
        'periodo': periodo,
        'saldo_actual_total': saldo_total,
        'total_socios': total_socios,
        'acciones_por_socio': acciones_por_socio,
        'fuente': fuente,
    }


def sync_dashboard_financial_snapshot(conn, periodo):
    snapshot = get_financial_snapshot_for_period(conn, periodo)
    if not snapshot:
        return None
    conn.execute(
        "INSERT OR REPLACE INTO configuracion(clave, valor) VALUES(?, ?)",
        ('saldo_actual_total_oficial', str(snapshot['saldo_actual_total'])),
    )
    conn.execute(
        "INSERT OR REPLACE INTO configuracion(clave, valor) VALUES(?, ?)",
        ('acciones_por_socio_oficial', str(snapshot['acciones_por_socio'])),
    )
    conn.execute(
        "INSERT OR REPLACE INTO configuracion(clave, valor) VALUES(?, ?)",
        ('resumen_financiero_periodo_oficial', snapshot['periodo']),
    )
    log_financial_event(
        'Snapshot financiero sincronizado',
        periodo=periodo,
        saldo_actual_total=snapshot['saldo_actual_total'],
        total_socios=snapshot['total_socios'],
        acciones_por_socio=snapshot['acciones_por_socio'],
        fuente=snapshot['fuente'],
    )
    return snapshot


def get_latest_closed_period(conn):
    row = conn.execute(
        """
        SELECT periodo
        FROM periodos
        WHERE estado IN ('Colocado', 'Cerrado')
        ORDER BY periodo DESC
        LIMIT 1
        """
    ).fetchone()
    return row['periodo'] if row else None


def annotate_schedule_rows(rows, closed_period):
    annotated = []
    for row in rows or []:
        item = dict(row)
        item['periodo_cuota'] = (item.get('fecha') or '')[:7]
        # Se resalta todo lo que ya quedó cubierto por el último cierre mensual.
        item['periodo_cerrado'] = bool(closed_period and item['periodo_cuota'] and item['periodo_cuota'] <= closed_period)
        annotated.append(item)
    return annotated


def format_date_display(value):
    if not value:
        return ''
    match = re.fullmatch(r'(\d{4})-(\d{2})-(\d{2})', str(value))
    if not match:
        return str(value)
    return f"{match.group(3)}/{match.group(2)}/{match.group(1)}"


def get_visible_excel_loan_range(prestamo_excel):
    if not prestamo_excel:
        return None, None
    if isinstance(prestamo_excel, sqlite3.Row):
        getter = prestamo_excel.__getitem__
    else:
        getter = prestamo_excel.get
    fecha_inicio = getter('fecha_inicio_manual') or getter('fecha_inicio')
    fecha_fin = getter('fecha_fin_manual') or getter('fecha_fin')
    return fecha_inicio, fecha_fin


def filter_visible_excel_schedule(rows, prestamo_excel):
    fecha_inicio, fecha_fin = get_visible_excel_loan_range(prestamo_excel)
    filtered = []
    for row in rows or []:
        fecha = row['fecha'] if isinstance(row, sqlite3.Row) else row.get('fecha')
        if fecha_inicio and fecha and str(fecha) < str(fecha_inicio):
            continue
        if fecha_fin and fecha and str(fecha) > str(fecha_fin):
            continue
        filtered.append(row)
    return filtered or list(rows or [])


def summarize_excel_schedule(rows):
    positivos = []
    for row in rows or []:
        plazo = row['plazo'] if isinstance(row, sqlite3.Row) else row.get('plazo')
        if (plazo or 0) > 0:
            positivos.append(row)
    total_intereses = round(sum(float((row['interes'] if isinstance(row, sqlite3.Row) else row.get('interes')) or 0) for row in positivos), 1)
    total_capital = round(sum(float((row['abono_capital'] if isinstance(row, sqlite3.Row) else row.get('abono_capital')) or 0) for row in positivos), 1)
    total_pagable = round(sum(float((row['cuota'] if isinstance(row, sqlite3.Row) else row.get('cuota')) or 0) for row in positivos), 1)
    plazo_total = max([int((row['plazo'] if isinstance(row, sqlite3.Row) else row.get('plazo')) or 0) for row in positivos], default=0)
    ultimo = rows[-1] if rows else None
    saldo_final = float((ultimo['saldo'] if isinstance(ultimo, sqlite3.Row) else ultimo.get('saldo')) or 0) if ultimo else 0
    return {
        'intereses': total_intereses,
        'capital': total_capital,
        'total_pagable': total_pagable,
        'plazo_total': plazo_total,
        'saldo_final': saldo_final,
    }


def period_month_index(periodo):
    year, month = periodo.split('-')
    return (int(year) * 12) + int(month)


def ensure_period_reunion_assignment(conn, periodo):
    actual = conn.execute(
        """
        SELECT *
        FROM reuniones_mensuales
        WHERE periodo=?
        """,
        (periodo,),
    ).fetchone()


def cleanup_legacy_auto_reunion_assignment(conn, periodo):
    row = conn.execute(
        """
        SELECT *
        FROM reuniones_mensuales
        WHERE periodo=?
        """,
        (periodo,),
    ).fetchone()
    if not row:
        return None
    if row['estado'] != 'Pendiente':
        return row
    if row['observacion'] not in (None, ''):
        return row
    if row['fecha_realizada'] not in (None, ''):
        return row
    if row['fecha_programada'] not in (None, '', f'{periodo}-01'):
        return row

    socios = conn.execute(
        """
        SELECT numero
        FROM socios
        ORDER BY numero
        """
    ).fetchall()
    if not socios:
        return row

    periodos = [p['periodo'] for p in conn.execute("SELECT periodo FROM periodos ORDER BY periodo").fetchall()]
    if periodo not in periodos:
        periodos.append(periodo)
    periodos = sorted(set(periodos))
    expected_numero = socios[periodos.index(periodo) % len(socios)]['numero']
    if row['socio_numero'] == expected_numero:
        conn.execute("DELETE FROM reuniones_mensuales WHERE periodo=?", (periodo,))
        conn.commit()
        return None
    return row
    if actual:
        return actual

    socios = conn.execute(
        """
        SELECT numero, nombre
        FROM socios
        ORDER BY numero
        """
    ).fetchall()
    if not socios:
        return None

    periodos = [row['periodo'] for row in conn.execute("SELECT periodo FROM periodos ORDER BY periodo").fetchall()]
    if periodo not in periodos:
        periodos.append(periodo)
    periodos = sorted(set(periodos))
    turno_index = periodos.index(periodo) % len(socios)
    socio_turno = socios[turno_index]

    conn.execute(
        """
        INSERT OR REPLACE INTO reuniones_mensuales(
            periodo, socio_numero, socio_nombre, estado, fecha_programada, actualizado_en
        ) VALUES(?, ?, ?, 'Pendiente', ?, CURRENT_TIMESTAMP)
        """,
        (periodo, socio_turno['numero'], socio_turno['nombre'], f"{periodo}-01"),
    )
    return conn.execute(
        """
        SELECT *
        FROM reuniones_mensuales
        WHERE periodo=?
        """,
        (periodo,),
    ).fetchone()


def add_months_iso(iso_date, months_to_add):
    base = datetime.strptime(iso_date, '%Y-%m-%d').date()
    month_index = (base.month - 1) + months_to_add
    year = base.year + month_index // 12
    month = (month_index % 12) + 1
    day = min(base.day, calendar.monthrange(year, month)[1])
    return date(year, month, day).isoformat()


def build_new_loan_schedule(monto, cuotas, tasa_mensual, fecha_desembolso):
    schedule = [
        {
            'plazo': 0,
            'fecha': fecha_desembolso,
            'prestamo': round(monto, 1),
            'interes': 0,
            'abono_capital': 0,
            'cuota': 0,
            'saldo': round(monto, 1),
        }
    ]
    saldo = round(monto, 1)
    capital_base = round(monto / cuotas, 1)

    for plazo in range(1, cuotas + 1):
        interes = round(saldo * tasa_mensual, 1)
        abono_capital = capital_base if plazo < cuotas else round(saldo, 1)
        cuota = round(interes + abono_capital, 1)
        saldo = round(max(saldo - abono_capital, 0), 1)
        schedule.append(
            {
                'plazo': plazo,
                'fecha': add_months_iso(fecha_desembolso, plazo),
                'prestamo': None,
                'interes': interes,
                'abono_capital': abono_capital,
                'cuota': cuota,
                'saldo': saldo,
            }
        )
    return schedule


def create_loan_history_record(conn, socio_numero, socio_nombre, prestamo, accion, estado_resultante, detalle, creado_por, snapshot_rows=None, socio_anterior=None):
    cur = conn.execute(
        """
        INSERT INTO historial_prestamos_socios(
            socio_numero, socio_nombre, prestamo_nuevo_id, periodo, accion, estado_resultante,
            monto, cuotas, fecha_desembolso, cuota_inicial, total_interes, total_pagable,
            saldo_anterior, meses_anteriores, fecha_prestamo_anterior, detalle, creado_por
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            socio_numero,
            socio_nombre,
            prestamo['id'] if prestamo else None,
            prestamo['periodo'] if prestamo else None,
            accion,
            estado_resultante,
            prestamo['monto'] if prestamo else None,
            prestamo['cuotas'] if prestamo else None,
            prestamo['fecha_desembolso'] if prestamo else None,
            prestamo['cuota_inicial'] if prestamo else None,
            prestamo['total_interes'] if prestamo else None,
            prestamo['total_pagable'] if prestamo else None,
            socio_anterior['saldo'] if socio_anterior else None,
            socio_anterior['meses'] if socio_anterior else None,
            socio_anterior['fecha_prestamo'] if socio_anterior else None,
            detalle,
            creado_por,
        ),
    )
    historial_id = cur.lastrowid

    for row in snapshot_rows or []:
        conn.execute(
            """
            INSERT INTO historial_prestamos_socios_cuotas(
                historial_id, plazo, fecha, prestamo, interes, abono_capital, cuota, saldo, hoja_origen
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                historial_id,
                row['plazo'],
                row['fecha'],
                row['prestamo'],
                row['interes'],
                row['abono_capital'],
                row['cuota'],
                row['saldo'],
                row['hoja_origen'],
            ),
        )
    log_financial_event(
        'Historial de préstamo registrado',
        historial_id=historial_id,
        socio_numero=socio_numero,
        socio_nombre=socio_nombre,
        accion=accion,
        estado_resultante=estado_resultante,
        detalle=detalle,
        creado_por=creado_por,
        prestamo_id=prestamo['id'] if prestamo else None,
        periodo=prestamo['periodo'] if prestamo else None,
        monto=prestamo['monto'] if prestamo else None,
        cuotas=prestamo['cuotas'] if prestamo else None,
        saldo_anterior=socio_anterior['saldo'] if socio_anterior else None,
    )
    return historial_id


def create_reserved_loan(conn, periodo, socio, monto, cuotas, tasa_mensual, fecha_desembolso):
    schedule = build_new_loan_schedule(monto, cuotas, tasa_mensual, fecha_desembolso)
    total_interes = round(sum([row['interes'] or 0 for row in schedule]), 1)
    total_pagable = round(sum([row['cuota'] or 0 for row in schedule]), 1)
    cuota_inicial = schedule[1]['cuota'] if len(schedule) > 1 else 0

    cur = conn.execute(
        """
        INSERT INTO prestamos_nuevos(
            periodo, socio_numero, socio_nombre, monto, cuotas, tasa_mensual,
            fecha_desembolso, cuota_inicial, total_interes, total_pagable, estado
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            periodo,
            socio['numero'],
            socio['nombre'],
            round(monto, 1),
            cuotas,
            tasa_mensual,
            fecha_desembolso,
            cuota_inicial,
            total_interes,
            total_pagable,
            'Reservado',
        ),
    )
    prestamo_id = cur.lastrowid
    for row in schedule:
        conn.execute(
            """
            INSERT INTO prestamos_nuevos_cronograma(
                prestamo_nuevo_id, plazo, fecha, prestamo, interes, abono_capital, cuota, saldo
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                prestamo_id,
                row['plazo'],
                row['fecha'],
                row['prestamo'],
                row['interes'],
                row['abono_capital'],
                row['cuota'],
                row['saldo'],
            ),
        )
    log_financial_event(
        'Préstamo reservado',
        prestamo_id=prestamo_id,
        periodo=periodo,
        socio_numero=socio['numero'],
        socio_nombre=socio['nombre'],
        monto=round(monto, 1),
        cuotas=cuotas,
        tasa_mensual=tasa_mensual,
        fecha_desembolso=fecha_desembolso,
        cuota_inicial=cuota_inicial,
        total_interes=total_interes,
        total_pagable=total_pagable,
    )
    return prestamo_id


def build_funding_suggestions(conn, periodo, fondo_disponible, min_cuotas, max_cuotas):
    sugerencias = []
    restante_sugerido = round(fondo_disponible or 0, 1)
    socios_prioridad = conn.execute(
        """
        SELECT s.numero, s.nombre,
               COALESCE(p.prioridad, 999999) as prioridad_manual,
               CASE WHEN p.prioridad IS NULL THEN 0 ELSE 1 END as tiene_prioridad,
               COALESCE(om.saldo_actual,
                        (SELECT saldo
                         FROM cuotas
                         WHERE socio_numero=s.numero AND plazo BETWEEN 0 AND 240
                         ORDER BY fecha DESC, plazo DESC
                         LIMIT 1),
                        s.saldo, 0) as saldo_prioridad,
               COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses, ?) as cuotas_sugeridas
        FROM socios s
        LEFT JOIN cuotas c ON c.socio_numero = s.numero
        LEFT JOIN prioridad_colocacion_manual p ON p.periodo=? AND p.socio_numero=s.numero
        LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
        WHERE NOT EXISTS (
            SELECT 1
            FROM prestamos_nuevos pn
            WHERE pn.periodo=? AND pn.socio_numero=s.numero AND pn.estado IN ('Reservado', 'Aprobado')
        )
        GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos, p.prioridad
        ORDER BY tiene_prioridad DESC, prioridad_manual ASC, saldo_prioridad ASC, s.numero ASC
        """,
        (min_cuotas, periodo, periodo, periodo),
    ).fetchall()
    for socio_row in socios_prioridad:
        if restante_sugerido <= 0:
            break
        monto_sugerido = round(min(restante_sugerido, socio_row['saldo_prioridad'] or restante_sugerido), 1)
        if monto_sugerido <= 0:
            continue
        cuotas_sugeridas = socio_row['cuotas_sugeridas'] or min_cuotas
        cuotas_sugeridas = max(min_cuotas, min(int(cuotas_sugeridas), max_cuotas))
        sugerencias.append(
            {
                'numero': socio_row['numero'],
                'nombre': socio_row['nombre'],
                'saldo_prioridad': socio_row['saldo_prioridad'] or 0,
                'monto_sugerido': monto_sugerido,
                'cuotas_sugeridas': cuotas_sugeridas,
                'prioridad_manual': None if socio_row['prioridad_manual'] == 999999 else socio_row['prioridad_manual'],
            }
        )
        restante_sugerido = round(restante_sugerido - monto_sugerido, 1)
    return sugerencias, restante_sugerido


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get('user_id'):
            if is_api_request():
                return api_error('Debes iniciar sesión para usar esta API.', status=401)
            return redirect(url_for('main.login'))
        return view(*args, **kwargs)
    return wrapped


def admin_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get('user_id'):
            return redirect(url_for('main.login'))
        if session.get('role') != 'Administrador':
            flash('Solo el administrador puede realizar esta acción.')
            return redirect(url_for('main.dashboard'))
        return view(*args, **kwargs)
    return wrapped


@bp.app_context_processor
def inject_config():
    try:
        with connect_db() as conn:
            cfg = {r['clave']: r['valor'] for r in conn.execute("SELECT clave, valor FROM configuracion").fetchall()}
            excel = conn.execute("SELECT valor FROM meta WHERE clave='excel_importado'").fetchone()
            excel_sync = conn.execute("SELECT valor FROM meta WHERE clave='excel_sync_at'").fetchone()
        logo_config = (cfg.get('logo_institucional') or '').strip()
        return dict(
            app_nombre=cfg.get('nombre_asociacion', 'Asociación JAWILVIO'),
            app_ubicacion=cfg.get('ubicacion', 'Celendín, Cajamarca'),
            app_logo=build_branding_logo_url(logo_config),
            excel_importado=excel['valor'] if excel else '',
            excel_sync_at=excel_sync['valor'] if excel_sync else '',
            can_access=lambda endpoint, write=False: role_can_access_endpoint(session.get('role'), endpoint, write),
            current_role=session.get('role', ''),
        )
    except Exception:
        return dict(
            app_nombre='Asociación JAWILVIO',
            app_ubicacion='Celendín, Cajamarca',
            app_logo=url_for('static', filename='img/logo_jawilvio_light.svg'),
            excel_importado='',
            excel_sync_at='',
            can_access=lambda endpoint, write=False: False,
            current_role=session.get('role', ''),
        )


@bp.route('/')
def index():
    return redirect(url_for('main.dashboard') if session.get('user_id') else url_for('main.login'))


@bp.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    pending_two_factor = bool(session.get('pending_2fa_user_id'))
    if request.method == 'POST':
        action = request.form.get('action', 'password_step').strip()
        if action == 'verify_2fa':
            pending_user_id = session.get('pending_2fa_user_id')
            code = request.form.get('two_factor_code', '').strip()
            if not pending_user_id:
                clear_pending_two_factor_session()
                error = 'Tu verificación en dos pasos venció. Ingresa nuevamente con tu usuario y contraseña.'
            else:
                with connect_db() as conn:
                    user = conn.execute("SELECT * FROM users WHERE id=?", (pending_user_id,)).fetchone()
                    if not user:
                        clear_pending_two_factor_session()
                        error = 'No se encontró el usuario para completar la verificación.'
                    elif not user['two_factor_enabled'] or not (user['two_factor_secret'] or '').strip():
                        clear_pending_two_factor_session()
                        error = 'La verificación en dos pasos ya no está activa para este usuario.'
                    elif not verify_totp_code(user['two_factor_secret'], code):
                        error = 'El código de verificación no es válido.'
                        log_system_event(
                            'Código 2FA inválido',
                            level=logging.WARNING,
                            username_intent=user['username'],
                        )
                    else:
                        conn.execute(
                            "UPDATE users SET ultimo_acceso=CURRENT_TIMESTAMP WHERE id=?",
                            (user['id'],),
                        )
                        conn.commit()
                        finalize_login_session(user)
                        log_action(user['username'], 'Inicio de sesión con 2FA', f"Rol: {user['role']}")
                        return redirect(url_for('main.dashboard'))
        else:
            username = request.form.get('username', '').strip()
            password = request.form.get('password', '').strip()
            with connect_db() as conn:
                user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
            if user:
                if user['estado'] == 'Suspendido':
                    error = 'Tu usuario está suspendido. Contacta al administrador.'
                    log_system_event(
                        'Intento de inicio de sesión con usuario suspendido',
                        level=logging.WARNING,
                        username_intent=username,
                        role=user['role'],
                    )
                elif not verify_password(user['password'], password):
                    error = 'Credenciales incorrectas'
                    log_system_event(
                        'Inicio de sesión fallido por contraseña incorrecta',
                        level=logging.WARNING,
                        username_intent=username,
                    )
                elif user['two_factor_enabled'] and (user['two_factor_secret'] or '').strip():
                    if not is_password_hashed(user['password']):
                        with connect_db() as conn:
                            conn.execute(
                                "UPDATE users SET password=? WHERE id=?",
                                (hash_password(password), user['id']),
                            )
                            conn.commit()
                    clear_pending_two_factor_session()
                    session['pending_2fa_user_id'] = user['id']
                    session['pending_2fa_username'] = user['username']
                    session['pending_2fa_role'] = user['role']
                    session['pending_2fa_started_at'] = datetime.now().isoformat()
                    pending_two_factor = True
                else:
                    with connect_db() as conn:
                        if not is_password_hashed(user['password']):
                            conn.execute(
                                "UPDATE users SET password=? WHERE id=?",
                                (hash_password(password), user['id']),
                            )
                        conn.execute(
                            "UPDATE users SET ultimo_acceso=CURRENT_TIMESTAMP WHERE id=?",
                            (user['id'],),
                        )
                        conn.commit()
                    finalize_login_session(user)
                    log_action(user['username'], 'Inicio de sesión', f"Rol: {user['role']}")
                    return redirect(url_for('main.dashboard'))
            else:
                error = 'Credenciales incorrectas'
                log_system_event(
                    'Inicio de sesión fallido por usuario inexistente',
                    level=logging.WARNING,
                    username_intent=username,
                )

    return render_template(
        'login.html',
        error=error,
        pending_two_factor=pending_two_factor,
        pending_username=session.get('pending_2fa_username', ''),
    )


@bp.route('/logout')
def logout():
    usuario = session.get('username', 'desconocido')
    session.clear()
    try:
        log_action(usuario, 'Cierre de sesión')
    except Exception:
        pass
    return redirect(url_for('main.login'))


@bp.route('/socio/<int:numero>', methods=['GET', 'POST'])
@login_required
def socio_detalle(numero):
    with connect_db() as conn:
        if request.method == 'POST':
            if session.get('role') != 'Administrador':
                flash('Solo el administrador puede realizar esta acción.')
                return redirect(url_for('main.socio_detalle', numero=numero))
            action = request.form.get('action', '').strip()
            if action == 'crear_prestamo_excel_manual':
                titulo_visible = request.form.get('titulo_visible', '').strip()
                fecha_inicio = request.form.get('fecha_inicio', '').strip()
                monto_raw = request.form.get('monto_inicial', '').strip()
                cuotas_raw = request.form.get('cuotas', '').strip()
                saldo_base_ref_raw = request.form.get('saldo_base_ref', '').strip()
                prestamo_adicional_ref_raw = request.form.get('prestamo_adicional_ref', '').strip()
                aplicar_como_vigente = request.form.get('aplicar_como_vigente') == '1'
                socio_base = conn.execute("SELECT numero, nombre FROM socios WHERE numero=?", (numero,)).fetchone()
                try:
                    monto = round(float(monto_raw), 1)
                except Exception:
                    monto = None
                try:
                    saldo_base_ref = round(float(saldo_base_ref_raw), 1) if saldo_base_ref_raw else None
                except Exception:
                    saldo_base_ref = None
                try:
                    prestamo_adicional_ref = round(float(prestamo_adicional_ref_raw), 1) if prestamo_adicional_ref_raw else None
                except Exception:
                    prestamo_adicional_ref = None
                cuotas = int(cuotas_raw) if cuotas_raw.isdigit() else None
                tasa_mensual = float(get_config_value(conn, 'tasa_prestamo_mensual', '0.01'))
                min_cuotas = int(float(get_config_value(conn, 'min_cuotas_prestamo', '12')))
                max_cuotas = int(float(get_config_value(conn, 'max_cuotas_prestamo', '84')))
                if not socio_base:
                    flash('No se encontró el socio solicitado.')
                    return redirect(url_for('main.socio_detalle', numero=numero))
                if not titulo_visible or not fecha_inicio or monto is None or monto <= 0 or cuotas is None:
                    flash('Completa título, fecha, monto y cuotas válidas para crear el préstamo manual.')
                    return redirect(url_for('main.socio_detalle', numero=numero))
                if cuotas < min_cuotas or cuotas > max_cuotas:
                    flash(f'Las cuotas deben estar entre {min_cuotas} y {max_cuotas}.')
                    return redirect(url_for('main.socio_detalle', numero=numero))
                prestamo_excel_id = create_manual_excel_loan(
                    conn,
                    numero,
                    socio_base['nombre'],
                    titulo_visible,
                    fecha_inicio,
                    monto,
                    cuotas,
                    tasa_mensual,
                    aplicar_como_vigente=aplicar_como_vigente,
                    saldo_base_ref=saldo_base_ref,
                    prestamo_adicional_ref=prestamo_adicional_ref,
                )
                conn.commit()
                log_action(
                    session.get('username', 'admin'),
                    'Creación manual de préstamo histórico',
                    f'Socio {numero}: préstamo manual creado ({titulo_visible}) por S/ {monto:.1f} a {cuotas} cuotas.'
                )
                flash('Préstamo histórico manual creado correctamente.')
                return redirect(url_for('main.socio_detalle', numero=numero))
            if action in {'aplicar_prestamo_excel', 'actualizar_prestamo_excel'}:
                prestamo_excel_id = request.form.get('prestamo_excel_id', type=int)
                return process_excel_loan_admin_update(
                    conn,
                    numero,
                    prestamo_excel_id,
                    'main.socio_detalle',
                    numero=numero,
                )
        periodo_cerrado = get_latest_closed_period(conn)
        periodo_actual = datetime.now().strftime('%Y-%m')
        socio = conn.execute("""
            SELECT s.*,
                   COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
            FROM socios s
            LEFT JOIN cuotas c ON c.socio_numero = s.numero
            WHERE s.numero=?
            GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
        """, (numero,)).fetchone()
        cuotas = conn.execute("SELECT * FROM cuotas WHERE socio_numero=? ORDER BY fecha", (numero,)).fetchall()
        prestamos_excel = conn.execute(
            """
            SELECT *
            FROM prestamos_excel_historial
            WHERE socio_numero=?
              AND COALESCE(oculto_manual, 0)=0
            ORDER BY date(fecha_inicio) DESC, bloque_orden DESC
            """,
            (numero,),
        ).fetchall()
        prestamos_excel = [dict(row) for row in prestamos_excel]
        for row in prestamos_excel:
            fecha_inicio_visible, fecha_fin_visible = get_visible_excel_loan_range(row)
            visible_rows = conn.execute(
                """
                SELECT plazo, fecha, interes, abono_capital, cuota, saldo
                FROM prestamos_excel_historial_cuotas
                WHERE prestamo_excel_id=?
                  AND (? IS NULL OR date(fecha) >= date(?))
                  AND (? IS NULL OR date(fecha) <= date(?))
                ORDER BY plazo, fecha
                """,
                (row['id'], fecha_inicio_visible, fecha_inicio_visible, fecha_fin_visible, fecha_fin_visible),
            ).fetchall()
            resumen_visible = summarize_excel_schedule(visible_rows)
            row['fecha_inicio_visible'] = fecha_inicio_visible or row.get('fecha_inicio')
            row['fecha_fin_visible'] = fecha_fin_visible or row.get('fecha_fin')
            row['plazo_total_visible'] = resumen_visible['plazo_total'] or row.get('plazo_total') or 0
            row['saldo_final_visible'] = resumen_visible['saldo_final'] if visible_rows else row.get('saldo_final')
            fecha_fin = row.get('fecha_fin_visible') or ''
            saldo_final = row.get('saldo_final_visible')
            if row.get('es_activo') and saldo_final == 0 and fecha_fin and fecha_fin[:7] < periodo_actual:
                row['estado_visible'] = 'Pagado'
            elif row.get('es_activo'):
                row['estado_visible'] = 'Activo en la app'
            else:
                row['estado_visible'] = 'Histórico'
        prestamo_excel_activo = next(
            (row for row in prestamos_excel if row.get('es_activo') and row.get('estado_visible') != 'Pagado'),
            None,
        )
        prestamo_excel_referencia = prestamo_excel_activo or next(
            (row for row in prestamos_excel if row.get('estado_visible') == 'Pagado'),
            None,
        ) or (prestamos_excel[0] if prestamos_excel else None)
        asistencia = conn.execute("SELECT * FROM asistencia WHERE socio_numero=? ORDER BY fecha DESC LIMIT 20", (numero,)).fetchall()
        historico_saldos = conn.execute(
            """
            SELECT periodo, saldo, fuente
            FROM saldo_historico_mensual
            WHERE socio_numero=?
            ORDER BY periodo DESC
            """,
            (numero,)
        ).fetchall()
        historial_prestamos = conn.execute(
            """
            SELECT h.*,
                   COUNT(c.id) as cuotas_snapshot
            FROM historial_prestamos_socios h
            LEFT JOIN historial_prestamos_socios_cuotas c ON c.historial_id = h.id
            WHERE h.socio_numero=?
            GROUP BY h.id
            ORDER BY h.creado_en DESC, h.id DESC
            """,
            (numero,)
        ).fetchall()
        resumen = conn.execute("""
            SELECT COUNT(DISTINCT CASE WHEN plazo BETWEEN 1 AND 240 THEN plazo END) as total_cuotas,
                   COALESCE(SUM(CASE WHEN plazo BETWEEN 1 AND 240 THEN interes ELSE 0 END), 0) as intereses,
                   COALESCE(SUM(CASE WHEN plazo BETWEEN 1 AND 240 THEN abono_capital ELSE 0 END), 0) as capital,
                   COALESCE(SUM(CASE WHEN plazo BETWEEN 1 AND 240 THEN cuota ELSE 0 END), 0) as total_pagable,
                   MIN(CASE WHEN plazo BETWEEN 0 AND 240 THEN fecha END) as primera_fecha,
                   MAX(CASE WHEN plazo BETWEEN 0 AND 240 THEN fecha END) as ultima_fecha
            FROM cuotas
            WHERE socio_numero=?
        """, (numero,)).fetchone()
        cronograma_historico = []
        resumen_historico = None
        if prestamo_excel_referencia:
            cronograma_historico_full = conn.execute(
                """
                SELECT *
                FROM prestamos_excel_historial_cuotas
                WHERE prestamo_excel_id=?
                ORDER BY plazo, fecha
                """,
                (prestamo_excel_referencia['id'],),
            ).fetchall()
            cronograma_historico = filter_visible_excel_schedule(cronograma_historico_full, prestamo_excel_referencia)
            resumen_visible_historico = summarize_excel_schedule(cronograma_historico)
            prestamo_excel_referencia['interes_visible'] = resumen_visible_historico['intereses']
            prestamo_excel_referencia['capital_visible'] = resumen_visible_historico['capital']
            prestamo_excel_referencia['cuota_total_visible'] = resumen_visible_historico['total_pagable']
            prestamo_excel_referencia['saldo_visible'] = resumen_visible_historico['saldo_final']
            resumen_historico = {
                'intereses': float(prestamo_excel_referencia['interes_total'] or 0),
                'capital': float(prestamo_excel_referencia['capital_total'] or 0),
                'total_pagable': float(prestamo_excel_referencia['cuota_total'] or 0),
            }
        else:
            cronograma_historico = cuotas
            resumen_historico = {
                'intereses': float(resumen['intereses'] or 0),
                'capital': float(resumen['capital'] or 0),
                'total_pagable': float(resumen['total_pagable'] or 0),
            }
    cronograma_vigente_terminado = False
    if cuotas:
        ultima_cuota = cuotas[-1]
        ultima_fecha = ultima_cuota['fecha'] or ''
        ultimo_saldo = ultima_cuota['saldo']
        if ultimo_saldo == 0 and ultima_fecha and ultima_fecha[:7] < periodo_actual:
            cronograma_vigente_terminado = True
    saldo_actual = historico_saldos[0]['saldo'] if historico_saldos else (socio['saldo'] if socio else 0)
    cuotas_annotated = annotate_schedule_rows(cuotas, periodo_cerrado)
    cronograma_historico_annotated = annotate_schedule_rows(cronograma_historico, periodo_cerrado)
    for row in cuotas_annotated:
        row['fecha_display'] = format_date_display(row.get('fecha'))
    for row in cronograma_historico_annotated:
        row['fecha_display'] = format_date_display(row.get('fecha'))
    return render_template(
        'socio_detalle.html',
        socio=socio,
        cuotas=cuotas_annotated,
        cronograma_historico=cronograma_historico_annotated,
        prestamos_excel=prestamos_excel,
        asistencia=asistencia,
        resumen=resumen,
        resumen_historico=resumen_historico,
        historico_saldos=historico_saldos,
        historial_prestamos=historial_prestamos,
        prestamo_excel_activo=prestamo_excel_activo,
        prestamo_excel_referencia=prestamo_excel_referencia,
        saldo_actual=saldo_actual,
        cronograma_vigente_terminado=cronograma_vigente_terminado,
        periodo_cerrado=periodo_cerrado,
    )


@bp.route('/socio/<int:numero>/prestamo-excel/<int:prestamo_excel_id>', methods=['GET', 'POST'])
@login_required
def prestamo_excel_detalle(numero, prestamo_excel_id):
    with connect_db() as conn:
        if request.method == 'POST':
            if session.get('role') != 'Administrador':
                flash('Solo el administrador puede realizar esta acción.')
                return redirect(url_for('main.prestamo_excel_detalle', numero=numero, prestamo_excel_id=prestamo_excel_id))
            action = request.form.get('action', '').strip()
            if action in {'aplicar_prestamo_excel', 'actualizar_prestamo_excel'}:
                return process_excel_loan_admin_update(
                    conn,
                    numero,
                    prestamo_excel_id,
                    'main.prestamo_excel_detalle',
                    numero=numero,
                    prestamo_excel_id=prestamo_excel_id,
                )
        periodo_cerrado = get_latest_closed_period(conn)
        socio = conn.execute(
            "SELECT numero, nombre FROM socios WHERE numero=?",
            (numero,),
        ).fetchone()
        prestamo_excel = conn.execute(
            """
            SELECT *
            FROM prestamos_excel_historial
            WHERE id=? AND socio_numero=? AND COALESCE(oculto_manual, 0)=0
            """,
            (prestamo_excel_id, numero),
        ).fetchone()
        cronograma = conn.execute(
            """
            SELECT *
            FROM prestamos_excel_historial_cuotas
            WHERE prestamo_excel_id=?
            ORDER BY plazo, fecha
            """,
            (prestamo_excel_id,),
        ).fetchall()
    if not prestamo_excel:
        flash('No se encontro el prestamo historico solicitado.')
        return redirect(url_for('main.socio_detalle', numero=numero))
    cronograma_visible = filter_visible_excel_schedule(cronograma, prestamo_excel)
    resumen_visible = summarize_excel_schedule(cronograma_visible)
    prestamo_excel_view = dict(prestamo_excel)
    fecha_inicio_visible, fecha_fin_visible = get_visible_excel_loan_range(prestamo_excel)
    prestamo_excel_view['fecha_inicio_visible'] = fecha_inicio_visible or prestamo_excel_view.get('fecha_inicio')
    prestamo_excel_view['fecha_fin_visible'] = fecha_fin_visible or prestamo_excel_view.get('fecha_fin')
    prestamo_excel_view['plazo_total_visible'] = resumen_visible['plazo_total'] or prestamo_excel_view.get('plazo_total') or 0
    prestamo_excel_view['interes_total_visible'] = resumen_visible['intereses']
    prestamo_excel_view['capital_total_visible'] = resumen_visible['capital']
    prestamo_excel_view['cuota_total_visible'] = resumen_visible['total_pagable']
    prestamo_excel_view['saldo_visible'] = resumen_visible['saldo_final']
    prestamo_excel_view['interes_visible'] = resumen_visible['intereses']
    if prestamo_excel_view.get('saldo_base_ref') is None and resumen_visible['saldo_final']:
        prestamo_excel_view['saldo_base_ref'] = resumen_visible['saldo_final']
    if prestamo_excel_view.get('prestamo_adicional_ref') is None and prestamo_excel_view.get('monto_inicial') is not None:
        prestamo_excel_view['prestamo_adicional_ref'] = prestamo_excel_view.get('monto_inicial')
    return render_template(
        'prestamo_excel_detalle.html',
        socio=socio,
        prestamo_excel=prestamo_excel_view,
        cronograma=[
            dict(item, fecha_display=format_date_display(item['fecha']))
            for item in annotate_schedule_rows(cronograma_visible, periodo_cerrado)
        ],
        periodo_cerrado=periodo_cerrado,
    )


@bp.route('/socio/<int:numero>/historial-prestamo/<int:historial_id>')
@login_required
def historial_prestamo_detalle(numero, historial_id):
    with connect_db() as conn:
        periodo_cerrado = get_latest_closed_period(conn)
        socio = conn.execute(
            "SELECT numero, nombre FROM socios WHERE numero=?",
            (numero,),
        ).fetchone()
        historial = conn.execute(
            """
            SELECT *
            FROM historial_prestamos_socios
            WHERE id=? AND socio_numero=?
            """,
            (historial_id, numero),
        ).fetchone()
        cronograma = conn.execute(
            """
            SELECT *
            FROM historial_prestamos_socios_cuotas
            WHERE historial_id=?
            ORDER BY plazo, fecha
            """,
            (historial_id,),
        ).fetchall()
    if not historial:
        flash('No se encontró el historial solicitado.')
        return redirect(url_for('main.socio_detalle', numero=numero))
    return render_template(
        'historial_prestamo_detalle.html',
        socio=socio,
        historial=historial,
          cronograma=annotate_schedule_rows(cronograma, periodo_cerrado),
          periodo_cerrado=periodo_cerrado,
      )


@bp.route('/socio/<int:numero>/historial-prestamo/<int:historial_id>/comparar')
@login_required
def historial_prestamo_comparar(numero, historial_id):
    with connect_db() as conn:
        socio = conn.execute(
            """
            SELECT s.*,
                   COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
            FROM socios s
            LEFT JOIN cuotas c ON c.socio_numero = s.numero
            WHERE s.numero=?
            GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
            """,
            (numero,),
        ).fetchone()
        historial = conn.execute(
            """
            SELECT *
            FROM historial_prestamos_socios
            WHERE id=? AND socio_numero=?
            """,
            (historial_id, numero),
        ).fetchone()
        anterior = conn.execute(
            """
            SELECT *
            FROM historial_prestamos_socios_cuotas
            WHERE historial_id=?
            ORDER BY plazo, fecha
            """,
            (historial_id,),
        ).fetchall()
        actual = conn.execute(
            """
            SELECT *
            FROM cuotas
            WHERE socio_numero=?
            ORDER BY plazo, fecha
            """,
            (numero,),
        ).fetchall()
    if not historial:
        flash('No se encontró el historial solicitado.')
        return redirect(url_for('main.socio_detalle', numero=numero))

    anterior_map = {row['plazo']: row for row in anterior}
    actual_map = {row['plazo']: row for row in actual}
    plazos = sorted(set(anterior_map.keys()) | set(actual_map.keys()))
    comparacion = []
    for plazo in plazos:
        anterior_row = anterior_map.get(plazo)
        actual_row = actual_map.get(plazo)
        changes = []
        if not anterior_row or not actual_row:
            changes.append('estructura')
        else:
            if (anterior_row['fecha'] or '') != (actual_row['fecha'] or ''):
                changes.append('fecha')
            if round(anterior_row['cuota'] or 0, 1) != round(actual_row['cuota'] or 0, 1):
                changes.append('cuota')
            if round(anterior_row['saldo'] or 0, 1) != round(actual_row['saldo'] or 0, 1):
                changes.append('saldo')
        if not changes:
            change_level = 'sin-cambio'
        elif 'estructura' in changes or len(changes) >= 2:
            change_level = 'cambio-fuerte'
        else:
            change_level = 'cambio-parcial'
        comparacion.append(
            {
                'plazo': plazo,
                'anterior': anterior_row,
                'actual': actual_row,
                'changes': changes,
                'change_level': change_level,
            }
        )

    resumen_comparacion = {
        'sin_cambio': len([row for row in comparacion if row['change_level'] == 'sin-cambio']),
        'cambio_parcial': len([row for row in comparacion if row['change_level'] == 'cambio-parcial']),
        'cambio_fuerte': len([row for row in comparacion if row['change_level'] == 'cambio-fuerte']),
        'total_plazos': len(comparacion),
    }
    anterior_inicio = next((row for row in anterior if row['plazo'] == 0), anterior[0] if anterior else None)
    actual_inicio = next((row for row in actual if row['plazo'] == 0), actual[0] if actual else None)
    diferencias_clave = {
        'monto_anterior': anterior_inicio['prestamo'] if anterior_inicio else None,
        'monto_actual': actual_inicio['prestamo'] if actual_inicio else None,
        'saldo_inicial_anterior': anterior_inicio['saldo'] if anterior_inicio else None,
        'saldo_inicial_actual': actual_inicio['saldo'] if actual_inicio else None,
        'fecha_anterior': historial['fecha_prestamo_anterior'] or (anterior_inicio['fecha'] if anterior_inicio else None),
        'fecha_actual': socio['fecha_prestamo'] if socio else (actual_inicio['fecha'] if actual_inicio else None),
        'cuotas_anteriores': historial['meses_anteriores'] or len([row for row in anterior if row['plazo'] and row['plazo'] > 0]),
        'cuotas_actuales': socio['plazo_real'] if socio else len([row for row in actual if row['plazo'] and row['plazo'] > 0]),
        'cuota_inicial_anterior': next((row['cuota'] for row in anterior if row['plazo'] == 1), None),
        'cuota_inicial_actual': next((row['cuota'] for row in actual if row['plazo'] == 1), None),
    }

    return render_template(
        'historial_prestamo_comparar.html',
        socio=socio,
        historial=historial,
        comparacion=comparacion,
        resumen_comparacion=resumen_comparacion,
        diferencias_clave=diferencias_clave,
    )


@bp.route('/prestamos')
@login_required
def prestamos():
    with connect_db() as conn:
        periodo_cerrado = get_latest_closed_period(conn)
        prestamos_rows = conn.execute("""
            SELECT s.*,
                   COUNT(DISTINCT CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END) as cuotas_generadas,
                   COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real,
                   COALESCE(SUM(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.interes ELSE 0 END), 0) as interes_total,
                   MAX(h.saldo) as saldo_marzo_2026,
                   COALESCE(MAX(h.saldo), s.saldo, 0) as saldo_relacion
            FROM socios s
            LEFT JOIN cuotas c ON c.socio_numero = s.numero
            LEFT JOIN saldo_historico_mensual h ON h.socio_numero = s.numero AND h.periodo = '2026-03'
            GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
            ORDER BY s.numero ASC
        """).fetchall()
        resumen_prestamos = conn.execute("""
            SELECT COUNT(*) as total_socios,
                   COALESCE(SUM(saldo_usado), 0) as saldo_total,
                   COALESCE(AVG(saldo_usado), 0) as saldo_promedio
            FROM (
                SELECT s.numero, COALESCE(MAX(h.saldo), s.saldo, 0) as saldo_usado
                FROM socios s
                LEFT JOIN saldo_historico_mensual h ON h.socio_numero = s.numero AND h.periodo = '2026-03'
                GROUP BY s.numero, s.saldo
            ) base
        """).fetchone()
        resumen_historial_excel = conn.execute(
            """
            SELECT COUNT(*) as socios_con_historial,
                   COALESCE(SUM(CASE WHEN tiene_marzo_2026 > 0 THEN 1 ELSE 0 END), 0) as socios_con_marzo_2026
            FROM (
                SELECT socio_numero,
                       COUNT(*) as total_bloques,
                       SUM(CASE WHEN es_activo=1 AND substr(fecha_inicio, 1, 7)='2026-03' THEN 1 ELSE 0 END) as tiene_marzo_2026
                FROM prestamos_excel_historial
                WHERE COALESCE(oculto_manual, 0)=0
                GROUP BY socio_numero
                HAVING COUNT(*) > 1
                   OR SUM(CASE WHEN es_activo=1 AND substr(fecha_inicio, 1, 7)='2026-03' THEN 1 ELSE 0 END) > 0
            ) base
            """
        ).fetchone()
        prestamos = []
        for row in prestamos_rows:
            item = dict(row)
            saldo_actual = item['saldo_relacion']
            match = conn.execute(
                """
                SELECT plazo
                FROM cuotas
                WHERE socio_numero=? AND plazo BETWEEN 0 AND 240 AND saldo IS NOT NULL
                ORDER BY ABS(saldo - ?) ASC, plazo DESC
                LIMIT 1
                """,
                (item['numero'], saldo_actual or 0),
            ).fetchone()
            plazo_match = match['plazo'] if match else 0
            plazo_real = item['plazo_real'] or item['plazo_balance'] or item['meses'] or 0
            item['cuotas_pendientes'] = max(int(plazo_real) - int(plazo_match), 0) if plazo_real else 0
            item['cuotas_pagadas'] = max(int(plazo_real) - int(item['cuotas_pendientes']), 0) if plazo_real else 0
            historial_excel = conn.execute(
                """
                SELECT COUNT(*) as total_bloques,
                       COALESCE(SUM(CASE WHEN es_activo=1 AND substr(fecha_inicio, 1, 7)='2026-03' THEN 1 ELSE 0 END), 0) as tiene_marzo_2026,
                       MAX(CASE WHEN es_activo=1 THEN COALESCE(titulo_manual, titulo) END) as titulo_activo,
                       MAX(CASE WHEN es_activo=1 THEN fecha_inicio END) as fecha_activa
                FROM prestamos_excel_historial
                WHERE socio_numero=?
                  AND COALESCE(oculto_manual, 0)=0
                """,
                (item['numero'],),
            ).fetchone()
            item['total_bloques_excel'] = historial_excel['total_bloques'] if historial_excel else 0
            item['tiene_historial_excel'] = 1 if historial_excel and (historial_excel['total_bloques'] or 0) > 1 else 0
            item['tiene_prestamo_marzo_2026'] = 1 if historial_excel and (historial_excel['tiene_marzo_2026'] or 0) > 0 else 0
            item['fecha_prestamo_vista'] = historial_excel['fecha_activa'] if historial_excel and historial_excel['fecha_activa'] else item['fecha_prestamo']
            item['titulo_prestamo_excel_activo'] = loan_title_visible(
                item['fecha_prestamo_vista'],
                historial_excel['titulo_activo'] if historial_excel and historial_excel['titulo_activo'] else None,
            )
            item['periodo_cerrado'] = periodo_cerrado
            item['periodo_cerrado_label'] = format_period_label(periodo_cerrado, periodo_cerrado) if periodo_cerrado else ''
            item['incluye_periodo_cerrado'] = False
            if periodo_cerrado:
                tiene_cuota_cerrada = conn.execute(
                    """
                    SELECT 1
                    FROM cuotas
                    WHERE socio_numero=?
                      AND plazo BETWEEN 1 AND 240
                      AND substr(fecha, 1, 7)=?
                    LIMIT 1
                    """,
                    (item['numero'], periodo_cerrado),
                ).fetchone()
                tiene_movimiento_cerrado = conn.execute(
                    """
                    SELECT 1
                    FROM obligaciones_mensuales
                    WHERE periodo=?
                      AND socio_numero=?
                      AND (
                        COALESCE(cuota_prestamo, 0) > 0
                        OR substr(COALESCE(fecha_prestamo, ''), 1, 7)=?
                      )
                    LIMIT 1
                    """,
                    (periodo_cerrado, item['numero'], periodo_cerrado),
                ).fetchone()
                item['incluye_periodo_cerrado'] = bool(tiene_cuota_cerrada or tiene_movimiento_cerrado)
            prestamos.append(item)
    return render_template(
        'prestamos.html',
        prestamos=prestamos,
        resumen_prestamos=resumen_prestamos,
        resumen_historial_excel=resumen_historial_excel,
        periodo_cerrado=periodo_cerrado,
        periodo_cerrado_label=format_period_label(periodo_cerrado, periodo_cerrado) if periodo_cerrado else '',
    )


@bp.route('/aportaciones-mensuales')
@login_required
def aportaciones_mensuales():
    periodo = request.args.get('periodo', '').strip()
    with connect_db() as conn:
        if not periodo:
            periodo = get_default_period(conn)
            return redirect(url_for('main.aportaciones_mensuales', periodo=periodo))
        context = get_aportaciones_period_context(conn, periodo)
    response = make_response(render_template('aportaciones_mensuales.html', **context))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@bp.route('/aportaciones-mensuales/imprimible')
@login_required
def aportaciones_mensuales_imprimible():
    periodo = request.args.get('periodo', '').strip()
    with connect_db() as conn:
        if not periodo:
            periodo = get_default_period(conn)
        context = get_aportaciones_period_context(conn, periodo)
    context['periodo_label'] = format_period_label(periodo, periodo)
    context['fecha_emision'] = datetime.now().strftime('%d/%m/%Y %H:%M')
    response = make_response(render_template('aportaciones_mensuales_imprimible.html', **context))
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.headers["Content-Disposition"] = f"inline; filename=aportaciones_mensuales_{periodo}.html"
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@bp.route('/estado-cuenta')
@login_required
def estado_cuenta():
    numero = request.args.get('numero', type=int)
    with connect_db() as conn:
        periodo_cerrado = get_latest_closed_period(conn)
        socios = conn.execute("SELECT numero, nombre FROM socios ORDER BY numero").fetchall()
        socio = None
        cuotas = []
        resumen = None
        if numero:
            socio = conn.execute("""
                SELECT s.*,
                       COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
                FROM socios s
                LEFT JOIN cuotas c ON c.socio_numero = s.numero
                WHERE s.numero=?
                GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
            """, (numero,)).fetchone()
            cuotas = conn.execute("SELECT * FROM cuotas WHERE socio_numero=? ORDER BY fecha", (numero,)).fetchall()
            saldo_hist = conn.execute(
                """
                SELECT saldo
                FROM saldo_historico_mensual
                WHERE socio_numero=?
                ORDER BY periodo DESC
                LIMIT 1
                """,
                (numero,)
            ).fetchone()
            resumen = conn.execute("""
                SELECT COUNT(DISTINCT CASE WHEN plazo BETWEEN 1 AND 240 THEN plazo END) as total_cuotas,
                       COALESCE(SUM(CASE WHEN plazo BETWEEN 1 AND 240 THEN interes ELSE 0 END), 0) as intereses,
                       COALESCE(SUM(CASE WHEN plazo BETWEEN 1 AND 240 THEN abono_capital ELSE 0 END), 0) as capital,
                       COALESCE(SUM(CASE WHEN plazo BETWEEN 1 AND 240 THEN cuota ELSE 0 END), 0) as total_pagable,
                       MIN(CASE WHEN plazo BETWEEN 0 AND 240 THEN fecha END) as primera_fecha,
                       MAX(CASE WHEN plazo BETWEEN 0 AND 240 THEN fecha END) as ultima_fecha
                FROM cuotas
                WHERE socio_numero=?
            """, (numero,)).fetchone()
            saldo_actual = saldo_hist['saldo'] if saldo_hist else (socio['saldo'] if socio else 0)
        else:
            saldo_actual = 0
    return render_template('estado_cuenta.html', socios=socios, socio=socio, cuotas=annotate_schedule_rows(cuotas, periodo_cerrado), numero=numero, resumen=resumen, saldo_actual=saldo_actual, periodo_cerrado=periodo_cerrado)


@bp.route('/estado-cuenta/imprimible')
@login_required
def estado_cuenta_imprimible():
    numero = request.args.get('numero', type=int)
    with connect_db() as conn:
        periodo_cerrado = get_latest_closed_period(conn)
        periodo_financiero = get_latest_financial_period(conn)
        socio = conn.execute("""
            SELECT s.*,
                   COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
            FROM socios s
            LEFT JOIN cuotas c ON c.socio_numero = s.numero
            WHERE s.numero=?
            GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
        """, (numero,)).fetchone()
        cuotas = conn.execute("SELECT * FROM cuotas WHERE socio_numero=? ORDER BY fecha", (numero,)).fetchall()
        saldo_hist = conn.execute(
            """
            SELECT saldo
            FROM saldo_historico_mensual
            WHERE socio_numero=?
            ORDER BY periodo DESC
            LIMIT 1
            """,
            (numero,)
        ).fetchone()
        resumen = conn.execute("""
            SELECT COUNT(DISTINCT CASE WHEN plazo BETWEEN 1 AND 240 THEN plazo END) as total_cuotas,
                   COALESCE(SUM(CASE WHEN plazo BETWEEN 1 AND 240 THEN interes ELSE 0 END), 0) as intereses,
                   COALESCE(SUM(CASE WHEN plazo BETWEEN 1 AND 240 THEN abono_capital ELSE 0 END), 0) as capital
                   ,COALESCE(SUM(CASE WHEN plazo BETWEEN 1 AND 240 THEN cuota ELSE 0 END), 0) as total_pagable,
                   MIN(CASE WHEN plazo BETWEEN 0 AND 240 THEN fecha END) as primera_fecha,
                   MAX(CASE WHEN plazo BETWEEN 0 AND 240 THEN fecha END) as ultima_fecha
            FROM cuotas
            WHERE socio_numero=?
        """, (numero,)).fetchone()
    saldo_actual = saldo_hist['saldo'] if saldo_hist else (socio['saldo'] if socio else 0)
    return render_template(
        'estado_cuenta_imprimible.html',
        socio=socio,
        cuotas=annotate_schedule_rows(cuotas, periodo_cerrado),
        resumen=resumen,
        saldo_actual=saldo_actual,
        periodo_cerrado=periodo_cerrado,
        periodo_corte=periodo_financiero or periodo_cerrado,
        periodo_corte_label=format_period_label(periodo_financiero or periodo_cerrado, 'Sin corte financiero'),
        fecha_emision=datetime.now().strftime('%d/%m/%Y %H:%M'),
    )


@bp.route('/fondo-total')
@login_required
def fondo_total():
    with connect_db() as conn:
        periodo_actual = get_default_period(conn)
        ensure_monthly_collections(conn, periodo_actual)
        periodo_financiero = get_latest_financial_period(conn) or periodo_actual
        snapshot_financiero = get_financial_snapshot_for_period(conn, periodo_financiero) or {
            'periodo': periodo_financiero,
            'saldo_actual_total': 0,
            'acciones_por_socio': 0,
            'total_socios': 0,
        }
        try:
            total_prestamo_acumulado = float(
                get_config_value(conn, 'total_prestamo_acumulado_oficial', 0) or 0
            )
        except Exception:
            total_prestamo_acumulado = 0.0

        periodo_vigente = conn.execute(
            """
            SELECT *
            FROM periodos
            WHERE periodo=?
            """,
            (periodo_actual,),
        ).fetchone()
        colocacion_vigente = get_period_placement_status(
            conn,
            periodo_actual,
            periodo_vigente['total_recaudado'] if periodo_vigente else 0,
        )
        mayor_saldo = conn.execute(
            """
            SELECT socio_numero as numero,
                   socio_nombre as nombre,
                   saldo_actual
            FROM obligaciones_mensuales
            WHERE periodo=?
            ORDER BY saldo_actual DESC, socio_numero ASC
            LIMIT 1
            """,
            (periodo_actual,),
        ).fetchone()
        historial_periodos = conn.execute(
            """
            SELECT periodo,
                   estado,
                   total_socios,
                   total_prestamos,
                   total_aportes,
                   total_recaudado,
                   total_colocado,
                   saldo_por_colocar
            FROM periodos
            ORDER BY periodo DESC
            LIMIT 12
            """
        ).fetchall()

    cartera_vigente_pct = round(
        ((snapshot_financiero['saldo_actual_total'] or 0) / (total_prestamo_acumulado or 1)) * 100,
        1,
    ) if (total_prestamo_acumulado or 0) > 0 else 0
    colocacion_pct = round(
        (((colocacion_vigente['total_colocado'] if colocacion_vigente else 0) / ((periodo_vigente['total_recaudado'] if periodo_vigente else 0) or 1)) * 100),
        1,
    ) if (periodo_vigente and (periodo_vigente['total_recaudado'] or 0) > 0) else 0
    return render_template(
        'fondo_total_resumen.html',
        periodo_financiero=periodo_financiero,
        snapshot_financiero=snapshot_financiero,
        total_prestamo_acumulado=total_prestamo_acumulado,
        periodo_actual=periodo_actual,
        periodo_vigente=periodo_vigente,
        colocacion_vigente=colocacion_vigente,
        historial_periodos=historial_periodos,
        cartera_vigente_pct=cartera_vigente_pct,
        colocacion_pct=colocacion_pct,
        mayor_saldo=mayor_saldo,
    )


@bp.route('/estado-general.pdf')
@login_required
def pdf_estado_general():
    with connect_db() as conn:
        periodo_actual = get_default_period(conn)
        ensure_monthly_collections(conn, periodo_actual)
        periodo_financiero = get_latest_financial_period(conn) or periodo_actual
        total_socios = conn.execute("SELECT COUNT(*) FROM socios").fetchone()[0]
        total_prestamos = conn.execute(
            """
            SELECT COUNT(*)
            FROM obligaciones_mensuales
            WHERE periodo=? AND COALESCE(saldo_actual, 0) > 0
            """,
            (periodo_actual,),
        ).fetchone()[0]
        morosos = 0
        saldo = conn.execute(
            """
            SELECT COALESCE(SUM(saldo_actual),0)
            FROM obligaciones_mensuales
            WHERE periodo=?
            """,
            (periodo_actual,),
        ).fetchone()[0]
        capital = conn.execute("SELECT COALESCE(SUM(abono_capital),0) FROM cuotas").fetchone()[0]
        intereses = conn.execute("SELECT COALESCE(SUM(interes),0) FROM cuotas").fetchone()[0]
        cuotas = conn.execute("SELECT COALESCE(SUM(cuota),0) FROM cuotas").fetchone()[0]
        promedio_saldo = (float(saldo or 0) / total_socios) if total_socios else 0
        total_financiero = float(saldo or 0) + float(capital or 0) + float(intereses or 0)
    html = render_template(
        'estado_general_pdf.html',
        total_socios=total_socios,
        total_prestamos=total_prestamos,
        morosos=morosos,
        saldo=saldo,
        capital=capital,
        intereses=intereses,
        cuotas=cuotas,
        promedio_saldo=promedio_saldo,
        total_financiero=total_financiero,
        periodo_actual=periodo_actual,
        periodo_financiero=periodo_financiero,
        periodo_label=format_period_label(periodo_financiero, periodo_financiero),
        fecha=datetime.now().strftime('%d/%m/%Y %H:%M'),
    )
    response = make_response(html)
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.headers["Content-Disposition"] = "inline; filename=estado_general_jawilvio_v10.html"
    return response


@bp.route('/reporte-imprimible')
@login_required
def reporte_imprimible():
    with connect_db() as conn:
        periodo_actual = get_default_period(conn)
        ensure_monthly_collections(conn, periodo_actual)
        reunion_periodo = conn.execute(
            """
            SELECT *
            FROM reuniones_mensuales
            WHERE periodo=?
            """,
            (periodo_actual,),
        ).fetchone()
        total_permisos_periodo = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM permisos_mensuales
                WHERE periodo=?
                """,
                (periodo_actual,),
            ).fetchone()[0]
            or 0
        )
        socios = conn.execute("""
            SELECT s.*,
                   COALESCE(om.saldo_actual, s.saldo, 0) as saldo_periodo,
                   COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real,
                   COALESCE(pm.total_permisos, 0) as permisos_periodo,
                   CASE WHEN rm.socio_numero IS NOT NULL THEN 1 ELSE 0 END as reunion_asignada,
                   rm.estado as reunion_estado,
                   rm.fecha_programada as reunion_fecha_programada
            FROM socios s
            LEFT JOIN cuotas c ON c.socio_numero = s.numero
            LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
            LEFT JOIN (
                SELECT socio_numero, COUNT(*) as total_permisos
                FROM permisos_mensuales
                WHERE periodo=?
                GROUP BY socio_numero
            ) pm ON pm.socio_numero = s.numero
            LEFT JOIN (
                SELECT socio_numero, estado, fecha_programada
                FROM reuniones_mensuales
                WHERE periodo=?
            ) rm ON rm.socio_numero = s.numero
            GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos,
                     om.saldo_actual, pm.total_permisos, rm.socio_numero, rm.estado, rm.fecha_programada
            ORDER BY s.numero
        """, (periodo_actual, periodo_actual, periodo_actual)).fetchall()
        cuotas = conn.execute("SELECT * FROM cuotas ORDER BY fecha LIMIT 50").fetchall()
        resumen = conn.execute("""
            SELECT COUNT(*) as socios,
                   COALESCE(SUM(saldo_actual),0) as saldo_total
            FROM obligaciones_mensuales
            WHERE periodo=?
        """, (periodo_actual,)).fetchone()
        total_saldo_listado = sum(float(s['saldo_periodo'] or 0) for s in socios)
        total_permisos_listados = sum(int(s['permisos_periodo'] or 0) for s in socios)
        lugar_reunion = '-'
        if reunion_periodo:
            partes_lugar = [reunion_periodo['tipo_via'] or '', reunion_periodo['direccion_reunion'] or '']
            lugar_reunion = ' '.join([parte.strip() for parte in partes_lugar if parte and parte.strip()]) or '-'
    return render_template(
        'reporte_imprimible.html',
        socios=socios,
        cuotas=cuotas,
        resumen=resumen,
        periodo_actual=periodo_actual,
        reunion_periodo=reunion_periodo,
        lugar_reunion=lugar_reunion,
        total_permisos_periodo=total_permisos_periodo,
        total_saldo_listado=total_saldo_listado,
        total_permisos_listados=total_permisos_listados,
        periodo_label=format_period_label(periodo_actual, periodo_actual),
        fecha_emision=datetime.now().strftime('%d/%m/%Y %H:%M'),
    )


@bp.route('/morosos.csv')
@login_required
def reporte_morosos_csv():
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['N°', 'Socio', 'Cuotas vencidas', 'Saldo'])
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=morosos_jawilvio_v10.csv"
    output.headers["Content-type"] = "text/csv; charset=utf-8"
    return output


@bp.route('/backup')
@login_required
def backup():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    db_path = current_app.config['DATABASE']
    db_primary = current_app.config.get('DATABASE_PRIMARY')
    db_fallback = current_app.config.get('DATABASE_FALLBACK')
    excel_path = current_app.config.get('EXCEL_PATH')
    photos_dir = current_app.config.get('SOCIOS_PHOTO_UPLOAD_DIR')
    permisos_dir = current_app.config.get('PERMISOS_UPLOAD_DIR')
    branding_dir = current_app.config.get('BRANDING_UPLOAD_DIR')

    zip_buffer = io.BytesIO()
    with connect_db() as conn:
        periodo_actual = get_default_period(conn)
        periodo_financiero = get_latest_financial_period(conn)
        periodo_cerrado = get_latest_closed_period(conn)
        total_socios = conn.execute("SELECT COUNT(*) FROM socios").fetchone()[0]
        total_usuarios = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total_periodos = conn.execute("SELECT COUNT(*) FROM periodos").fetchone()[0]
        total_prestamos_nuevos = conn.execute("SELECT COUNT(*) FROM prestamos_nuevos").fetchone()[0]
        total_reuniones = conn.execute("SELECT COUNT(*) FROM reuniones_mensuales").fetchone()[0]
        total_permisos = conn.execute("SELECT COUNT(*) FROM permisos_mensuales").fetchone()[0]
        snapshot_financiero = get_financial_snapshot_for_period(conn, periodo_financiero) if periodo_financiero else None

    metadata = {
        'exportado_en': datetime.now().isoformat(),
        'base_activa': os.path.basename(db_path) if db_path else None,
        'base_primaria': os.path.basename(db_primary) if db_primary else None,
        'base_fallback': os.path.basename(db_fallback) if db_fallback else None,
        'modo_recuperado': bool(current_app.config.get('DATABASE_RECOVERED')),
        'excel_activo': os.path.basename(excel_path) if excel_path else None,
        'excel_path': excel_path,
        'periodo_actual': periodo_actual,
        'ultimo_periodo_financiero': periodo_financiero,
        'ultimo_periodo_cerrado': periodo_cerrado,
        'resumen_financiero': snapshot_financiero,
        'conteos': {
            'socios': total_socios,
            'usuarios': total_usuarios,
            'periodos': total_periodos,
            'prestamos_nuevos': total_prestamos_nuevos,
            'reuniones_mensuales': total_reuniones,
            'permisos_mensuales': total_permisos,
        },
    }

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        if db_path and os.path.exists(db_path):
            zf.write(db_path, arcname=f"database/{os.path.basename(db_path)}")
        if excel_path and os.path.exists(excel_path):
            zf.write(excel_path, arcname=f"excel/{os.path.basename(excel_path)}")
        if photos_dir and os.path.isdir(photos_dir):
            for root, _, files in os.walk(photos_dir):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    rel_path = os.path.relpath(file_path, photos_dir)
                    zf.write(file_path, arcname=os.path.join('socios_fotos', rel_path).replace('\\', '/'))
        if permisos_dir and os.path.isdir(permisos_dir):
            for root, _, files in os.walk(permisos_dir):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    rel_path = os.path.relpath(file_path, permisos_dir)
                    zf.write(file_path, arcname=os.path.join('permisos_documentos', rel_path).replace('\\', '/'))
        if branding_dir and os.path.isdir(branding_dir):
            for root, _, files in os.walk(branding_dir):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    rel_path = os.path.relpath(file_path, branding_dir)
                    zf.write(file_path, arcname=os.path.join('branding', rel_path).replace('\\', '/'))
        zf.writestr('metadata/resumen.json', json.dumps(metadata, ensure_ascii=False, indent=2))

    output = make_response(zip_buffer.getvalue())
    output.headers["Content-Disposition"] = f"attachment; filename=jawilvio_v10_respaldo_{timestamp}.zip"
    output.headers["Content-type"] = "application/zip"
    return output


@bp.route('/restaurar-respaldo', methods=['GET', 'POST'])
@admin_required
def restaurar_respaldo():
    db_path = current_app.config['DATABASE']
    excel_path = current_app.config.get('EXCEL_PATH')
    photos_dir = current_app.config.get('SOCIOS_PHOTO_UPLOAD_DIR')
    permisos_dir = current_app.config.get('PERMISOS_UPLOAD_DIR')
    branding_dir = current_app.config.get('BRANDING_UPLOAD_DIR')
    restore_backup_dir = os.path.join(current_app.instance_path, 'restore_backups')

    if request.method == 'POST':
        restore_mode = request.form.get('restore_mode', 'full').strip().lower()
        if restore_mode not in ('database_only', 'full'):
            restore_mode = 'full'
        backup_file = request.files.get('backup_file')
        if not backup_file or not backup_file.filename:
            flash('Selecciona un archivo de respaldo en formato ZIP.')
            return redirect(url_for('main.restaurar_respaldo'))
        if not backup_file.filename.lower().endswith('.zip'):
            flash('El respaldo debe estar en formato ZIP.')
            return redirect(url_for('main.restaurar_respaldo'))

        try:
            zip_data = backup_file.read()
            zip_buffer = io.BytesIO(zip_data)
            with zipfile.ZipFile(zip_buffer, 'r') as zf:
                names = zf.namelist()
                db_entries = [name for name in names if name.startswith('database/') and not name.endswith('/')]
                if not db_entries:
                    flash('El respaldo no contiene una base de datos válida.')
                    return redirect(url_for('main.restaurar_respaldo'))

                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                restored_parts = []
                warnings = []
                os.makedirs(restore_backup_dir, exist_ok=True)

                if os.path.exists(db_path):
                    with open(db_path, 'rb') as current_db:
                        db_backup_path = os.path.join(
                            restore_backup_dir,
                            f"{os.path.basename(db_path)}.before_restore_{timestamp}.bak",
                        )
                        with open(db_backup_path, 'wb') as db_backup:
                            db_backup.write(current_db.read())

                with open(db_path, 'wb') as restored_db:
                    restored_db.write(zf.read(db_entries[0]))
                restored_parts.append('base de datos')

                if restore_mode == 'full':
                    excel_entries = [name for name in names if name.startswith('excel/') and not name.endswith('/')]
                    if excel_entries and excel_path:
                        try:
                            os.makedirs(os.path.dirname(excel_path), exist_ok=True)
                            if os.path.exists(excel_path):
                                with open(excel_path, 'rb') as current_excel:
                                    excel_backup_path = os.path.join(
                                        restore_backup_dir,
                                        f"{os.path.basename(excel_path)}.before_restore_{timestamp}.bak",
                                    )
                                    with open(excel_backup_path, 'wb') as excel_backup:
                                        excel_backup.write(current_excel.read())
                            with open(excel_path, 'wb') as restored_excel:
                                restored_excel.write(zf.read(excel_entries[0]))
                            restored_parts.append('Excel')
                        except Exception:
                            warnings.append('Excel no restaurado')

                    restored_photos = 0
                    if photos_dir:
                        os.makedirs(photos_dir, exist_ok=True)
                        for name in names:
                            if not name.startswith('socios_fotos/') or name.endswith('/'):
                                continue
                            rel_path = os.path.normpath(name.split('/', 1)[1])
                            if rel_path.startswith('..') or os.path.isabs(rel_path):
                                continue
                            photo_target = os.path.join(photos_dir, rel_path)
                            os.makedirs(os.path.dirname(photo_target), exist_ok=True)
                            with open(photo_target, 'wb') as restored_photo:
                                restored_photo.write(zf.read(name))
                            restored_photos += 1
                        if restored_photos:
                            restored_parts.append(f'fotos de socios ({restored_photos})')

                    restored_docs = 0
                    if permisos_dir:
                        os.makedirs(permisos_dir, exist_ok=True)
                        for name in names:
                            if not name.startswith('permisos_documentos/') or name.endswith('/'):
                                continue
                            rel_path = os.path.normpath(name.split('/', 1)[1])
                            if rel_path.startswith('..') or os.path.isabs(rel_path):
                                continue
                            doc_target = os.path.join(permisos_dir, rel_path)
                            os.makedirs(os.path.dirname(doc_target), exist_ok=True)
                            with open(doc_target, 'wb') as restored_doc:
                                restored_doc.write(zf.read(name))
                            restored_docs += 1
                        if restored_docs:
                            restored_parts.append(f'documentos de permisos ({restored_docs})')

                    restored_branding = 0
                    if branding_dir:
                        os.makedirs(branding_dir, exist_ok=True)
                        for name in names:
                            if not name.startswith('branding/') or name.endswith('/'):
                                continue
                            rel_path = os.path.normpath(name.split('/', 1)[1])
                            if rel_path.startswith('..') or os.path.isabs(rel_path):
                                continue
                            branding_target = os.path.join(branding_dir, rel_path)
                            os.makedirs(os.path.dirname(branding_target), exist_ok=True)
                            with open(branding_target, 'wb') as restored_branding_file:
                                restored_branding_file.write(zf.read(name))
                            restored_branding += 1
                        if restored_branding:
                            restored_parts.append(f'branding ({restored_branding})')

            try:
                with connect_db() as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO meta(clave, valor) VALUES(?, ?)",
                        ('excel_importado', os.path.basename(excel_path) if excel_path else ''),
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO meta(clave, valor) VALUES(?, ?)",
                        ('excel_sync_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                    )
                    conn.commit()
            except Exception:
                pass

            try:
                log_action(
                    session.get('username', 'admin'),
                    'Restauración de respaldo',
                    f"Modo: {restore_mode}. Se restauró: {', '.join(restored_parts)}",
                )
            except Exception:
                pass

            flash_message = f"Respaldo restaurado correctamente: {', '.join(restored_parts)}."
            if warnings:
                flash_message += f" Avisos: {', '.join(warnings)}."
            flash(flash_message)
            return redirect(url_for('main.restaurar_respaldo'))
        except zipfile.BadZipFile:
            flash('El archivo seleccionado no es un ZIP válido.')
            return redirect(url_for('main.restaurar_respaldo'))
        except Exception as exc:
            flash(f'No se pudo restaurar el respaldo: {exc}')
            return redirect(url_for('main.restaurar_respaldo'))

    current_excel_name = os.path.basename(excel_path) if excel_path else '-'
    photo_count = 0
    if photos_dir and os.path.isdir(photos_dir):
        for _, _, files in os.walk(photos_dir):
            photo_count += len(files)
    restore_info = {
        'database_name': os.path.basename(db_path) if db_path else '-',
        'excel_name': current_excel_name,
        'modo_recuperado': bool(current_app.config.get('DATABASE_RECOVERED')),
        'photo_count': photo_count,
    }
    return render_template('restaurar_respaldo.html', restore_info=restore_info)


# Registro modular progresivo: estos módulos ya salen del monolito principal
# para mantener el blueprint único sin seguir inflando este archivo legacy.
from .route_modules import api_routes  # noqa: E402,F401
from .route_modules import admin_routes  # noqa: E402,F401
from .route_modules import asistencia_routes  # noqa: E402,F401
from .route_modules import dashboard_routes  # noqa: E402,F401
from .route_modules import monthly_routes  # noqa: E402,F401
from .route_modules import report_routes  # noqa: E402,F401
from .route_modules import reuniones_routes  # noqa: E402,F401
from .route_modules import socios_routes  # noqa: E402,F401

