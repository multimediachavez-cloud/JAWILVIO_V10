from flask import Blueprint, render_template, request, redirect, url_for, session, current_app, make_response, flash
import sqlite3, csv, io, json, zipfile
import os
import re
from functools import wraps
from datetime import datetime, date
import calendar
from werkzeug.utils import secure_filename
from .db import import_excel_if_needed
from flask import Blueprint, render_template, request, redirect, url_for, session, current_app, make_response, flash, jsonify
from flask import Response

bp = Blueprint('main', __name__)

REUNION_TIPOS_VIA = [
    ('Jr.', 'Jirón'),
    ('Psje.', 'Pasaje'),
    ('Av.', 'Avenida'),
]
PERMISO_MOTIVOS = ['Personal', 'Familiar', 'Salud', 'Trabajo', 'Otros']
PERMISO_DOCUMENT_EXTENSIONS = {'.pdf', '.doc', '.docx'}

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
        'main.estado_cuenta', 'main.estado_cuenta_imprimible', 'main.reuniones', 'main.reportes',
        'main.saldo_actual', 'main.fondo_total', 'main.reporte_mensual_csv', 'main.graficos',
        'main.pdf_estado_general', 'main.reporte_imprimible', 'main.reporte_morosos_csv',
        'main.mi_cuenta',
    },
    'Secretario': {
        'main.dashboard', 'main.socios', 'main.socio_detalle', 'main.prestamo_excel_detalle',
        'main.historial_prestamo_detalle', 'main.historial_prestamo_comparar', 'main.prestamos',
        'main.estado_cuenta', 'main.estado_cuenta_imprimible', 'main.reuniones', 'main.asistencia',
        'main.reportes', 'main.saldo_actual', 'main.graficos', 'main.pdf_estado_general',
        'main.reporte_imprimible', 'main.reporte_morosos_csv', 'main.mi_cuenta',
    },
    'Consulta': {
        'main.dashboard', 'main.socios', 'main.socio_detalle', 'main.prestamo_excel_detalle',
        'main.historial_prestamo_detalle', 'main.historial_prestamo_comparar', 'main.prestamos',
        'main.aportaciones_mensuales', 'main.aportaciones_mensuales_imprimible', 'main.cierre_mensual', 'main.nuevos_prestamos',
        'main.estado_cuenta', 'main.estado_cuenta_imprimible', 'main.reuniones', 'main.asistencia',
        'main.reportes', 'main.saldo_actual', 'main.fondo_total', 'main.reporte_mensual_csv',
        'main.graficos', 'main.pdf_estado_general', 'main.reporte_imprimible', 'main.reporte_morosos_csv',
        'main.mi_cuenta',
    },
}

ROLE_WRITE_ACCESS = {
    'Administrador': {'*'},
    'Tesorero': {'main.cierre_mensual', 'main.nuevos_prestamos'},
    'Secretario': {'main.asistencia', 'main.reuniones'},
    'Consulta': set(),
}


def connect_db():
    conn = sqlite3.connect(current_app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn


def build_socio_photo_url(foto):
    if not foto:
        return None
    if foto.startswith('http://') or foto.startswith('https://') or foto.startswith('/'):
        return foto
    return url_for('static', filename=foto.replace('\\', '/'))


def build_permiso_document_url(documento):
    if not documento:
        return None
    if documento.startswith('http://') or documento.startswith('https://') or documento.startswith('/'):
        return documento
    return url_for('static', filename=documento.replace('\\', '/'))


def save_socio_photo(file_storage, socio_numero):
    if not file_storage or not file_storage.filename:
        return None
    filename = secure_filename(file_storage.filename)
    if not filename:
        return None
    _, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext not in {'.jpg', '.jpeg', '.png', '.webp'}:
        return False
    upload_dir = current_app.config['SOCIOS_PHOTO_UPLOAD_DIR']
    os.makedirs(upload_dir, exist_ok=True)
    final_name = f"socio_{socio_numero}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"
    final_path = os.path.join(upload_dir, final_name)
    file_storage.save(final_path)
    return f"uploads/socios/{final_name}"


def save_permiso_document(file_storage, periodo, socio_numero):
    if not file_storage or not file_storage.filename:
        return None
    filename = secure_filename(file_storage.filename)
    if not filename:
        return None
    _, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext not in PERMISO_DOCUMENT_EXTENSIONS:
        return False
    upload_dir = current_app.config['PERMISOS_UPLOAD_DIR']
    os.makedirs(upload_dir, exist_ok=True)
    final_name = f"permiso_{periodo}_{socio_numero}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"
    final_path = os.path.join(upload_dir, final_name)
    file_storage.save(final_path)
    return f"uploads/permisos/{final_name}"


def delete_permiso_document_if_local(documento):
    if not documento or documento.startswith('http://') or documento.startswith('https://') or documento.startswith('/'):
        return
    full_path = os.path.join(current_app.static_folder, documento.replace('/', os.sep))
    if os.path.exists(full_path):
        try:
            os.remove(full_path)
        except OSError:
            pass


def delete_socio_photo_if_local(foto):
    if not foto or foto.startswith('http://') or foto.startswith('https://') or foto.startswith('/'):
        return
    full_path = os.path.join(current_app.static_folder, foto.replace('/', os.sep))
    if os.path.exists(full_path):
        try:
            os.remove(full_path)
        except OSError:
            pass


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
        flash('Tu rol no tiene permiso para realizar esta acción.')
    else:
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


def apply_excel_loan_as_active(conn, socio_numero, prestamo_excel_id, titulo_visible=None, saldo_base_ref=None, prestamo_adicional_ref=None):
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
    conn.execute(
        "UPDATE prestamos_excel_historial SET es_activo=CASE WHEN id=? THEN 1 ELSE 0 END WHERE socio_numero=?",
        (prestamo_excel_id, socio_numero),
    )
    conn.execute(
        "UPDATE prestamos_excel_historial SET titulo_manual=?, saldo_base_ref=COALESCE(?, saldo_base_ref), prestamo_adicional_ref=COALESCE(?, prestamo_adicional_ref) WHERE id=?",
        (titulo_visible, saldo_base_ref, prestamo_adicional_ref, prestamo_excel_id),
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
            prestamo['fecha_inicio'],
            prestamo['plazo_total'],
            prestamo['plazo_total'],
            socio_numero,
        ),
    )
    periodo_oficial = str(get_config_value(conn, 'resumen_financiero_periodo_oficial', '2026-03') or '2026-03')
    periodos = [row['periodo'] for row in conn.execute("SELECT periodo FROM periodos ORDER BY periodo").fetchall()]
    for periodo in periodos:
        if periodo == periodo_oficial:
            continue
        ensure_monthly_collections(conn, periodo)
    return prestamo


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


def log_action(usuario, accion, detalle=''):
    with connect_db() as conn:
        conn.execute("INSERT INTO auditoria(usuario, accion, detalle) VALUES(?,?,?)", (usuario, accion, detalle))
        conn.commit()


def get_default_period(conn):
    return datetime.now().strftime('%Y-%m')


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

    periodo_actual_sistema = get_default_period(conn)
    periodo_existente = conn.execute(
        "SELECT estado FROM periodos WHERE periodo=?",
        (periodo,),
    ).fetchone()
    obligaciones_existentes = conn.execute(
        "SELECT COUNT(*) FROM obligaciones_mensuales WHERE periodo=?",
        (periodo,),
    ).fetchone()[0]
    if (
        periodo_existente
        and periodo_existente['estado'] == 'Cerrado'
        and periodo < periodo_actual_sistema
        and obligaciones_existentes > 0
    ):
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
        return dict(
            app_nombre=cfg.get('nombre_asociacion', 'Asociación JAWILVIO'),
            app_ubicacion=cfg.get('ubicacion', 'Celendín, Cajamarca'),
            excel_importado=excel['valor'] if excel else '',
            excel_sync_at=excel_sync['valor'] if excel_sync else '',
            can_access=lambda endpoint, write=False: role_can_access_endpoint(session.get('role'), endpoint, write),
            current_role=session.get('role', ''),
        )
    except Exception:
        return dict(
            app_nombre='Asociación JAWILVIO',
            app_ubicacion='Celendín, Cajamarca',
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
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        with connect_db() as conn:
            user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        if user:
            if user['estado'] == 'Suspendido':
                error = 'Tu usuario está suspendido. Contacta al administrador.'
                return render_template('login.html', error=error)
            if user['password'] != password:
                error = 'Credenciales incorrectas'
                return render_template('login.html', error=error)
            with connect_db() as conn:
                conn.execute(
                    "UPDATE users SET ultimo_acceso=CURRENT_TIMESTAMP WHERE id=?",
                    (user['id'],),
                )
                conn.commit()
            session['user_id'] = user['id']
            session['username'] = user['username']
            session['role'] = user['role']
            log_action(user['username'], 'Inicio de sesión', f"Rol: {user['role']}")
            return redirect(url_for('main.dashboard'))
        error = 'Credenciales incorrectas'
    return render_template('login.html', error=error)


@bp.route('/logout')
def logout():
    usuario = session.get('username', 'desconocido')
    session.clear()
    try:
        log_action(usuario, 'Cierre de sesión')
    except Exception:
        pass
    return redirect(url_for('main.login'))


@bp.route('/api/socio/<int:socio_numero>', methods=['GET'])
def api_socio(socio_numero):
    with connect_db() as conn:
        socio = conn.execute(
            """
            SELECT numero, nombre
            FROM socios
            WHERE numero = ?
            """,
            (socio_numero,)
        ).fetchone()

        if socio is None:
            return jsonify({"error": "Socio no encontrado"}), 404

        periodo_actual = get_default_period(conn)
        ensure_monthly_collections(conn, periodo_actual)

        saldo = conn.execute(
            """
            SELECT saldo_actual
            FROM obligaciones_mensuales
            WHERE socio_numero = ? AND periodo = ?
            LIMIT 1
            """,
            (socio_numero, periodo_actual)
        ).fetchone()

    respuesta = {
    "id": socio["numero"],
    "nombre": socio["nombre"],
    "aporte_total": float(saldo["saldo_actual"]) if saldo else 0.0
    }

    return Response(
        json.dumps(respuesta, indent=4, ensure_ascii=False),
        mimetype="application/json"
    )

@bp.route('/api/socios', methods=['GET'])
def api_socios():
    with connect_db() as conn:
        periodo_actual = get_default_period(conn)
        ensure_monthly_collections(conn, periodo_actual)

        socios = conn.execute(
            """
            SELECT s.numero, s.nombre, COALESCE(o.saldo_actual, 0) AS aporte_total
            FROM socios s
            LEFT JOIN obligaciones_mensuales o
                ON o.socio_numero = s.numero AND o.periodo = ?
            ORDER BY s.numero ASC
            """,
            (periodo_actual,)
        ).fetchall()

    resultado = []
    for socio in socios:
        resultado.append({
            "id": socio["numero"],
            "nombre": socio["nombre"],
            "aporte_total": float(socio["aporte_total"] or 0)
        })

    return jsonify({
        "ok": True,
        "periodo": periodo_actual,
        "total": len(resultado),
        "socios": resultado
    })

@bp.route('/api/dashboard', methods=['GET'])
def api_dashboard():
    with connect_db() as conn:
        periodo_actual = get_default_period(conn)
        ensure_monthly_collections(conn, periodo_actual)
        cleanup_legacy_auto_reunion_assignment(conn, periodo_actual)

        total_socios = conn.execute("SELECT COUNT(*) FROM socios").fetchone()[0]

        prestamos_activos = conn.execute(
            """
            SELECT COUNT(*)
            FROM obligaciones_mensuales
            WHERE periodo=? AND COALESCE(saldo_actual, 0) > 0
            """,
            (periodo_actual,),
        ).fetchone()[0]

        saldo_total = conn.execute(
            """
            SELECT COALESCE(SUM(saldo_actual), 0)
            FROM obligaciones_mensuales
            WHERE periodo=?
            """,
            (periodo_actual,),
        ).fetchone()[0] or 0

        reuniones = conn.execute(
            "SELECT COUNT(*) FROM reuniones_mensuales WHERE periodo=?",
            (periodo_actual,),
        ).fetchone()[0]

        permisos = conn.execute(
            "SELECT COUNT(*) FROM permisos_mensuales WHERE periodo=?",
            (periodo_actual,),
        ).fetchone()[0]

        capital_recuperado = conn.execute(
            "SELECT COALESCE(SUM(abono_capital),0) FROM cuotas WHERE plazo > 0"
        ).fetchone()[0] or 0

        intereses = conn.execute(
            "SELECT COALESCE(SUM(interes),0) FROM cuotas WHERE plazo > 0"
        ).fetchone()[0] or 0

        cuotas_total = conn.execute(
            "SELECT COALESCE(SUM(cuota),0) FROM cuotas WHERE plazo > 0"
        ).fetchone()[0] or 0

        cuota_promedio = conn.execute(
            "SELECT COALESCE(AVG(cuota),0) FROM cuotas WHERE cuota > 0"
        ).fetchone()[0] or 0

    return jsonify({
        "ok": True,
        "periodo_actual": periodo_actual,
        "totales": {
            "total_socios": total_socios,
            "prestamos_activos": prestamos_activos,
            "saldo_total": float(saldo_total),
            "reuniones": reuniones,
            "permisos": permisos,
            "capital_recuperado": float(capital_recuperado),
            "intereses": float(intereses),
            "cuotas_total": float(cuotas_total),
            "cuota_promedio": float(cuota_promedio)
        }
    })
    
@bp.route('/dashboard')
@login_required
def dashboard():
    with connect_db() as conn:
        periodo_actual = get_default_period(conn)
        ensure_monthly_collections(conn, periodo_actual)
        cleanup_legacy_auto_reunion_assignment(conn, periodo_actual)
        total_socios = conn.execute("SELECT COUNT(*) FROM socios").fetchone()[0]
        prestamos_activos = conn.execute(
            """
            SELECT COUNT(*)
            FROM obligaciones_mensuales
            WHERE periodo=? AND COALESCE(saldo_actual, 0) > 0
            """,
            (periodo_actual,),
        ).fetchone()[0]
        saldo_total = conn.execute(
            """
            SELECT COALESCE(SUM(saldo_actual), 0)
            FROM obligaciones_mensuales
            WHERE periodo=?
            """,
            (periodo_actual,),
        ).fetchone()[0]
        alertas = 0
        socios = conn.execute("""
            SELECT s.*,
                   COALESCE(om.saldo_actual, s.saldo, 0) as saldo_periodo,
                   COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
            FROM socios s
            LEFT JOIN cuotas c ON c.socio_numero = s.numero
            LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
            GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
            ORDER BY s.numero
            LIMIT 8
        """, (periodo_actual,)).fetchall()
        reuniones = conn.execute(
            "SELECT COUNT(*) FROM reuniones_mensuales WHERE periodo=?",
            (periodo_actual,),
        ).fetchone()[0]
        permisos = conn.execute(
            "SELECT COUNT(*) FROM permisos_mensuales WHERE periodo=?",
            (periodo_actual,),
        ).fetchone()[0]
        capital_recuperado = conn.execute("SELECT COALESCE(SUM(abono_capital),0) FROM cuotas WHERE plazo > 0").fetchone()[0]
        intereses = conn.execute("SELECT COALESCE(SUM(interes),0) FROM cuotas WHERE plazo > 0").fetchone()[0]
        cuotas_total = conn.execute("SELECT COALESCE(SUM(cuota),0) FROM cuotas WHERE plazo > 0").fetchone()[0]
        cuota_promedio = conn.execute("SELECT COALESCE(AVG(cuota),0) FROM cuotas WHERE cuota > 0").fetchone()[0]
        periodo_financiero_panel = get_latest_financial_period(conn) or periodo_actual
        snapshot_financiero = get_financial_snapshot_for_period(conn, periodo_financiero_panel)
        if snapshot_financiero:
            saldo_actual_total_oficial = float(snapshot_financiero['saldo_actual_total'] or 0)
            acciones_por_socio_oficial = float(snapshot_financiero['acciones_por_socio'] or 0)
        else:
            try:
                saldo_actual_total_oficial = float(get_config_value(conn, 'saldo_actual_total_oficial', saldo_total or 0) or 0)
            except Exception:
                saldo_actual_total_oficial = float(saldo_total or 0)
            try:
                acciones_por_socio_oficial = float(get_config_value(conn, 'acciones_por_socio_oficial', 0) or 0)
            except Exception:
                acciones_por_socio_oficial = 0
        try:
            total_prestamo_acumulado = float(get_config_value(conn, 'total_prestamo_acumulado_oficial', cuotas_total or 0) or 0)
        except Exception:
            total_prestamo_acumulado = float(cuotas_total or 0)
        morosos = 0
        top_morosos = []
        resumen_mensual = conn.execute("""
            SELECT substr(fecha,1,7) as periodo,
                   ROUND(SUM(interes),2) as intereses,
                   ROUND(SUM(abono_capital),2) as capital,
                   ROUND(SUM(cuota),2) as cuotas
            FROM cuotas
            WHERE fecha IS NOT NULL
            GROUP BY substr(fecha,1,7)
            ORDER BY periodo DESC
            LIMIT 6
        """).fetchall()
        import_status = conn.execute("SELECT valor FROM meta WHERE clave='import_status'").fetchone()
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
        detalle_periodo = conn.execute(
            """
            SELECT socio_numero, socio_nombre, cuota_prestamo, aporte_mensual, total_mes, saldo_actual
            FROM obligaciones_mensuales
            WHERE periodo=?
            ORDER BY total_mes DESC, socio_numero ASC
            LIMIT 8
            """,
            (periodo_actual,),
        ).fetchall()
        historial_periodos = conn.execute(
            """
            SELECT periodo, estado, total_recaudado, total_colocado, saldo_por_colocar
            FROM periodos
            ORDER BY periodo DESC
            LIMIT 6
            """
        ).fetchall()
        prestamos_periodo = conn.execute(
            """
            SELECT COUNT(*) as total,
                   COALESCE(SUM(CASE WHEN estado='Reservado' THEN 1 ELSE 0 END), 0) as reservados,
                   COALESCE(SUM(CASE WHEN estado='Aprobado' THEN 1 ELSE 0 END), 0) as aprobados
            FROM prestamos_nuevos
            WHERE periodo=?
            """,
            (periodo_actual,),
        ).fetchone()
        periodo_anterior = conn.execute(
            """
            SELECT periodo, estado, total_recaudado, total_colocado, saldo_por_colocar
            FROM periodos
            WHERE periodo < ?
            ORDER BY periodo DESC
            LIMIT 1
            """,
            (periodo_actual,),
        ).fetchone()
        top_saldos = conn.execute(
            """
            SELECT s.numero,
                   s.nombre,
                   COALESCE(om.saldo_actual, s.saldo, 0) as saldo_actual,
                   COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
            FROM socios s
            LEFT JOIN cuotas c ON c.socio_numero = s.numero
            LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
            GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
            ORDER BY COALESCE(om.saldo_actual, s.saldo, 0) DESC, s.numero ASC
            LIMIT 5
            """
            ,
            (periodo_actual,),
        ).fetchall()
        resumen_prestamos_multiples = conn.execute(
            """
            SELECT COUNT(*) as socios_multiples
            FROM (
                SELECT socio_numero
                FROM prestamos_excel_historial
                WHERE COALESCE(oculto_manual, 0)=0
                GROUP BY socio_numero
                HAVING COUNT(*) > 1
            ) x
            """
        ).fetchone()

    fondo_total = saldo_total or 0
    saldo_actual_total_panel = saldo_actual_total_oficial or fondo_total
    max_referencia = max(fondo_total, capital_recuperado or 0, intereses or 0, cuotas_total or 0, 1)
    barras = {
        'saldo_total': round((fondo_total / max_referencia) * 100, 2),
        'capital_recuperado': round(((capital_recuperado or 0) / max_referencia) * 100, 2),
        'intereses': round(((intereses or 0) / max_referencia) * 100, 2),
        'cuotas_total': round(((cuotas_total or 0) / max_referencia) * 100, 2),
    }
    periodo_actual_resumen = {
        'periodo': periodo_actual,
        'estado': periodo_vigente['estado'] if periodo_vigente else 'Abierto',
        'socios': periodo_vigente['total_socios'] if periodo_vigente else 0,
        'total_prestamos': periodo_vigente['total_prestamos'] if periodo_vigente else 0,
        'total_aportes': periodo_vigente['total_aportes'] if periodo_vigente else 0,
        'total_recaudado': periodo_vigente['total_recaudado'] if periodo_vigente else 0,
        'total_colocado': colocacion_vigente['total_colocado'] if colocacion_vigente else 0,
        'saldo_por_colocar': colocacion_vigente['saldo_por_colocar'] if colocacion_vigente else 0,
        'colocacion_completa': (colocacion_vigente['saldo_por_colocar'] if colocacion_vigente else 0) <= 0.0001,
        'porcentaje_colocado': round(
            (((colocacion_vigente['total_colocado'] if colocacion_vigente else 0) / ((periodo_vigente['total_recaudado'] if periodo_vigente else 0) or 1)) * 100),
            1,
        ) if (periodo_vigente and (periodo_vigente['total_recaudado'] or 0) > 0) else 0,
        'promedio_por_socio': round(
            ((periodo_vigente['total_recaudado'] if periodo_vigente else 0) / ((periodo_vigente['total_socios'] if periodo_vigente else 0) or 1)),
            1,
        ) if (periodo_vigente and (periodo_vigente['total_socios'] or 0) > 0) else 0,
    }
    mayor_total_mes = detalle_periodo[0] if detalle_periodo else None
    mayor_saldo = top_saldos[0] if top_saldos else None
    colocacion_counts = colocacion_vigente['origin_counts'] if colocacion_vigente else {'excel': 0, 'app_aprobado': 0, 'app_reservado': 0}
    acciones_por_socio = acciones_por_socio_oficial or (round((saldo_actual_total_panel / total_socios), 1) if total_socios else 0)
    cartera_vigente_pct = round(((fondo_total or 0) / (total_prestamo_acumulado or 1)) * 100, 1) if (total_prestamo_acumulado or 0) > 0 else 0
    capital_recuperado_pct = round(((capital_recuperado or 0) / (total_prestamo_acumulado or 1)) * 100, 1) if (total_prestamo_acumulado or 0) > 0 else 0
    intereses_pct = round(((intereses or 0) / (total_prestamo_acumulado or 1)) * 100, 1) if (total_prestamo_acumulado or 0) > 0 else 0
    brecha_colocacion_pct = round((((periodo_actual_resumen['saldo_por_colocar'] or 0) / ((periodo_actual_resumen['total_recaudado'] or 0) or 1)) * 100), 1) if (periodo_actual_resumen['total_recaudado'] or 0) > 0 else 0
    panel_financiero = {
        'saldo_actual_total': saldo_actual_total_panel,
        'total_prestamo_acumulado': total_prestamo_acumulado,
        'capital_recuperado': capital_recuperado,
        'intereses_proyectados': intereses,
        'fondo_mes': periodo_actual_resumen['total_recaudado'],
        'ya_colocado': periodo_actual_resumen['total_colocado'],
        'por_colocar': periodo_actual_resumen['saldo_por_colocar'],
        'acciones_por_socio': acciones_por_socio,
        'promedio_periodo': periodo_actual_resumen['promedio_por_socio'],
        'cartera_vigente_pct': cartera_vigente_pct,
        'capital_recuperado_pct': capital_recuperado_pct,
        'intereses_pct': intereses_pct,
        'colocacion_pct': periodo_actual_resumen['porcentaje_colocado'],
        'brecha_colocacion_pct': brecha_colocacion_pct,
    }

    return render_template(
        'dashboard_reporte.html',
        total_socios=total_socios,
        prestamos_activos=prestamos_activos,
        saldo_total=saldo_total,
        alertas=alertas,
        socios=socios,
        reuniones=reuniones,
        permisos=permisos,
        capital_recuperado=capital_recuperado,
        intereses=intereses,
        cuotas_total=cuotas_total,
        total_prestamo_acumulado=total_prestamo_acumulado,
        cuota_promedio=cuota_promedio,
        morosos=morosos,
        top_morosos=top_morosos,
        resumen_mensual=resumen_mensual,
        import_status=import_status['valor'] if import_status else 'Sin importación',
        fondo_total=fondo_total,
        saldo_actual_total_panel=saldo_actual_total_panel,
        barras=barras,
        periodo_actual_resumen=periodo_actual_resumen,
        detalle_periodo=detalle_periodo,
        historial_periodos=historial_periodos,
        prestamos_periodo=prestamos_periodo,
        periodo_anterior=periodo_anterior,
        colocacion_detalle_rows=colocacion_vigente['detalle_rows'] if colocacion_vigente else [],
        colocacion_counts=colocacion_counts,
        top_saldos=top_saldos,
        resumen_prestamos_multiples=resumen_prestamos_multiples,
        mayor_total_mes=mayor_total_mes,
        mayor_saldo=mayor_saldo,
        acciones_por_socio=acciones_por_socio,
        panel_financiero=panel_financiero,
        periodo_financiero_panel=periodo_financiero_panel,
    )


@bp.route('/socios', methods=['GET', 'POST'])
@login_required
def socios():
    q = request.args.get('q', '').strip()
    with connect_db() as conn:
        periodo_actual = get_default_period(conn)
        ensure_monthly_collections(conn, periodo_actual)
        if request.method == 'POST':
            if session.get('role') != 'Administrador':
                flash('Solo el administrador puede realizar esta acción.')
                return redirect(url_for('main.socios', q=q))
            action = request.form.get('action', '').strip()
            if action in {'create', 'update'}:
                numero = request.form.get('numero', '').strip()
                nombre = request.form.get('nombre', '').strip()
                dni = request.form.get('dni', '').strip()
                meses = request.form.get('meses', '').strip()
                fecha_prestamo = request.form.get('fecha_prestamo', '').strip()
                saldo = request.form.get('saldo', '').strip()
                mes_2026 = request.form.get('mes_2026', '').strip()
                numero_int = int(numero) if numero.isdigit() else None
                meses_int = int(meses) if meses.isdigit() else None
                try:
                    saldo_float = float(saldo) if saldo not in ('', None) else 0.0
                except Exception:
                    saldo_float = None
                if numero_int is None or not nombre:
                    flash('Completa al menos número y nombre del socio.')
                    return redirect(url_for('main.socios', q=q))
                if saldo_float is None:
                    flash('Ingresa un saldo válido.')
                    return redirect(url_for('main.socios', q=q))
                exists = conn.execute("SELECT id FROM socios WHERE numero=?", (numero_int,)).fetchone()
                if action == 'create' and exists:
                    flash('Ya existe un socio con ese número.')
                    return redirect(url_for('main.socios', q=q))
                foto_actual = conn.execute("SELECT foto FROM socios WHERE numero=?", (numero_int,)).fetchone()
                foto_guardada = foto_actual['foto'] if foto_actual else None
                nueva_foto = save_socio_photo(request.files.get('foto_archivo'), numero_int)
                if nueva_foto is False:
                    flash('La foto debe ser JPG, JPEG, PNG o WEBP.')
                    return redirect(url_for('main.socios', q=q, edit=numero_int if action == 'update' else None))
                if nueva_foto:
                    delete_socio_photo_if_local(foto_guardada)
                    foto_guardada = nueva_foto
                conn.execute(
                    """
                    INSERT OR REPLACE INTO socios(
                        id, numero, nombre, dni, foto, meses, plazo_balance, fecha_prestamo, saldo, mes_2026, reunion, permisos
                    )
                    VALUES(
                        COALESCE((SELECT id FROM socios WHERE numero=?), NULL), ?, ?, ?, ?, ?, 
                        COALESCE((SELECT plazo_balance FROM socios WHERE numero=?), NULL),
                        ?, ?, ?, 
                        COALESCE((SELECT reunion FROM socios WHERE numero=?), NULL),
                        COALESCE((SELECT permisos FROM socios WHERE numero=?), NULL)
                    )
                    """,
                    (
                        numero_int,
                        numero_int,
                        nombre,
                        dni or None,
                        foto_guardada,
                        meses_int,
                        numero_int,
                        fecha_prestamo or None,
                        saldo_float,
                        mes_2026 or None,
                        numero_int,
                        numero_int,
                    ),
                )
                conn.commit()
                log_action(session.get('username', 'admin'), 'Gestión de socios', f'Socio {"actualizado" if action == "update" else "creado"}: {numero_int} - {nombre}')
                flash(f'Socio {"actualizado" if action == "update" else "creado"} correctamente.')
                return redirect(url_for('main.socios', q=q))
            if action == 'delete':
                numero = request.form.get('numero', '').strip()
                numero_int = int(numero) if numero.isdigit() else None
                socio = conn.execute("SELECT nombre FROM socios WHERE numero=?", (numero_int,)).fetchone() if numero_int is not None else None
                if not socio:
                    flash('No se encontró el socio solicitado.')
                    return redirect(url_for('main.socios', q=q))
                historial_ids = [row['id'] for row in conn.execute("SELECT id FROM historial_prestamos_socios WHERE socio_numero=?", (numero_int,)).fetchall()]
                prestamo_excel_ids = [row['id'] for row in conn.execute("SELECT id FROM prestamos_excel_historial WHERE socio_numero=?", (numero_int,)).fetchall()]
                delete_socio_photo_if_local(conn.execute("SELECT foto FROM socios WHERE numero=?", (numero_int,)).fetchone()['foto'])
                for historial_id in historial_ids:
                    conn.execute("DELETE FROM historial_prestamos_socios_cuotas WHERE historial_id=?", (historial_id,))
                for prestamo_excel_id in prestamo_excel_ids:
                    conn.execute("DELETE FROM prestamos_excel_historial_cuotas WHERE prestamo_excel_id=?", (prestamo_excel_id,))
                conn.execute("DELETE FROM cuotas WHERE socio_numero=?", (numero_int,))
                conn.execute("DELETE FROM asistencia WHERE socio_numero=?", (numero_int,))
                conn.execute("DELETE FROM saldo_historico_mensual WHERE socio_numero=?", (numero_int,))
                conn.execute("DELETE FROM obligaciones_mensuales WHERE socio_numero=?", (numero_int,))
                conn.execute("DELETE FROM aportaciones_mensuales WHERE socio_numero=?", (numero_int,))
                conn.execute("DELETE FROM obligaciones_mensuales_override WHERE socio_numero=?", (numero_int,))
                conn.execute("DELETE FROM prioridad_colocacion_manual WHERE socio_numero=?", (numero_int,))
                conn.execute("DELETE FROM prestamos_nuevos WHERE socio_numero=?", (numero_int,))
                conn.execute("DELETE FROM historial_prestamos_socios WHERE socio_numero=?", (numero_int,))
                conn.execute("DELETE FROM prestamos_excel_historial WHERE socio_numero=?", (numero_int,))
                conn.execute("DELETE FROM socios WHERE numero=?", (numero_int,))
                conn.commit()
                log_action(session.get('username', 'admin'), 'Gestión de socios', f'Socio eliminado: {numero_int} - {socio["nombre"]}')
                flash('Socio eliminado correctamente.')
                return redirect(url_for('main.socios', q=q))
        if q:
            data = conn.execute(
                """
                SELECT s.*,
                       COALESCE(om.saldo_actual, s.saldo, 0) as saldo_periodo,
                       COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
                FROM socios s
                LEFT JOIN cuotas c ON c.socio_numero = s.numero
                LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
                WHERE s.nombre LIKE ? OR CAST(s.numero AS TEXT) LIKE ? OR COALESCE(s.dni, '') LIKE ?
                GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
                ORDER BY s.numero
                """,
                (periodo_actual, f'%{q}%', f'%{q}%', f'%{q}%'),
            ).fetchall()
        else:
            data = conn.execute("""
                SELECT s.*,
                       COALESCE(om.saldo_actual, s.saldo, 0) as saldo_periodo,
                       COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
                FROM socios s
                LEFT JOIN cuotas c ON c.socio_numero = s.numero
                LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
                GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
                ORDER BY s.numero
            """, (periodo_actual,)).fetchall()
        edit_numero = request.args.get('edit', '').strip()
        edit_numero_int = int(edit_numero) if edit_numero.isdigit() else None
        edit_socio = conn.execute(
            "SELECT * FROM socios WHERE numero=?",
            (edit_numero_int,),
        ).fetchone() if edit_numero_int is not None else None
    socios_render = []
    for row in data:
        row_dict = dict(row)
        row_dict['foto_url'] = build_socio_photo_url(row_dict.get('foto'))
        socios_render.append(row_dict)
    edit_socio_dict = dict(edit_socio) if edit_socio else None
    if edit_socio_dict:
        edit_socio_dict['foto_url'] = build_socio_photo_url(edit_socio_dict.get('foto'))
    return render_template('socios_gestion.html', socios=socios_render, q=q, edit_socio=edit_socio_dict)


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
            if action == 'aplicar_prestamo_excel':
                prestamo_excel_id = request.form.get('prestamo_excel_id', type=int)
                titulo_visible = request.form.get('titulo_visible', '').strip()
                saldo_base_ref_raw = request.form.get('saldo_base_ref', '').strip()
                prestamo_adicional_ref_raw = request.form.get('prestamo_adicional_ref', '').strip()
                try:
                    saldo_base_ref = round(float(saldo_base_ref_raw), 1) if saldo_base_ref_raw else None
                except Exception:
                    saldo_base_ref = None
                try:
                    prestamo_adicional_ref = round(float(prestamo_adicional_ref_raw), 1) if prestamo_adicional_ref_raw else None
                except Exception:
                    prestamo_adicional_ref = None
                prestamo_aplicado = apply_excel_loan_as_active(conn, numero, prestamo_excel_id, titulo_visible, saldo_base_ref, prestamo_adicional_ref)
                if not prestamo_aplicado:
                    flash('No se encontró el préstamo histórico solicitado.')
                    return redirect(url_for('main.socio_detalle', numero=numero))
                conn.commit()
                log_action(
                    session.get('username', 'admin'),
                    'Corrección de préstamo histórico',
                    f'Socio {numero}: se aplicó como vigente el bloque {prestamo_excel_id} con título visible "{titulo_visible or prestamo_aplicado["titulo"]}".'
                )
                flash('Préstamo histórico aplicado como vigente correctamente.')
                return redirect(url_for('main.socio_detalle', numero=numero))
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
            prestamos_excel[0] if prestamos_excel else None,
        )
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
            cronograma_historico = conn.execute(
                """
                SELECT *
                FROM prestamos_excel_historial_cuotas
                WHERE prestamo_excel_id=?
                ORDER BY plazo, fecha
                """,
                (prestamo_excel_referencia['id'],),
            ).fetchall()
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


@bp.route('/socio/<int:numero>/prestamo-excel/<int:prestamo_excel_id>')
@login_required
def prestamo_excel_detalle(numero, prestamo_excel_id):
    with connect_db() as conn:
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
    context['fecha_emision'] = datetime.now().strftime('%d/%m/%Y %H:%M')
    response = make_response(render_template('aportaciones_mensuales_imprimible.html', **context))
    response.headers["Content-Type"] = "text/html; charset=utf-8"
    response.headers["Content-Disposition"] = f"inline; filename=aportaciones_mensuales_{periodo}.html"
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@bp.route('/cierre-mensual', methods=['GET', 'POST'])
@login_required
def cierre_mensual():
    periodo = request.args.get('periodo', '').strip()
    with connect_db() as conn:
        if request.method == 'GET' and not periodo:
            periodo = get_default_period(conn)
            return redirect(url_for('main.cierre_mensual', periodo=periodo))
        periodo = periodo or get_default_period(conn)
        ensure_monthly_collections(conn, periodo)
        if request.method == 'POST':
            action = request.form.get('action', '').strip()
            periodo_form = request.form.get('periodo', '').strip() or periodo
            ensure_monthly_collections(conn, periodo_form)
            cierre_actual = conn.execute(
                "SELECT * FROM periodos WHERE periodo=?",
                (periodo_form,),
            ).fetchone()
            colocacion_actual = get_period_placement_status(conn, periodo_form, cierre_actual['total_recaudado'] if cierre_actual else 0)

            if action == 'cerrar_periodo':
                if not cierre_actual:
                    flash('No se encontró el período para cerrar.')
                elif (colocacion_actual['saldo_por_colocar'] or 0) > 0.0001:
                    flash(f'No se puede cerrar el período. Aún faltan S/ {colocacion_actual["saldo_por_colocar"]:.1f} por colocar.')
                else:
                    conn.execute(
                        """
                        UPDATE periodos
                        SET estado='Cerrado',
                            fecha_calculo=CURRENT_TIMESTAMP
                        WHERE periodo=?
                        """,
                        (periodo_form,),
                    )
                    sync_dashboard_financial_snapshot(conn, periodo_form)
                    conn.commit()
                    log_action(
                        session.get('username', 'sistema'),
                        'Cierre de período',
                        f'Período {periodo_form} cerrado manualmente.'
                    )
                    flash('Período cerrado correctamente.')
                return redirect(url_for('main.cierre_mensual', periodo=periodo_form))

            if action == 'abrir_mes_nuevo':
                siguiente_periodo = get_next_period(periodo_form)
                if not cierre_actual:
                    flash('No se encontró el período actual para abrir el siguiente mes.')
                    return redirect(url_for('main.cierre_mensual', periodo=periodo_form))
                if (colocacion_actual['saldo_por_colocar'] or 0) > 0.0001:
                    flash(f'No se puede abrir {siguiente_periodo}. Primero debes colocar todo el fondo de {periodo_form}. Aún faltan S/ {colocacion_actual["saldo_por_colocar"]:.1f}.')
                    return redirect(url_for('main.cierre_mensual', periodo=periodo_form))
                conn.execute(
                    """
                    UPDATE periodos
                    SET estado='Cerrado',
                        fecha_calculo=CURRENT_TIMESTAMP
                    WHERE periodo=?
                    """,
                    (periodo_form,),
                )
                ensure_monthly_collections(conn, siguiente_periodo)
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Apertura de período',
                    f'Se cerró {periodo_form} y se abrió el período {siguiente_periodo}.'
                )
                flash(f'Se cerró {periodo_form} y se abrió el nuevo período {siguiente_periodo}.')
                return redirect(url_for('main.cierre_mensual', periodo=siguiente_periodo))

        cierre = conn.execute(
            "SELECT * FROM periodos WHERE periodo=?",
            (periodo,),
        ).fetchone()
        periodo_historico_oficial = str(get_config_value(conn, 'resumen_financiero_periodo_oficial', '2026-03') or '2026-03')
        siguiente_periodo_operativo = get_next_period(periodo_historico_oficial)
        colocacion = get_period_placement_status(conn, periodo, cierre['total_recaudado'] if cierre else 0)
        historico = conn.execute(
            """
            SELECT *
            FROM periodos
            ORDER BY periodo DESC
            """
        ).fetchall()
    response = make_response(render_template(
        'cierre_mensual.html',
        periodo=periodo,
        cierre=cierre,
        historico=historico,
        total_colocado=colocacion['total_colocado'] or 0,
        saldo_por_colocar=colocacion['saldo_por_colocar'] or 0,
        colocacion_fuente=colocacion['fuente'],
        colocacion_excel_rows=colocacion['excel_rows'],
        colocacion_detalle_rows=colocacion['detalle_rows'],
        colocacion_origin_counts=colocacion['origin_counts'],
        colocacion_completa=(colocacion['saldo_por_colocar'] or 0) <= 0.0001,
        periodo_historico_oficial=periodo_historico_oficial,
        siguiente_periodo_operativo=siguiente_periodo_operativo,
        es_periodo_historico_oficial=(periodo == periodo_historico_oficial),
    ))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@bp.route('/nuevos-prestamos', methods=['GET', 'POST'])
@login_required
def nuevos_prestamos():
    selected_id = request.args.get('prestamo_id', type=int)
    periodo_query = request.args.get('periodo', '').strip()
    filtro_query = request.args.get('filtro', 'todos').strip().lower()
    with connect_db() as conn:
        if request.method == 'GET' and not periodo_query:
            periodo = get_default_period(conn)
            return redirect(url_for('main.nuevos_prestamos', periodo=periodo, filtro=filtro_query if filtro_query in ('todos', 'manuales', 'automaticos') else 'todos'))
        periodo = periodo_query or get_default_period(conn)
        filtro = filtro_query if filtro_query in ('todos', 'manuales', 'automaticos') else 'todos'
        ensure_monthly_collections(conn, periodo)

        if request.method == 'POST':
            action = request.form.get('action', '').strip()
            periodo = request.form.get('periodo', '').strip() or periodo
            filtro_post = request.form.get('filtro', filtro).strip().lower()
            filtro = filtro_post if filtro_post in ('todos', 'manuales', 'automaticos') else 'todos'
            ensure_monthly_collections(conn, periodo)
            tasa_mensual = float(get_config_value(conn, 'tasa_prestamo_mensual', '0.01'))
            min_cuotas = int(float(get_config_value(conn, 'min_cuotas_prestamo', '12')))
            max_cuotas = int(float(get_config_value(conn, 'max_cuotas_prestamo', '84')))

            if action == 'guardar_prioridades':
                conn.execute("DELETE FROM prioridad_colocacion_manual WHERE periodo=?", (periodo,))
                socios_ids = conn.execute("SELECT numero FROM socios ORDER BY numero").fetchall()
                guardadas = 0
                for row in socios_ids:
                    raw = request.form.get(f'prioridad_{row["numero"]}', '').strip()
                    if raw.isdigit():
                        prioridad = int(raw)
                        if prioridad > 0:
                            conn.execute(
                                "INSERT OR REPLACE INTO prioridad_colocacion_manual(periodo, socio_numero, prioridad) VALUES(?,?,?)",
                                (periodo, row['numero'], prioridad),
                            )
                            guardadas += 1
                conn.commit()
                flash(f'Orden manual guardado. Prioridades actualizadas: {guardadas}.')
                return redirect(url_for('main.nuevos_prestamos', periodo=periodo, filtro=filtro))

            if action == 'limpiar_prioridades':
                conn.execute("DELETE FROM prioridad_colocacion_manual WHERE periodo=?", (periodo,))
                conn.commit()
                flash('El orden manual se limpió para este período.')
                return redirect(url_for('main.nuevos_prestamos', periodo=periodo, filtro=filtro))

            if action in ('subir_prioridad', 'bajar_prioridad', 'enviar_arriba', 'enviar_abajo'):
                socio_numero = request.form.get('socio_numero', type=int)
                orden_actual = conn.execute(
                    """
                    SELECT s.numero
                    FROM socios s
                    LEFT JOIN prioridad_colocacion_manual pcm
                      ON pcm.periodo=? AND pcm.socio_numero=s.numero
                    LEFT JOIN (
                        SELECT socio_numero, saldo
                        FROM saldo_historico_mensual
                        WHERE periodo=?
                    ) shm ON shm.socio_numero=s.numero
                    ORDER BY
                        CASE WHEN pcm.prioridad IS NULL THEN 1 ELSE 0 END,
                        pcm.prioridad ASC,
                        COALESCE(shm.saldo, s.saldo, 0) ASC,
                        s.numero ASC
                    """,
                    (periodo, periodo),
                ).fetchall()
                numeros = [row['numero'] for row in orden_actual]
                if socio_numero in numeros:
                    idx = numeros.index(socio_numero)
                    if action == 'subir_prioridad' and idx > 0:
                        numeros[idx - 1], numeros[idx] = numeros[idx], numeros[idx - 1]
                    elif action == 'bajar_prioridad' and idx < len(numeros) - 1:
                        numeros[idx + 1], numeros[idx] = numeros[idx], numeros[idx + 1]
                    elif action == 'enviar_arriba' and idx > 0:
                        numero = numeros.pop(idx)
                        numeros.insert(0, numero)
                    elif action == 'enviar_abajo' and idx < len(numeros) - 1:
                        numero = numeros.pop(idx)
                        numeros.append(numero)
                    conn.execute("DELETE FROM prioridad_colocacion_manual WHERE periodo=?", (periodo,))
                    for prioridad, numero in enumerate(numeros, start=1):
                        conn.execute(
                            """
                            INSERT INTO prioridad_colocacion_manual(periodo, socio_numero, prioridad)
                            VALUES(?,?,?)
                            """,
                            (periodo, numero, prioridad),
                        )
                    conn.commit()
                    if action == 'subir_prioridad':
                        movimiento = 'subida'
                    elif action == 'bajar_prioridad':
                        movimiento = 'bajada'
                    elif action == 'enviar_arriba':
                        movimiento = 'envio al inicio'
                    else:
                        movimiento = 'envio al final'
                    flash(f'Orden manual actualizado: {movimiento}.')
                else:
                    flash('No se encontró el socio para mover la prioridad.')
                return redirect(url_for('main.nuevos_prestamos', periodo=periodo, filtro=filtro))

            if action == 'aplicar_sugerencia':
                cierre = conn.execute("SELECT * FROM periodos WHERE periodo=?", (periodo,)).fetchone()
                reservado = conn.execute(
                    """
                    SELECT COALESCE(SUM(monto), 0)
                    FROM prestamos_nuevos
                    WHERE periodo=? AND estado IN ('Reservado', 'Aprobado')
                    """,
                    (periodo,),
                ).fetchone()[0]
                fondo_disponible = max((cierre['total_recaudado'] if cierre else 0) - (reservado or 0), 0)
                creados = 0
                ultimo_id = None
                sugerencias, _ = build_funding_suggestions(conn, periodo, fondo_disponible, min_cuotas, max_cuotas)
                for socio_row in sugerencias:
                    socio = {'numero': socio_row['numero'], 'nombre': socio_row['nombre']}
                    ultimo_id = create_reserved_loan(
                        conn,
                        periodo,
                        socio,
                        socio_row['monto_sugerido'],
                        socio_row['cuotas_sugeridas'],
                        tasa_mensual,
                        f'{periodo}-29',
                    )
                    creados += 1
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Aplicacion de sugerencia automatica',
                    f'Periodo {periodo} - Prestamos creados: {creados}'
                )
                if creados:
                    flash(f'Sugerencia aplicada correctamente. Préstamos creados: {creados}.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=periodo, prestamo_id=ultimo_id, filtro=filtro))
                flash('No hubo préstamos sugeridos para crear en este período.')
                return redirect(url_for('main.nuevos_prestamos', periodo=periodo, filtro=filtro))

            if action in ('aprobar', 'anular'):
                prestamo_id = request.form.get('prestamo_id', type=int)
                prestamo = conn.execute(
                    "SELECT * FROM prestamos_nuevos WHERE id=?",
                    (prestamo_id,),
                ).fetchone()
                if not prestamo:
                    flash('No se encontró el préstamo seleccionado.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=periodo, filtro=filtro))

                if action == 'anular':
                    if prestamo['estado'] == 'Aprobado':
                        flash('No puedes anular aquí un préstamo que ya fue aprobado.')
                    elif prestamo['estado'] == 'Anulado':
                        flash('Ese préstamo ya estaba anulado.')
                    else:
                        create_loan_history_record(
                            conn,
                            prestamo['socio_numero'],
                            prestamo['socio_nombre'],
                            prestamo,
                            'Anulacion',
                            'Anulado',
                            'Préstamo nuevo anulado antes de convertirse en cronograma real.',
                            session.get('username', 'sistema'),
                        )
                        conn.execute(
                            "UPDATE prestamos_nuevos SET estado='Anulado' WHERE id=?",
                            (prestamo_id,),
                        )
                        conn.commit()
                        log_action(
                            session.get('username', 'sistema'),
                            'Préstamo anulado',
                            f'ID {prestamo_id} - Socio {prestamo["socio_numero"]} - Monto {prestamo["monto"]:.1f}'
                        )
                        flash('Préstamo anulado. El fondo quedó liberado nuevamente.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=periodo, prestamo_id=prestamo_id, filtro=filtro))

                if prestamo['estado'] == 'Aprobado':
                    flash('Ese préstamo ya fue aprobado.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=periodo, prestamo_id=prestamo_id, filtro=filtro))
                if prestamo['estado'] == 'Anulado':
                    flash('No se puede aprobar un préstamo que ya fue anulado.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=periodo, prestamo_id=prestamo_id, filtro=filtro))

                cronograma = conn.execute(
                    """
                    SELECT *
                    FROM prestamos_nuevos_cronograma
                    WHERE prestamo_nuevo_id=?
                    ORDER BY plazo
                    """,
                    (prestamo_id,),
                ).fetchall()
                if not cronograma:
                    flash('El préstamo no tiene cronograma disponible para aprobar.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=periodo, prestamo_id=prestamo_id, filtro=filtro))

                socio_anterior = conn.execute(
                    "SELECT numero, nombre, meses, plazo_balance, fecha_prestamo, saldo FROM socios WHERE numero=?",
                    (prestamo['socio_numero'],),
                ).fetchone()
                cuotas_anteriores = conn.execute(
                    "SELECT * FROM cuotas WHERE socio_numero=? ORDER BY plazo, fecha",
                    (prestamo['socio_numero'],),
                ).fetchall()
                create_loan_history_record(
                    conn,
                    prestamo['socio_numero'],
                    prestamo['socio_nombre'],
                    prestamo,
                    'Aprobacion',
                    'Aprobado',
                    'Préstamo aprobado y cronograma anterior reemplazado por uno nuevo.',
                    session.get('username', 'sistema'),
                    snapshot_rows=cuotas_anteriores,
                    socio_anterior=socio_anterior,
                )
                conn.execute("DELETE FROM cuotas WHERE socio_numero=?", (prestamo['socio_numero'],))
                for row in cronograma:
                    conn.execute(
                        """
                        INSERT INTO cuotas(socio_numero, plazo, fecha, prestamo, interes, abono_capital, cuota, saldo, hoja_origen)
                        VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            prestamo['socio_numero'],
                            row['plazo'],
                            row['fecha'],
                            row['prestamo'],
                            row['interes'],
                            row['abono_capital'],
                            row['cuota'],
                            row['saldo'],
                            f'prestamo_nuevo:{prestamo_id}',
                        ),
                    )

                conn.execute(
                    """
                    UPDATE socios
                    SET meses=?,
                        plazo_balance=?,
                        fecha_prestamo=?,
                        saldo=?
                    WHERE numero=?
                    """,
                    (
                        prestamo['cuotas'],
                        prestamo['cuotas'],
                        prestamo['fecha_desembolso'],
                        prestamo['cuota_inicial'],
                        prestamo['socio_numero'],
                    ),
                )
                conn.execute(
                    "UPDATE prestamos_nuevos SET estado='Aprobado' WHERE id=?",
                    (prestamo_id,),
                )
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Préstamo aprobado',
                    f'ID {prestamo_id} - Socio {prestamo["socio_numero"]} - Monto {prestamo["monto"]:.1f} - Cuotas {prestamo["cuotas"]}'
                )
                flash('Préstamo aprobado y aplicado al cronograma real del socio.')
                return redirect(url_for('main.nuevos_prestamos', periodo=periodo, prestamo_id=prestamo_id, filtro=filtro))

            socio_numero = request.form.get('socio_numero', type=int)
            monto = request.form.get('monto', type=float)
            cuotas = request.form.get('cuotas', type=int)
            fecha_desembolso = request.form.get('fecha_desembolso', '').strip() or f'{periodo}-29'

            cierre = conn.execute("SELECT * FROM periodos WHERE periodo=?", (periodo,)).fetchone()
            colocacion = get_period_placement_status(conn, periodo, cierre['total_recaudado'] if cierre else 0)
            reservado = colocacion['total_colocado']
            fondo_disponible = colocacion['saldo_por_colocar']
            socio = conn.execute("SELECT numero, nombre FROM socios WHERE numero=?", (socio_numero,)).fetchone()

            error = None
            if not socio:
                error = 'Selecciona un socio válido.'
            elif monto is None or monto <= 0:
                error = 'Ingresa un monto válido.'
            elif cuotas is None or cuotas < min_cuotas or cuotas > max_cuotas:
                error = f'Las cuotas deben estar entre {min_cuotas} y {max_cuotas}.'
            elif monto > fondo_disponible:
                error = f'El monto excede el fondo disponible del período: S/ {fondo_disponible:.1f}.'

            if error:
                flash(error)
                return redirect(url_for('main.nuevos_prestamos', periodo=periodo, filtro=filtro))

            prestamo_id = create_reserved_loan(
                conn,
                periodo,
                socio,
                monto,
                cuotas,
                tasa_mensual,
                fecha_desembolso,
            )
            conn.commit()
            log_action(
                session.get('username', 'sistema'),
                'Nuevo prestamo reservado',
                f'Periodo {periodo} - Socio {socio["numero"]} - Monto {monto:.1f} - Cuotas {cuotas}'
            )
            flash('Nuevo préstamo generado correctamente.')
            return redirect(url_for('main.nuevos_prestamos', periodo=periodo, prestamo_id=prestamo_id, filtro=filtro))

        cierre = conn.execute("SELECT * FROM periodos WHERE periodo=?", (periodo,)).fetchone()
        periodo_historico_oficial = str(get_config_value(conn, 'resumen_financiero_periodo_oficial', '2026-03') or '2026-03')
        siguiente_periodo_operativo = get_next_period(periodo_historico_oficial)
        tasa_mensual = float(get_config_value(conn, 'tasa_prestamo_mensual', '0.01'))
        min_cuotas = int(float(get_config_value(conn, 'min_cuotas_prestamo', '12')))
        max_cuotas = int(float(get_config_value(conn, 'max_cuotas_prestamo', '84')))
        colocacion = get_period_placement_status(conn, periodo, cierre['total_recaudado'] if cierre else 0)
        reservado = colocacion['total_colocado']
        fondo_total = cierre['total_recaudado'] if cierre else 0
        fondo_disponible = colocacion['saldo_por_colocar']
        porcentaje_colocado = round((((reservado or 0) / (fondo_total or 1)) * 100), 1) if (fondo_total or 0) > 0 else 0
        socios = conn.execute("SELECT numero, nombre FROM socios ORDER BY numero").fetchall()
        sugerencias, restante_sugerido = build_funding_suggestions(
            conn,
            periodo,
            fondo_disponible,
            min_cuotas,
            max_cuotas,
        )
        prioridades = {
            row['socio_numero']: row['prioridad']
            for row in conn.execute(
                """
                SELECT socio_numero, prioridad
                FROM prioridad_colocacion_manual
                WHERE periodo=?
                """,
                (periodo,),
            ).fetchall()
        }
        socios_orden_manual = conn.execute(
            """
            SELECT s.numero,
                   s.nombre,
                   pcm.prioridad,
                   COALESCE(om.saldo_actual, s.saldo, 0) AS saldo_prioridad
            FROM socios s
            LEFT JOIN prioridad_colocacion_manual pcm
              ON pcm.periodo=? AND pcm.socio_numero=s.numero
            LEFT JOIN obligaciones_mensuales om
              ON om.periodo=? AND om.socio_numero=s.numero
            ORDER BY
                CASE WHEN pcm.prioridad IS NULL THEN 1 ELSE 0 END,
                pcm.prioridad ASC,
                saldo_prioridad ASC,
                s.numero ASC
            """,
            (periodo, periodo),
        ).fetchall()
        filtro_counts = {
            'todos': len(socios_orden_manual),
            'manuales': sum(1 for row in socios_orden_manual if prioridades.get(row['numero'])),
            'automaticos': sum(1 for row in socios_orden_manual if not prioridades.get(row['numero'])),
        }
        resumen_prioridades = {
            'manuales_activos': filtro_counts['manuales'],
            'automaticos_activos': filtro_counts['automaticos'],
            'socios_sugeridos_mes': len(sugerencias),
            'monto_sugerido_mes': round(sum((row.get('monto_sugerido') or 0) for row in sugerencias), 1),
            'restante_sin_sugerir': round(restante_sugerido or 0, 1),
            'porcentaje_sugerido_fondo': round(
                ((sum((row.get('monto_sugerido') or 0) for row in sugerencias) / (fondo_disponible or 1)) * 100),
                1,
            ) if (fondo_disponible or 0) > 0 else 0,
        }
        if filtro == 'manuales':
            socios_orden_manual = [row for row in socios_orden_manual if prioridades.get(row['numero'])]
            sugerencias = [row for row in sugerencias if row.get('prioridad_manual') is not None]
        elif filtro == 'automaticos':
            socios_orden_manual = [row for row in socios_orden_manual if not prioridades.get(row['numero'])]
            sugerencias = [row for row in sugerencias if row.get('prioridad_manual') is None]
        prestamos = conn.execute(
            """
            SELECT *
            FROM prestamos_nuevos
            WHERE periodo=?
            ORDER BY id DESC
            """,
            (periodo,),
        ).fetchall()
        if not selected_id and prestamos:
            selected_id = prestamos[0]['id']
        selected_prestamo = conn.execute(
            "SELECT * FROM prestamos_nuevos WHERE id=?",
            (selected_id,),
        ).fetchone() if selected_id else None
        cronograma = conn.execute(
            """
            SELECT *
            FROM prestamos_nuevos_cronograma
            WHERE prestamo_nuevo_id=?
            ORDER BY plazo
            """,
            (selected_id,),
        ).fetchall() if selected_id else []

    response = make_response(render_template(
        'nuevos_prestamos.html',
        periodo=periodo,
        cierre=cierre,
        socios=socios,
        prestamos=prestamos,
        prestamo=selected_prestamo,
        cronograma=cronograma,
        fondo_total=fondo_total or 0,
        fondo_reservado=reservado or 0,
        fondo_disponible=fondo_disponible or 0,
        saldo_por_colocar=fondo_disponible or 0,
        colocacion_fuente=colocacion['fuente'],
        colocacion_excel_rows=colocacion['excel_rows'],
        colocacion_detalle_rows=colocacion['detalle_rows'],
        colocacion_origin_counts=colocacion['origin_counts'],
        porcentaje_colocado=porcentaje_colocado,
        colocacion_completa=(fondo_disponible or 0) <= 0.0001,
        sugerencias=sugerencias,
        restante_sugerido=restante_sugerido,
        filtro=filtro,
        filtro_counts=filtro_counts,
        resumen_prioridades=resumen_prioridades,
        prioridades=prioridades,
        socios_orden_manual=socios_orden_manual,
        tasa_mensual=tasa_mensual,
        min_cuotas=min_cuotas,
        max_cuotas=max_cuotas,
        periodo_historico_oficial=periodo_historico_oficial,
        siguiente_periodo_operativo=siguiente_periodo_operativo,
        es_periodo_historico_oficial=(periodo == periodo_historico_oficial),
    ))
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
        fecha_emision=datetime.now().strftime('%d/%m/%Y %H:%M'),
    )


@bp.route('/reuniones', methods=['GET', 'POST'])
@login_required
def reuniones():
    with connect_db() as conn:
        periodo = request.args.get('periodo', '').strip()
        if not periodo:
            periodo = get_default_period(conn)
            return redirect(url_for('main.reuniones', periodo=periodo))

        ensure_monthly_collections(conn, periodo)
        cleanup_legacy_auto_reunion_assignment(conn, periodo)

        if request.method == 'POST':
            if not role_can_access_endpoint(session.get('role'), 'main.reuniones', write=True):
                flash('Tu rol no tiene permiso para realizar esta acción.')
                return redirect(url_for('main.reuniones', periodo=periodo))

            action = request.form.get('action', '').strip()
            if action == 'guardar_reunion':
                socio_numero = request.form.get('socio_numero', '').strip()
                estado = request.form.get('estado', 'Pendiente').strip() or 'Pendiente'
                fecha_programada = request.form.get('fecha_programada', '').strip() or f'{periodo}-01'
                fecha_realizada = request.form.get('fecha_realizada', '').strip() or None
                tipo_via = request.form.get('tipo_via', '').strip()
                direccion_reunion = request.form.get('direccion_reunion', '').strip()
                observacion = request.form.get('observacion', '').strip()
                socio = conn.execute(
                    "SELECT numero, nombre FROM socios WHERE numero=?",
                    (socio_numero,),
                ).fetchone() if socio_numero.isdigit() else None
                if not socio:
                    flash('Selecciona un socio válido para la reunión del período.')
                else:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO reuniones_mensuales(
                            periodo, socio_numero, socio_nombre, estado, fecha_programada, fecha_realizada, tipo_via, direccion_reunion, observacion, actualizado_por, actualizado_en
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                        """,
                        (
                            periodo,
                            socio['numero'],
                            socio['nombre'],
                            estado,
                            fecha_programada,
                            fecha_realizada,
                            tipo_via or None,
                            direccion_reunion or None,
                            observacion,
                            session.get('username', 'sistema'),
                        ),
                    )
                    conn.commit()
                    log_action(session.get('username', 'sistema'), 'Control de reuniones', f'Período {periodo}: reunión asignada a socio {socio["numero"]}.')
                    flash('Reunión del período actualizada correctamente.')
                return redirect(url_for('main.reuniones', periodo=periodo))

            if action == 'registrar_permiso':
                socio_numero = request.form.get('socio_numero', '').strip()
                fecha_permiso = request.form.get('fecha_permiso', '').strip() or f'{periodo}-01'
                motivo = request.form.get('motivo', '').strip() or 'Otros'
                observacion = request.form.get('observacion', '').strip()
                documento = request.files.get('documento_permiso')
                socio = conn.execute(
                    "SELECT numero, nombre FROM socios WHERE numero=?",
                    (socio_numero,),
                ).fetchone() if socio_numero.isdigit() else None
                if not socio:
                    flash('Selecciona un socio válido para registrar el permiso.')
                elif motivo not in PERMISO_MOTIVOS:
                    flash('Selecciona un motivo válido para el permiso.')
                else:
                    documento_path = save_permiso_document(documento, periodo, socio['numero'])
                    if documento_path is False:
                        flash('El documento del permiso debe ser PDF, DOC o DOCX.')
                        return redirect(url_for('main.reuniones', periodo=periodo))
                    conn.execute(
                        """
                        INSERT INTO permisos_mensuales(
                            periodo, socio_numero, socio_nombre, fecha_permiso, motivo, documento, observacion, registrado_por
                        ) VALUES(?,?,?,?,?,?,?,?)
                        """,
                        (
                            periodo,
                            socio['numero'],
                            socio['nombre'],
                            fecha_permiso,
                            motivo,
                            documento_path,
                            observacion,
                            session.get('username', 'sistema'),
                        ),
                    )
                    conn.commit()
                    log_action(session.get('username', 'sistema'), 'Control de permisos', f'Período {periodo}: permiso registrado para socio {socio["numero"]}.')
                    flash('Permiso registrado correctamente.')
                return redirect(url_for('main.reuniones', periodo=periodo))

            if action == 'eliminar_permiso':
                permiso_id = request.form.get('permiso_id', '').strip()
                permiso = conn.execute(
                    "SELECT * FROM permisos_mensuales WHERE id=? AND periodo=?",
                    (permiso_id, periodo),
                ).fetchone()
                if permiso:
                    delete_permiso_document_if_local(permiso['documento'])
                    conn.execute("DELETE FROM permisos_mensuales WHERE id=?", (permiso['id'],))
                    conn.commit()
                    log_action(session.get('username', 'sistema'), 'Control de permisos', f'Período {periodo}: permiso eliminado #{permiso["id"]}.')
                    flash('Permiso eliminado correctamente.')
                return redirect(url_for('main.reuniones', periodo=periodo))

        reunion_periodo = conn.execute(
            """
            SELECT *
            FROM reuniones_mensuales
            WHERE periodo=?
            """,
            (periodo,),
        ).fetchone()
        socios = conn.execute("SELECT numero, nombre FROM socios ORDER BY numero").fetchall()
        periodos = conn.execute("SELECT periodo FROM periodos ORDER BY periodo DESC").fetchall()
        permisos_registrados = conn.execute(
            """
            SELECT *
            FROM permisos_mensuales
            WHERE periodo=?
            ORDER BY fecha_permiso DESC, id DESC
            """,
            (periodo,),
        ).fetchall()
        permisos_resumen = conn.execute(
            """
            SELECT socio_numero, socio_nombre, COUNT(*) as total_permisos
            FROM permisos_mensuales
            WHERE periodo=?
            GROUP BY socio_numero, socio_nombre
            ORDER BY total_permisos DESC, socio_numero ASC
            """,
            (periodo,),
        ).fetchall()
        obligaciones = conn.execute(
            """
            SELECT socio_numero, socio_nombre, total_mes
            FROM obligaciones_mensuales
            WHERE periodo=?
            ORDER BY socio_numero
            """,
              (periodo,),
          ).fetchall()
        lugar_reunion = '-'
        if reunion_periodo:
            partes_lugar = [reunion_periodo['tipo_via'] or '', reunion_periodo['direccion_reunion'] or '']
            lugar_reunion = ' '.join([parte.strip() for parte in partes_lugar if parte and parte.strip()]) or '-'
    return render_template(
        'reuniones_control.html',
        periodo=periodo,
        reunion_periodo=reunion_periodo,
        socios=socios,
        periodos=periodos,
        permisos_registrados=permisos_registrados,
        permisos_resumen=permisos_resumen,
        obligaciones=obligaciones,
        reunion_tipos_via=REUNION_TIPOS_VIA,
        lugar_reunion=lugar_reunion,
        permiso_motivos=PERMISO_MOTIVOS,
        build_permiso_document_url=build_permiso_document_url,
        puede_editar=role_can_access_endpoint(session.get('role'), 'main.reuniones', write=True),
    )


@bp.route('/reportes')
@login_required
def reportes():
    with connect_db() as conn:
        periodo_actual = get_default_period(conn)
        ensure_monthly_collections(conn, periodo_actual)
        resumen = conn.execute("""
            SELECT COUNT(*) as socios,
                   COALESCE(SUM(saldo_actual),0) as saldo_total,
                   COALESCE(AVG(saldo_actual),0) as saldo_promedio,
                   COALESCE(MAX(saldo_actual),0) as saldo_max
            FROM obligaciones_mensuales
            WHERE periodo=?
        """, (periodo_actual,)).fetchone()
        top = conn.execute("""
            SELECT s.nombre,
                   COALESCE(om.saldo_actual, s.saldo, 0) as saldo_periodo,
                   s.fecha_prestamo,
                   COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
            FROM socios s
            LEFT JOIN cuotas c ON c.socio_numero = s.numero
            LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
            GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
            ORDER BY COALESCE(om.saldo_actual, s.saldo, 0) DESC, s.numero ASC
            LIMIT 10
        """, (periodo_actual,)).fetchall()
        morosos = []
        mensual = conn.execute("""
            SELECT substr(fecha,1,7) as periodo,
                   ROUND(SUM(interes),2) as intereses,
                   ROUND(SUM(abono_capital),2) as capital,
                   ROUND(SUM(cuota),2) as cuotas
            FROM cuotas
            WHERE fecha IS NOT NULL
            GROUP BY substr(fecha,1,7)
            ORDER BY periodo DESC
            LIMIT 12
        """).fetchall()
        prestamos_multiples = conn.execute(
            """
            SELECT h.socio_numero as numero,
                   COALESCE(MAX(h.socio_nombre), s.nombre) as nombre,
                   COUNT(*) as total_prestamos,
                   COALESCE(SUM(CASE WHEN h.es_activo=1 AND substr(h.fecha_inicio, 1, 7)='2026-03' THEN 1 ELSE 0 END), 0) as tiene_marzo_2026,
                   MAX(CASE WHEN h.es_activo=1 THEN COALESCE(h.titulo_manual, h.titulo) END) as prestamo_activo,
                   MAX(CASE WHEN h.es_activo=1 THEN h.fecha_inicio END) as fecha_prestamo_activo
            FROM prestamos_excel_historial h
            LEFT JOIN socios s ON s.numero = h.socio_numero
            WHERE COALESCE(h.oculto_manual, 0)=0
            GROUP BY h.socio_numero
            HAVING COUNT(*) > 1
            ORDER BY total_prestamos DESC, h.socio_numero ASC
            """
        ).fetchall()
        prestamos_multiples = [
            {
                **dict(row),
                'prestamo_activo': loan_title_visible(row['fecha_prestamo_activo'], row['prestamo_activo'] or '-'),
            }
            for row in prestamos_multiples
        ]
        resumen_prestamos_multiples = {
            'socios_multiples': len(prestamos_multiples),
            'socios_marzo_2026': sum([1 for row in prestamos_multiples if row['tiene_marzo_2026']]),
        }
    return render_template(
        'reportes.html',
        resumen=resumen,
        top=top,
        morosos=morosos,
        mensual=mensual,
        prestamos_multiples=prestamos_multiples,
        resumen_prestamos_multiples=resumen_prestamos_multiples,
    )


@bp.route('/reportes/saldo-actual')
@login_required
def saldo_actual():
    umbral = 30000
    with connect_db() as conn:
        periodo = request.args.get('periodo', '').strip() or get_latest_financial_period(conn) or get_default_period(conn)
        cierre_periodo = conn.execute(
            "SELECT * FROM periodos WHERE periodo=?",
            (periodo,),
        ).fetchone()
        snapshot_periodo = get_financial_snapshot_for_period(conn, periodo) or {
            'saldo_actual_total': 0,
            'acciones_por_socio': 0,
            'total_socios': 0,
        }
        colocacion_periodo = get_period_placement_status(
            conn,
            periodo,
            cierre_periodo['total_recaudado'] if cierre_periodo else 0,
        )
        try:
            total_prestamo_acumulado = float(
                get_config_value(conn, 'total_prestamo_acumulado_oficial', 0) or 0
            )
        except Exception:
            total_prestamo_acumulado = 0
        rows = conn.execute(
            """
            SELECT periodo, socio_numero, socio_nombre, saldo, fuente
            FROM saldo_historico_mensual
            WHERE periodo=?
            ORDER BY saldo ASC, socio_nombre ASC
            """
        , (periodo,)).fetchall()

    saldos = []
    max_saldo = max([row['saldo'] for row in rows], default=1)
    for row in rows:
        saldos.append(
            {
                'numero': row['socio_numero'],
                'nombre': row['socio_nombre'],
                'saldo': row['saldo'],
                'fuente': row['fuente'],
                'pct': round((row['saldo'] / max_saldo) * 100, 2),
                'supera_umbral': row['saldo'] >= umbral,
            }
        )

    resumen = {
        'cantidad': len(saldos),
        'minimo': min([row['saldo'] for row in rows], default=0),
        'maximo': max_saldo if rows else 0,
        'promedio': round(sum([row['saldo'] for row in rows]) / len(rows), 2) if rows else 0,
        'vinculados': len([row for row in rows if row['socio_numero'] is not None]),
    }
    cierre_historico = {
        'periodo': periodo,
        'periodo_label': format_period_label(periodo, periodo),
        'estado': cierre_periodo['estado'] if cierre_periodo else 'Sin cierre',
        'saldo_actual_total': round(float(snapshot_periodo['saldo_actual_total'] or 0), 1),
        'acciones_por_socio': round(float(snapshot_periodo['acciones_por_socio'] or 0), 1),
        'total_prestamo_acumulado': round(float(total_prestamo_acumulado or 0), 1),
        'total_mensual': round(float(cierre_periodo['total_recaudado'] or 0), 1) if cierre_periodo else 0,
        'ya_colocado': round(float(colocacion_periodo['total_colocado'] or 0), 1),
        'saldo_por_colocar': round(float(colocacion_periodo['saldo_por_colocar'] or 0), 1),
    }
    composicion_mes = {
        'cuotas_mes': round(float(cierre_periodo['total_prestamos'] or 0), 1) if cierre_periodo else 0,
        'aportes_fijos': round(float(cierre_periodo['total_aportes'] or 0), 1) if cierre_periodo else 0,
        'total_mensual': round(float(cierre_periodo['total_recaudado'] or 0), 1) if cierre_periodo else 0,
    }
    colocaciones = [
        {
            'numero': row['socio_numero'],
            'nombre': row['socio_nombre'],
            'fecha_inicio': row['fecha_inicio'],
            'monto_inicial': round(float(row['monto_inicial'] or 0), 1),
            'cuotas': row['plazo_total'],
            'origen': row['origen'],
        }
        for row in colocacion_periodo['detalle_rows']
    ]

    return render_template(
        'saldo_actual.html',
        saldos=saldos,
        resumen=resumen,
        umbral=umbral,
        cierre_historico=cierre_historico,
        composicion_mes=composicion_mes,
        colocaciones=colocaciones,
    )


@bp.route('/reportes/saldos-marzo-2026')
@login_required
def saldos_marzo_2026():
    periodo = request.args.get('periodo', '').strip()
    if periodo:
        return redirect(url_for('main.saldo_actual', periodo=periodo))
    return redirect(url_for('main.saldo_actual'))


@bp.route('/reportes/saldo-actual/vincular', methods=['GET', 'POST'])
@admin_required
def vincular_saldo_actual():
    with connect_db() as conn:
        if request.method == 'POST':
            rows = conn.execute(
                """
                SELECT id, socio_numero
                FROM saldo_historico_mensual
                WHERE periodo='2026-03'
                ORDER BY saldo ASC, socio_nombre ASC
                """
            ).fetchall()

            cambios = 0
            for row in rows:
                field_name = f"asignacion_{row['id']}"
                raw_value = request.form.get(field_name, '').strip()
                nuevo_numero = int(raw_value) if raw_value.isdigit() else None
                if nuevo_numero != row['socio_numero']:
                    conn.execute(
                        "UPDATE saldo_historico_mensual SET socio_numero=? WHERE id=?",
                        (nuevo_numero, row['id'])
                    )
                    cambios += 1

            conn.commit()
            log_action(
                session.get('username', 'admin'),
                'Vinculacion manual de saldos',
                f'Marzo 2026: {cambios} cambios guardados'
            )
            flash(f'Vinculaciones guardadas correctamente: {cambios}.')
            return redirect(url_for('main.vincular_saldo_actual'))

        pendientes = conn.execute(
            """
            SELECT id, socio_nombre, saldo, socio_numero
            FROM saldo_historico_mensual
            WHERE periodo='2026-03' AND socio_numero IS NULL
            ORDER BY saldo ASC, socio_nombre ASC
            """
        ).fetchall()
        socios = conn.execute(
            "SELECT numero, nombre FROM socios ORDER BY nombre ASC, numero ASC"
        ).fetchall()
        vinculados = conn.execute(
            """
            SELECT h.id, h.socio_nombre, h.saldo, h.socio_numero, s.nombre as socio_vinculado
            FROM saldo_historico_mensual h
            LEFT JOIN socios s ON s.numero = h.socio_numero
            WHERE h.periodo='2026-03' AND h.socio_numero IS NOT NULL
            ORDER BY h.saldo ASC, h.socio_nombre ASC
            """
        ).fetchall()

    return render_template(
        'vincular_saldo_actual.html',
        pendientes=pendientes,
        vinculados=vinculados,
        socios=socios,
    )


@bp.route('/reportes/saldos-marzo-2026/vincular', methods=['GET', 'POST'])
@admin_required
def vincular_saldos_marzo_2026():
    return redirect(url_for('main.vincular_saldo_actual'))


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


@bp.route('/reportes/mensual.csv')
@login_required
def reporte_mensual_csv():
    with connect_db() as conn:
        rows = conn.execute("""
            SELECT substr(fecha,1,7) as periodo,
                   ROUND(SUM(interes),2) as intereses,
                   ROUND(SUM(abono_capital),2) as capital,
                   ROUND(SUM(cuota),2) as cuotas
            FROM cuotas
            WHERE fecha IS NOT NULL
            GROUP BY substr(fecha,1,7)
            ORDER BY periodo DESC
        """).fetchall()
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['Periodo', 'Intereses', 'Capital', 'Cuotas'])
    for r in rows:
        cw.writerow([r['periodo'], r['intereses'], r['capital'], r['cuotas']])
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=resumen_mensual_jawilvio_v10.csv"
    output.headers["Content-type"] = "text/csv; charset=utf-8"
    return output


@bp.route('/graficos')
@login_required
def graficos():
    with connect_db() as conn:
        mensual = conn.execute("""
            SELECT substr(fecha,1,7) as periodo,
                   ROUND(SUM(interes),2) as intereses,
                   ROUND(SUM(abono_capital),2) as capital,
                   ROUND(SUM(cuota),2) as cuotas
            FROM cuotas
            WHERE fecha IS NOT NULL
            GROUP BY substr(fecha,1,7)
            ORDER BY periodo DESC
            LIMIT 12
        """).fetchall()
    mensual = list(reversed(mensual))
    max_cuota = max([r['cuotas'] or 0 for r in mensual], default=1)
    return render_template('graficos.html', mensual=mensual, max_cuota=max_cuota)


@bp.route('/asistencia', methods=['GET', 'POST'])
@login_required
def asistencia():
    with connect_db() as conn:
        if request.method == 'POST':
            socio_numero = int(request.form['socio_numero'])
            socio = conn.execute("SELECT nombre FROM socios WHERE numero=?", (socio_numero,)).fetchone()
            conn.execute(
                "INSERT INTO asistencia(socio_numero, socio_nombre, fecha, estado, observacion) VALUES(?,?,?,?,?)",
                (
                    socio_numero,
                    socio['nombre'] if socio else str(socio_numero),
                    request.form['fecha'],
                    request.form['estado'],
                    request.form.get('observacion', ''),
                )
            )
            conn.commit()
            log_action(session.get('username', 'sistema'), 'Registro de asistencia', f"Socio {socio_numero} - {request.form['estado']}")
            flash('Asistencia registrada correctamente.')
            return redirect(url_for('main.asistencia'))
        socios = conn.execute("SELECT numero, nombre FROM socios ORDER BY numero").fetchall()
        registros = conn.execute("SELECT * FROM asistencia ORDER BY fecha DESC, id DESC LIMIT 50").fetchall()
    return render_template('asistencia.html', socios=socios, registros=registros, hoy=datetime.now().strftime('%Y-%m-%d'))


@bp.route('/estado-general.pdf')
@login_required
def pdf_estado_general():
    with connect_db() as conn:
        periodo_actual = get_default_period(conn)
        ensure_monthly_collections(conn, periodo_actual)
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
    html = render_template(
        'estado_general_pdf.html',
        total_socios=total_socios,
        total_prestamos=total_prestamos,
        morosos=morosos,
        saldo=saldo,
        capital=capital,
        intereses=intereses,
        cuotas=cuotas,
        fecha=datetime.now().strftime('%Y-%m-%d %H:%M'),
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
        socios = conn.execute("""
            SELECT s.*,
                   COALESCE(om.saldo_actual, s.saldo, 0) as saldo_periodo,
                   COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
            FROM socios s
            LEFT JOIN cuotas c ON c.socio_numero = s.numero
            LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
            GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
            ORDER BY s.numero
        """, (periodo_actual,)).fetchall()
        
        cuotas = conn.execute("SELECT * FROM cuotas ORDER BY fecha LIMIT 50").fetchall()
       
        resumen = conn.execute("""
            SELECT COUNT(*) as socios,
                   COALESCE(SUM(saldo_actual),0) as saldo_total
            FROM obligaciones_mensuales
            WHERE periodo=?
        """, (periodo_actual,)).fetchone()

        reunion_periodo = conn.execute("""
            SELECT socio_numero,
                   socio_nombre,
                   estado,
                   tipo_via,
                   direccion_reunion
            FROM reuniones_mensuales
            WHERE periodo=?
            LIMIT 1
        """, (periodo_actual,)).fetchone()
    
        return render_template(
            'reporte_imprimible.html',
            socios=socios,
            cuotas=cuotas,
            resumen=resumen,
            periodo_actual=periodo_actual,
            fecha_emision=datetime.now().strftime('%d/%m/%Y %H:%M'),
            reunion_periodo=reunion_periodo
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


@bp.route('/usuarios', methods=['GET', 'POST'])
@admin_required
def usuarios():
    with connect_db() as conn:
        if request.method == 'POST':
            action = request.form.get('action', '').strip()
            if action == 'create_user':
                username = request.form.get('username', '').strip()
                password = request.form.get('password', '').strip()
                role = request.form.get('role', '').strip()
                if not username or not password or not role:
                    flash('Completa usuario, contraseña y rol.')
                else:
                    exists = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
                    if exists:
                        flash('Ese nombre de usuario ya existe.')
                    else:
                        conn.execute(
                            "INSERT INTO users(username,password,role,estado) VALUES(?,?,?, 'Activo')",
                            (username, password, role),
                        )
                        conn.commit()
                        log_action(session.get('username', 'admin'), 'Gestión de usuarios', f'Usuario creado: {username} ({role})')
                        flash('Usuario creado correctamente.')
                return redirect(url_for('main.usuarios'))
            if action == 'toggle_status':
                user_id = request.form.get('user_id', '').strip()
                user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
                if not user:
                    flash('No se encontró el usuario solicitado.')
                elif user['id'] == session.get('user_id'):
                    flash('No puedes suspender tu propio usuario desde esta pantalla.')
                else:
                    nuevo_estado = 'Activo' if user['estado'] == 'Suspendido' else 'Suspendido'
                    conn.execute("UPDATE users SET estado=? WHERE id=?", (nuevo_estado, user['id']))
                    conn.commit()
                    log_action(session.get('username', 'admin'), 'Gestión de usuarios', f'Estado actualizado: {user["username"]} -> {nuevo_estado}')
                    flash(f'Estado actualizado: {user["username"]} ahora está {nuevo_estado.lower()}.')
                return redirect(url_for('main.usuarios'))
            if action == 'reset_password':
                user_id = request.form.get('user_id', '').strip()
                new_password = request.form.get('new_password', '').strip()
                user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
                if not user:
                    flash('No se encontró el usuario solicitado.')
                elif len(new_password) < 4:
                    flash('La nueva contraseña debe tener al menos 4 caracteres.')
                else:
                    conn.execute("UPDATE users SET password=? WHERE id=?", (new_password, user['id']))
                    conn.commit()
                    log_action(session.get('username', 'admin'), 'Gestión de usuarios', f'Contraseña restablecida: {user["username"]}')
                    flash(f'Contraseña restablecida para {user["username"]}.')
                return redirect(url_for('main.usuarios'))
        users = conn.execute(
            """
            SELECT id, username, role, estado, creado_en, ultimo_acceso
            FROM users
            ORDER BY username
            """
        ).fetchall()
    return render_template('usuarios.html', users=users)


@bp.route('/mi-cuenta', methods=['GET', 'POST'])
@login_required
def mi_cuenta():
    with connect_db() as conn:
        user = conn.execute(
            """
            SELECT id, username, role, estado, creado_en, ultimo_acceso, password
            FROM users
            WHERE id=?
            """,
            (session.get('user_id'),),
        ).fetchone()
        if not user:
            session.clear()
            flash('No se encontró tu usuario. Inicia sesión nuevamente.')
            return redirect(url_for('main.login'))
        if request.method == 'POST':
            current_password = request.form.get('current_password', '').strip()
            new_password = request.form.get('new_password', '').strip()
            confirm_password = request.form.get('confirm_password', '').strip()
            if current_password != user['password']:
                flash('La contraseña actual no coincide.')
            elif len(new_password) < 4:
                flash('La nueva contraseña debe tener al menos 4 caracteres.')
            elif new_password != confirm_password:
                flash('La confirmación de la nueva contraseña no coincide.')
            else:
                conn.execute(
                    "UPDATE users SET password=? WHERE id=?",
                    (new_password, user['id']),
                )
                conn.commit()
                log_action(session.get('username', 'usuario'), 'Cambio de contraseña', 'Actualizó su contraseña de acceso.')
                flash('Tu contraseña fue actualizada correctamente.')
                return redirect(url_for('main.mi_cuenta'))
    return render_template('mi_cuenta.html', user=user)


@bp.route('/configuracion', methods=['GET', 'POST'])
@admin_required
def configuracion():
    with connect_db() as conn:
        if request.method == 'POST':
            for clave in ['nombre_asociacion', 'ubicacion', 'aporte_mensual', 'saldo_actual_total_oficial', 'total_prestamo_acumulado_oficial', 'multa_inasistencia', 'multa_tardanza']:
                conn.execute("INSERT OR REPLACE INTO configuracion(clave, valor) VALUES(?,?)", (clave, request.form.get(clave, '')))
            conn.commit()
            log_action(session.get('username', 'admin'), 'Configuración', 'Actualización de parámetros institucionales')
            flash('Configuración guardada correctamente.')
            return redirect(url_for('main.configuracion'))
        cfg = {r['clave']: r['valor'] for r in conn.execute("SELECT clave, valor FROM configuracion").fetchall()}
    return render_template('configuracion.html', cfg=cfg)


@bp.route('/auditoria')
@admin_required
def auditoria():
    with connect_db() as conn:
        rows = conn.execute("SELECT * FROM auditoria ORDER BY id DESC LIMIT 200").fetchall()
    return render_template('auditoria.html', rows=rows)

