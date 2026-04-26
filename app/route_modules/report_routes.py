"""Reporting and monitoring routes extracted from the main routes file."""

import csv
import io
from datetime import datetime

from flask import flash, make_response, redirect, render_template, request, session, url_for

from app.routes import (
    MULTA_ASISTENCIA_ESTADOS,
    admin_required,
    bp,
    ensure_monthly_collections,
    ensure_period_is_writable,
    format_period_label,
    get_config_value,
    get_default_period,
    get_financial_snapshot_for_period,
    get_latest_attendance_rows_for_period,
    get_latest_financial_period,
    get_period_placement_status,
    loan_title_visible,
    log_action,
    login_required,
    normalize_attendance_state,
    role_can_access_endpoint,
    sync_attendance_fines_for_period,
)
from app.utils.trends import build_period_trend_snapshot, build_variation


def _render_without_cache(template_name, **context):
    """Render HTML responses that should never be cached by the browser."""
    response = make_response(render_template(template_name, **context))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@bp.route('/reportes')
@login_required
def reportes():
    """Show the executive reporting dashboard for the current period."""
    from app.routes import connect_db

    with connect_db() as conn:
        current_period = get_default_period(conn)
        ensure_monthly_collections(conn, current_period)
        summary = conn.execute(
            """
            SELECT COUNT(*) as socios,
                   COALESCE(SUM(saldo_actual),0) as saldo_total,
                   COALESCE(AVG(saldo_actual),0) as saldo_promedio,
                   COALESCE(MAX(saldo_actual),0) as saldo_max
            FROM obligaciones_mensuales
            WHERE periodo=?
            """,
            (current_period,),
        ).fetchone()
        top_balances = conn.execute(
            """
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
            """,
            (current_period,),
        ).fetchall()
        monthly_projection = conn.execute(
            """
            SELECT substr(fecha,1,7) as periodo,
                   ROUND(SUM(interes),2) as intereses,
                   ROUND(SUM(abono_capital),2) as capital,
                   ROUND(SUM(cuota),2) as cuotas
            FROM cuotas
            WHERE fecha IS NOT NULL
            GROUP BY substr(fecha,1,7)
            ORDER BY periodo DESC
            LIMIT 12
            """
        ).fetchall()
        trend_period_rows = conn.execute(
            """
            SELECT periodo,
                   estado,
                   total_socios,
                   total_recaudado,
                   total_colocado,
                   saldo_por_colocar
            FROM periodos
            ORDER BY periodo DESC
            LIMIT 12
            """
        ).fetchall()
        multi_loan_rows = conn.execute(
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
        latest_financial_period = get_latest_financial_period(conn) or current_period
        latest_financial_snapshot = get_financial_snapshot_for_period(conn, latest_financial_period)
        previous_financial_period_row = conn.execute(
            """
            SELECT periodo
            FROM periodos
            WHERE estado IN ('Colocado', 'Cerrado')
              AND periodo < ?
            ORDER BY periodo DESC
            LIMIT 1
            """,
            (latest_financial_period,),
        ).fetchone()
        previous_financial_snapshot = (
            get_financial_snapshot_for_period(conn, previous_financial_period_row['periodo'])
            if previous_financial_period_row else None
        )

    multi_loan_rows = [
        {
            **dict(row),
            'prestamo_activo': loan_title_visible(row['fecha_prestamo_activo'], row['prestamo_activo'] or '-'),
        }
        for row in multi_loan_rows
    ]
    trend_snapshot = build_period_trend_snapshot(trend_period_rows)
    financial_trend = build_variation(
        'Saldo actual total',
        latest_financial_snapshot['saldo_actual_total'] if latest_financial_snapshot else 0,
        previous_financial_snapshot['saldo_actual_total'] if previous_financial_snapshot else None,
        positive_direction='down',
    )
    multi_loan_summary = {
        'socios_multiples': len(multi_loan_rows),
        'socios_marzo_2026': sum(1 for row in multi_loan_rows if row['tiene_marzo_2026']),
    }
    return render_template(
        'reportes.html',
        resumen=summary,
        top=top_balances,
        morosos=[],
        mensual=monthly_projection,
        tendencia_periodos=trend_snapshot,
        tendencia_financiera=financial_trend,
        periodo_financiero=latest_financial_period,
        prestamos_multiples=multi_loan_rows,
        resumen_prestamos_multiples=multi_loan_summary,
    )


@bp.route('/reportes/saldo-actual')
@login_required
def saldo_actual():
    """Show the official monthly balance snapshot used by the association."""
    from app.routes import connect_db

    threshold = 30000
    with connect_db() as conn:
        requested_period = request.args.get('periodo', '').strip()
        financial_period = requested_period or get_latest_financial_period(conn) or get_default_period(conn)
        closed_period = conn.execute("SELECT * FROM periodos WHERE periodo=?", (financial_period,)).fetchone()
        snapshot = get_financial_snapshot_for_period(conn, financial_period) or {
            'saldo_actual_total': 0,
            'acciones_por_socio': 0,
            'total_socios': 0,
        }
        placement_status = get_period_placement_status(
            conn,
            financial_period,
            closed_period['total_recaudado'] if closed_period else 0,
        )
        try:
            accumulated_loan_total = float(get_config_value(conn, 'total_prestamo_acumulado_oficial', 0) or 0)
        except Exception:
            accumulated_loan_total = 0
        balance_rows = conn.execute(
            """
            SELECT periodo, socio_numero, socio_nombre, saldo, fuente
            FROM saldo_historico_mensual
            WHERE periodo=?
            ORDER BY saldo ASC, socio_nombre ASC
            """,
            (financial_period,),
        ).fetchall()

    max_balance = max((row['saldo'] for row in balance_rows), default=1)
    balances = [
        {
            'numero': row['socio_numero'],
            'nombre': row['socio_nombre'],
            'saldo': row['saldo'],
            'fuente': row['fuente'],
            'pct': round((row['saldo'] / max_balance) * 100, 2),
            'supera_umbral': row['saldo'] >= threshold,
        }
        for row in balance_rows
    ]
    summary = {
        'cantidad': len(balances),
        'minimo': min((row['saldo'] for row in balance_rows), default=0),
        'maximo': max_balance if balance_rows else 0,
        'promedio': round(sum(row['saldo'] for row in balance_rows) / len(balance_rows), 2) if balance_rows else 0,
        'vinculados': len([row for row in balance_rows if row['socio_numero'] is not None]),
    }
    historical_close = {
        'periodo': financial_period,
        'periodo_label': format_period_label(financial_period, financial_period),
        'estado': closed_period['estado'] if closed_period else 'Sin cierre',
        'saldo_actual_total': round(float(snapshot['saldo_actual_total'] or 0), 1),
        'acciones_por_socio': round(float(snapshot['acciones_por_socio'] or 0), 1),
        'total_prestamo_acumulado': round(float(accumulated_loan_total or 0), 1),
        'total_mensual': round(float(closed_period['total_recaudado'] or 0), 1) if closed_period else 0,
        'ya_colocado': round(float(placement_status['total_colocado'] or 0), 1),
        'saldo_por_colocar': round(float(placement_status['saldo_por_colocar'] or 0), 1),
    }
    month_composition = {
        'cuotas_mes': round(float(closed_period['total_prestamos'] or 0), 1) if closed_period else 0,
        'aportes_fijos': round(float(closed_period['total_aportes'] or 0), 1) if closed_period else 0,
        'total_mensual': round(float(closed_period['total_recaudado'] or 0), 1) if closed_period else 0,
    }
    placement_rows = [
        {
            'numero': row['socio_numero'],
            'nombre': row['socio_nombre'],
            'fecha_inicio': row['fecha_inicio'],
            'monto_inicial': round(float(row['monto_inicial'] or 0), 1),
            'cuotas': row['plazo_total'],
            'origen': row['origen'],
        }
        for row in placement_status['detalle_rows']
    ]
    return render_template(
        'saldo_actual.html',
        saldos=balances,
        resumen=summary,
        umbral=threshold,
        cierre_historico=historical_close,
        composicion_mes=month_composition,
        colocaciones=placement_rows,
    )


@bp.route('/reportes/saldos-marzo-2026')
@login_required
def saldos_marzo_2026():
    """Legacy compatibility redirect kept for old bookmarks."""
    requested_period = request.args.get('periodo', '').strip()
    if requested_period:
        return redirect(url_for('main.saldo_actual', periodo=requested_period))
    return redirect(url_for('main.saldo_actual'))


@bp.route('/reportes/saldo-actual/vincular', methods=['GET', 'POST'])
@admin_required
def vincular_saldo_actual():
    """Manual linkage screen for the historical March 2026 balance import."""
    from app.routes import connect_db

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

            changes_saved = 0
            for row in rows:
                field_name = f"asignacion_{row['id']}"
                raw_value = request.form.get(field_name, '').strip()
                updated_number = int(raw_value) if raw_value.isdigit() else None
                if updated_number != row['socio_numero']:
                    conn.execute(
                        "UPDATE saldo_historico_mensual SET socio_numero=? WHERE id=?",
                        (updated_number, row['id']),
                    )
                    changes_saved += 1

            conn.commit()
            log_action(
                session.get('username', 'admin'),
                'Vinculación manual de saldos',
                f'Marzo 2026: {changes_saved} cambios guardados',
                categoria='finanzas',
            )
            flash(f'Vinculaciones guardadas correctamente: {changes_saved}.')
            return redirect(url_for('main.vincular_saldo_actual'))

        pending_rows = conn.execute(
            """
            SELECT id, socio_nombre, saldo, socio_numero
            FROM saldo_historico_mensual
            WHERE periodo='2026-03' AND socio_numero IS NULL
            ORDER BY saldo ASC, socio_nombre ASC
            """
        ).fetchall()
        socios = conn.execute("SELECT numero, nombre FROM socios ORDER BY nombre ASC, numero ASC").fetchall()
        linked_rows = conn.execute(
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
        pendientes=pending_rows,
        vinculados=linked_rows,
        socios=socios,
    )


@bp.route('/reportes/saldos-marzo-2026/vincular', methods=['GET', 'POST'])
@admin_required
def vincular_saldos_marzo_2026():
    """Legacy compatibility redirect for the old balance-linking URL."""
    return redirect(url_for('main.vincular_saldo_actual'))


@bp.route('/reportes/mensual.csv')
@login_required
def reporte_mensual_csv():
    """Export the monthly projected collection summary as CSV."""
    from app.routes import connect_db

    with connect_db() as conn:
        rows = conn.execute(
            """
            SELECT substr(fecha,1,7) as periodo,
                   ROUND(SUM(interes),2) as intereses,
                   ROUND(SUM(abono_capital),2) as capital,
                   ROUND(SUM(cuota),2) as cuotas
            FROM cuotas
            WHERE fecha IS NOT NULL
            GROUP BY substr(fecha,1,7)
            ORDER BY periodo DESC
            """
        ).fetchall()

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(['Periodo', 'Intereses', 'Capital', 'Cuotas'])
    for row in rows:
        writer.writerow([row['periodo'], row['intereses'], row['capital'], row['cuotas']])

    response = make_response(csv_buffer.getvalue())
    response.headers["Content-Disposition"] = "attachment; filename=resumen_mensual_jawilvio_v10.csv"
    response.headers["Content-type"] = "text/csv; charset=utf-8"
    return response


@bp.route('/graficos')
@login_required
def graficos():
    """Render the compact charts page based on the projected monthly schedule."""
    from app.routes import connect_db

    with connect_db() as conn:
        monthly_rows = conn.execute(
            """
            SELECT substr(fecha,1,7) as periodo,
                   ROUND(SUM(interes),2) as intereses,
                   ROUND(SUM(abono_capital),2) as capital,
                   ROUND(SUM(cuota),2) as cuotas
            FROM cuotas
            WHERE fecha IS NOT NULL
            GROUP BY substr(fecha,1,7)
            ORDER BY periodo DESC
            LIMIT 12
            """
        ).fetchall()
    monthly_rows = list(reversed(monthly_rows))
    max_installment = max((row['cuotas'] or 0 for row in monthly_rows), default=1)
    return render_template('graficos.html', mensual=monthly_rows, max_cuota=max_installment)


@bp.route('/multas-asistencia', methods=['GET', 'POST'])
@login_required
def multas_asistencia():
    """Review and collect automatic attendance fines by period."""
    from app.routes import connect_db

    with connect_db() as conn:
        selected_period = request.args.get('periodo', '').strip()
        if not selected_period:
            selected_period = get_default_period(conn)
            return redirect(url_for('main.multas_asistencia', periodo=selected_period))

        ensure_monthly_collections(conn, selected_period)
        if request.method == 'POST':
            if not role_can_access_endpoint(session.get('role'), 'main.multas_asistencia', write=True):
                flash('Tu rol puede revisar las multas, pero no actualizar su cobro.')
                return redirect(url_for('main.multas_asistencia', periodo=selected_period))
            is_writable, message, _ = ensure_period_is_writable(conn, selected_period)
            if not is_writable:
                flash(message)
                return redirect(url_for('main.multas_asistencia', periodo=selected_period))

            fine_id = request.form.get('multa_id', '').strip()
            action = request.form.get('action', '').strip()
            fine_row = conn.execute(
                """
                SELECT *
                FROM multas_asistencia
                WHERE id=? AND periodo=?
                """,
                (fine_id, selected_period),
            ).fetchone()
            if not fine_row:
                flash('No se encontró la multa solicitada para este período.')
                return redirect(url_for('main.multas_asistencia', periodo=selected_period))

            if action == 'editar_multa':
                try:
                    inasistencias = max(int(request.form.get('inasistencias', fine_row['inasistencias']) or 0), 0)
                except Exception:
                    inasistencias = int(fine_row['inasistencias'] or 0)
                try:
                    tardanzas = max(int(request.form.get('tardanzas', fine_row['tardanzas']) or 0), 0)
                except Exception:
                    tardanzas = int(fine_row['tardanzas'] or 0)
                if inasistencias == 0 and tardanzas == 0:
                    flash('Usa Eliminar si quieres quitar completamente la multa del período.')
                    return redirect(url_for('main.multas_asistencia', periodo=selected_period))
                multa_inasistencia = round(float(get_config_value(conn, 'multa_inasistencia', 10) or 10), 1)
                multa_tardanza = round(float(get_config_value(conn, 'multa_tardanza', 5) or 5), 1)
                total_multa = round((inasistencias * multa_inasistencia) + (tardanzas * multa_tardanza), 1)
                conn.execute(
                    """
                    UPDATE multas_asistencia
                    SET inasistencias=?,
                        tardanzas=?,
                        monto_multa_inasistencia=?,
                        monto_multa_tardanza=?,
                        total_multa=?,
                        observacion=NULLIF(?, ''),
                        edicion_manual=1,
                        oculto_manual=0,
                        calculado_en=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (
                        inasistencias,
                        tardanzas,
                        multa_inasistencia,
                        multa_tardanza,
                        total_multa,
                        request.form.get('observacion_manual', '').strip(),
                        fine_row['id'],
                    ),
                )
                updated_fine = conn.execute("SELECT * FROM multas_asistencia WHERE id=?", (fine_row['id'],)).fetchone()
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Edición manual de multa automática',
                    f'Período {selected_period}: multa editada para socio {fine_row["socio_numero"]}.',
                    categoria='finanzas',
                    modulo='multas_asistencia',
                    entidad='multa_asistencia',
                    entidad_id=fine_row['id'],
                    periodo=selected_period,
                    antes=fine_row,
                    despues=updated_fine,
                )
                flash('Multa actualizada correctamente y marcada como ajuste manual.')
                return redirect(url_for('main.multas_asistencia', periodo=selected_period))

            if action == 'eliminar_multa':
                conn.execute(
                    """
                    UPDATE multas_asistencia
                    SET oculto_manual=1,
                        edicion_manual=0,
                        calculado_en=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (fine_row['id'],),
                )
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Eliminación manual de multa automática',
                    f'Período {selected_period}: multa ocultada manualmente para socio {fine_row["socio_numero"]}.',
                    categoria='finanzas',
                )
                flash('La multa se eliminó del período y no se regenerará automáticamente mientras siga oculta.')
                return redirect(url_for('main.multas_asistencia', periodo=selected_period))

            if action == 'marcar_cobrada':
                conn.execute(
                    """
                    UPDATE multas_asistencia
                    SET estado_cobro='Cobrada',
                        fecha_cobro=?,
                        observacion=COALESCE(NULLIF(?, ''), observacion)
                    WHERE id=?
                    """,
                    (
                        datetime.now().strftime('%Y-%m-%d %H:%M'),
                        request.form.get('observacion_cobro', '').strip(),
                        fine_row['id'],
                    ),
                )
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Cobro de multa automática',
                    f"Período {selected_period}: multa cobrada para socio {fine_row['socio_numero']}.",
                    categoria='finanzas',
                )
                flash('Multa marcada como cobrada correctamente.')
                return redirect(url_for('main.multas_asistencia', periodo=selected_period))

            if action == 'reabrir_cobro':
                conn.execute(
                    """
                    UPDATE multas_asistencia
                    SET estado_cobro='Pendiente',
                        fecha_cobro=NULL
                    WHERE id=?
                    """,
                    (fine_row['id'],),
                )
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Reapertura de multa automática',
                    f"Período {selected_period}: multa reabierta para socio {fine_row['socio_numero']}.",
                    categoria='finanzas',
                )
                flash('La multa volvió a estado pendiente.')
                return redirect(url_for('main.multas_asistencia', periodo=selected_period))

        fine_config = sync_attendance_fines_for_period(conn, selected_period)
        available_periods = conn.execute(
            """
            SELECT periodo
            FROM (
                SELECT periodo FROM periodos
                UNION
                SELECT substr(fecha, 1, 7) AS periodo FROM asistencia WHERE fecha IS NOT NULL
            )
            WHERE periodo IS NOT NULL AND periodo <> ''
            ORDER BY periodo DESC
            """
        ).fetchall()
        fine_rows = conn.execute(
            """
            SELECT *
            FROM multas_asistencia
            WHERE periodo=?
              AND COALESCE(oculto_manual, 0)=0
            ORDER BY total_multa DESC, socio_numero ASC
            """,
            (selected_period,),
        ).fetchall()
        hidden_fine_numbers = {
            row['socio_numero']
            for row in conn.execute(
                """
                SELECT socio_numero
                FROM multas_asistencia
                WHERE periodo=?
                  AND COALESCE(oculto_manual, 0)=1
                """,
                (selected_period,),
            ).fetchall()
        }

        fine_detail_rows = []
        for row in get_latest_attendance_rows_for_period(conn, selected_period):
            normalized_state = normalize_attendance_state(row['estado'])
            if normalized_state not in MULTA_ASISTENCIA_ESTADOS:
                continue
            if row['socio_numero'] in hidden_fine_numbers:
                continue
            fine_detail_rows.append(
                {
                    'id': row['id'],
                    'socio_numero': row['socio_numero'],
                    'socio_nombre': row['socio_nombre'],
                    'fecha': row['fecha'],
                    'estado': normalized_state,
                    'observacion': row['observacion'],
                    'monto_multa': fine_config['multa_inasistencia'] if normalized_state == 'Faltó' else fine_config['multa_tardanza'],
                }
            )

    total_fines = round(sum(float(row['total_multa'] or 0) for row in fine_rows), 1)
    total_collected = round(sum(float(row['total_multa'] or 0) for row in fine_rows if row['estado_cobro'] == 'Cobrada'), 1)
    summary = {
        'periodo': selected_period,
        'periodo_label': format_period_label(selected_period, selected_period),
        'socios_multados': len(fine_rows),
        'total_inasistencias': sum(int(row['inasistencias'] or 0) for row in fine_rows),
        'total_tardanzas': sum(int(row['tardanzas'] or 0) for row in fine_rows),
        'total_multas': total_fines,
        'total_cobradas': total_collected,
        'total_pendientes': round(total_fines - total_collected, 1),
        'eventos_sancionados': len(fine_detail_rows),
        'multa_maxima': round(max((float(row['total_multa'] or 0) for row in fine_rows), default=0), 1),
    }
    return _render_without_cache(
        'multas_asistencia.html',
        periodo=selected_period,
        periodos=available_periods,
        multas_rows=fine_rows,
        detalle_rows=fine_detail_rows,
        resumen=summary,
        multa_inasistencia=fine_config['multa_inasistencia'],
        multa_tardanza=fine_config['multa_tardanza'],
        puede_editar=role_can_access_endpoint(session.get('role'), 'main.multas_asistencia', write=True),
    )


@bp.route('/multas-asistencia/evento/<int:evento_id>/editar', methods=['POST'])
@admin_required
def multas_asistencia_editar_evento(evento_id):
    """Allow administrators to correct the attendance event that generated a fine."""
    from app.routes import connect_db

    periodo = (request.form.get('periodo') or '').strip()
    if not periodo:
        flash('No se recibió el período del evento que quieres corregir.')
        return redirect(url_for('main.multas_asistencia'))

    with connect_db() as conn:
        evento = conn.execute(
            """
            SELECT *
            FROM asistencia
            WHERE id=? AND fecha IS NOT NULL AND substr(fecha, 1, 7)=?
            """,
            (evento_id, periodo),
        ).fetchone()
        if not evento:
            flash('No se encontró el evento sancionable seleccionado.')
            return redirect(url_for('main.multas_asistencia', periodo=periodo))

        estado_evento = normalize_attendance_state(request.form.get('estado_evento', evento['estado']))
        observacion_evento = request.form.get('observacion_evento', '').strip() or None
        conn.execute(
            """
            UPDATE asistencia
            SET estado=?,
                observacion=?
            WHERE id=?
            """,
            (estado_evento, observacion_evento, evento['id']),
        )
        conn.commit()
        sync_attendance_fines_for_period(conn, periodo)

    log_action(
        session.get('username', 'sistema'),
        'Edición de evento con multa',
        f'Período {periodo}: evento {evento_id} actualizado desde multas automáticas.',
        categoria='finanzas',
    )
    flash('El evento sancionable se actualizó correctamente y las multas del período fueron recalculadas.')
    return redirect(url_for('main.multas_asistencia', periodo=periodo))


@bp.route('/multas-asistencia/evento/<int:evento_id>/eliminar', methods=['POST'])
@admin_required
def multas_asistencia_eliminar_evento(evento_id):
    """Allow administrators to remove the attendance event that generated a fine."""
    from app.routes import connect_db

    periodo = (request.form.get('periodo') or '').strip()
    if not periodo:
        flash('No se recibió el período del evento que quieres eliminar.')
        return redirect(url_for('main.multas_asistencia'))

    with connect_db() as conn:
        evento = conn.execute(
            """
            SELECT *
            FROM asistencia
            WHERE id=? AND fecha IS NOT NULL AND substr(fecha, 1, 7)=?
            """,
            (evento_id, periodo),
        ).fetchone()
        if not evento:
            flash('No se encontró el evento sancionable seleccionado.')
            return redirect(url_for('main.multas_asistencia', periodo=periodo))

        conn.execute("DELETE FROM asistencia WHERE id=?", (evento['id'],))
        conn.commit()
        sync_attendance_fines_for_period(conn, periodo)

    log_action(
        session.get('username', 'sistema'),
        'Eliminación de evento con multa',
        f'Período {periodo}: evento {evento_id} eliminado desde multas automáticas.',
        categoria='finanzas',
    )
    flash('El evento sancionable se eliminó y las multas del período fueron recalculadas.')
    return redirect(url_for('main.multas_asistencia', periodo=periodo))
