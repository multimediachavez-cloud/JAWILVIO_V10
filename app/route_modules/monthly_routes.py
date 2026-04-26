"""Routes for the monthly operational cycle."""

from datetime import datetime

from flask import flash, make_response, redirect, render_template, request, session, url_for

from app.routes import (
    bp,
    build_funding_suggestions,
    create_loan_history_record,
    create_reserved_loan,
    ensure_monthly_collections,
    ensure_period_is_writable,
    get_config_value,
    get_default_period,
    get_next_period,
    get_period_placement_status,
    log_action,
    login_required,
    sync_dashboard_financial_snapshot,
)


def _render_without_cache(template_name, **context):
    """Render monthly workflow screens with explicit no-cache headers."""
    response = make_response(render_template(template_name, **context))
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@bp.route('/cierre-mensual', methods=['GET', 'POST'])
@login_required
def cierre_mensual():
    """Control the period close/open workflow and its placement status."""
    from app.routes import connect_db

    selected_period = request.args.get('periodo', '').strip()
    with connect_db() as conn:
        if request.method == 'GET' and not selected_period:
            selected_period = get_default_period(conn)
            return redirect(url_for('main.cierre_mensual', periodo=selected_period))

        selected_period = selected_period or get_default_period(conn)
        ensure_monthly_collections(conn, selected_period)

        if request.method == 'POST':
            action = request.form.get('action', '').strip()
            selected_period = request.form.get('periodo', '').strip() or selected_period
            ensure_monthly_collections(conn, selected_period)
            is_writable, message, _ = ensure_period_is_writable(conn, selected_period)
            if not is_writable:
                flash(message)
                return redirect(url_for('main.cierre_mensual', periodo=selected_period))

            current_close = conn.execute("SELECT * FROM periodos WHERE periodo=?", (selected_period,)).fetchone()
            placement_status = get_period_placement_status(conn, selected_period, current_close['total_recaudado'] if current_close else 0)

            if action == 'cerrar_periodo':
                if not current_close:
                    flash('No se encontró el período para cerrar.')
                elif (placement_status['saldo_por_colocar'] or 0) > 0.0001:
                    flash(f'No se puede cerrar el período. Aún faltan S/ {placement_status["saldo_por_colocar"]:.1f} por colocar.')
                else:
                    conn.execute(
                        """
                        UPDATE periodos
                        SET estado='Cerrado',
                            fecha_calculo=CURRENT_TIMESTAMP
                        WHERE periodo=?
                        """,
                        (selected_period,),
                    )
                    updated_close = conn.execute("SELECT * FROM periodos WHERE periodo=?", (selected_period,)).fetchone()
                    sync_dashboard_financial_snapshot(conn, selected_period)
                    conn.commit()
                    log_action(
                        session.get('username', 'sistema'),
                        'Cierre de período',
                        f'Período {selected_period} cerrado manualmente.',
                        categoria='finanzas',
                        modulo='cierre_mensual',
                        entidad='periodo',
                        entidad_id=selected_period,
                        periodo=selected_period,
                        antes=current_close,
                        despues=updated_close,
                    )
                    flash('Período cerrado correctamente.')
                return redirect(url_for('main.cierre_mensual', periodo=selected_period))

            if action == 'abrir_mes_nuevo':
                next_period = get_next_period(selected_period)
                if not current_close:
                    flash('No se encontró el período actual para abrir el siguiente mes.')
                    return redirect(url_for('main.cierre_mensual', periodo=selected_period))
                if (placement_status['saldo_por_colocar'] or 0) > 0.0001:
                    flash(f'No se puede abrir {next_period}. Primero debes colocar todo el fondo de {selected_period}. Aún faltan S/ {placement_status["saldo_por_colocar"]:.1f}.')
                    return redirect(url_for('main.cierre_mensual', periodo=selected_period))

                conn.execute(
                    """
                    UPDATE periodos
                    SET estado='Cerrado',
                        fecha_calculo=CURRENT_TIMESTAMP
                    WHERE periodo=?
                    """,
                    (selected_period,),
                )
                ensure_monthly_collections(conn, next_period)
                updated_close = conn.execute("SELECT * FROM periodos WHERE periodo=?", (selected_period,)).fetchone()
                opened_period = conn.execute("SELECT * FROM periodos WHERE periodo=?", (next_period,)).fetchone()
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Apertura de período',
                    f'Se cerró {selected_period} y se abrió el período {next_period}.',
                    categoria='finanzas',
                    modulo='cierre_mensual',
                    entidad='periodo',
                    entidad_id=next_period,
                    periodo=next_period,
                    antes=current_close,
                    despues=opened_period,
                    metadata={'periodo_cerrado': selected_period, 'periodo_abierto': next_period, 'cierre_actualizado': dict(updated_close) if updated_close else None},
                )
                flash(f'Se cerró {selected_period} y se abrió el nuevo período {next_period}.')
                return redirect(url_for('main.cierre_mensual', periodo=next_period))

        close_row = conn.execute("SELECT * FROM periodos WHERE periodo=?", (selected_period,)).fetchone()
        official_historical_period = str(get_config_value(conn, 'resumen_financiero_periodo_oficial', '2026-03') or '2026-03')
        next_operational_period = get_next_period(official_historical_period)
        placement_status = get_period_placement_status(conn, selected_period, close_row['total_recaudado'] if close_row else 0)
        history_rows = conn.execute(
            """
            SELECT *
            FROM periodos
            ORDER BY periodo DESC
            """
        ).fetchall()

    return _render_without_cache(
        'cierre_mensual.html',
        periodo=selected_period,
        cierre=close_row,
        historico=history_rows,
        total_colocado=placement_status['total_colocado'] or 0,
        saldo_por_colocar=placement_status['saldo_por_colocar'] or 0,
        colocacion_fuente=placement_status['fuente'],
        colocacion_excel_rows=placement_status['excel_rows'],
        colocacion_detalle_rows=placement_status['detalle_rows'],
        colocacion_origin_counts=placement_status['origin_counts'],
        colocacion_completa=(placement_status['saldo_por_colocar'] or 0) <= 0.0001,
        periodo_historico_oficial=official_historical_period,
        siguiente_periodo_operativo=next_operational_period,
        es_periodo_historico_oficial=(selected_period == official_historical_period),
    )


@bp.route('/nuevos-prestamos', methods=['GET', 'POST'])
@login_required
def nuevos_prestamos():
    """Manage funding suggestions and period loans from one monthly screen."""
    from app.routes import connect_db

    selected_loan_id = request.args.get('prestamo_id', type=int)
    requested_period = request.args.get('periodo', '').strip()
    requested_filter = request.args.get('filtro', 'todos').strip().lower()

    with connect_db() as conn:
        if request.method == 'GET' and not requested_period:
            default_period = get_default_period(conn)
            selected_filter = requested_filter if requested_filter in ('todos', 'manuales', 'automaticos') else 'todos'
            return redirect(url_for('main.nuevos_prestamos', periodo=default_period, filtro=selected_filter))

        selected_period = requested_period or get_default_period(conn)
        selected_filter = requested_filter if requested_filter in ('todos', 'manuales', 'automaticos') else 'todos'
        ensure_monthly_collections(conn, selected_period)

        if request.method == 'POST':
            action = request.form.get('action', '').strip()
            selected_period = request.form.get('periodo', '').strip() or selected_period
            posted_filter = request.form.get('filtro', selected_filter).strip().lower()
            selected_filter = posted_filter if posted_filter in ('todos', 'manuales', 'automaticos') else 'todos'
            ensure_monthly_collections(conn, selected_period)
            is_writable, message, _ = ensure_period_is_writable(conn, selected_period)
            if not is_writable:
                flash(message)
                return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, filtro=selected_filter))

            monthly_rate = float(get_config_value(conn, 'tasa_prestamo_mensual', '0.01'))
            min_installments = int(float(get_config_value(conn, 'min_cuotas_prestamo', '12')))
            max_installments = int(float(get_config_value(conn, 'max_cuotas_prestamo', '84')))

            if action == 'guardar_prioridades':
                conn.execute("DELETE FROM prioridad_colocacion_manual WHERE periodo=?", (selected_period,))
                member_ids = conn.execute("SELECT numero FROM socios ORDER BY numero").fetchall()
                saved_priorities = 0
                saved_order = []
                for row in member_ids:
                    raw_priority = request.form.get(f'prioridad_{row["numero"]}', '').strip()
                    if raw_priority.isdigit():
                        priority = int(raw_priority)
                        if priority > 0:
                            conn.execute(
                                "INSERT OR REPLACE INTO prioridad_colocacion_manual(periodo, socio_numero, prioridad) VALUES(?,?,?)",
                                (selected_period, row['numero'], priority),
                            )
                            saved_priorities += 1
                            saved_order.append({'socio_numero': row['numero'], 'prioridad': priority})
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Prioridades manuales actualizadas',
                    f'Período {selected_period}: prioridades guardadas ({saved_priorities}).',
                    categoria='operacion',
                    modulo='nuevos_prestamos',
                    entidad='prioridad_colocacion_manual',
                    entidad_id=selected_period,
                    periodo=selected_period,
                    despues=saved_order,
                )
                flash(f'Orden manual guardado. Prioridades actualizadas: {saved_priorities}.')
                return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, filtro=selected_filter))

            if action == 'limpiar_prioridades':
                previous_rows = conn.execute(
                    """
                    SELECT socio_numero, prioridad
                    FROM prioridad_colocacion_manual
                    WHERE periodo=?
                    ORDER BY prioridad ASC, socio_numero ASC
                    """,
                    (selected_period,),
                ).fetchall()
                conn.execute("DELETE FROM prioridad_colocacion_manual WHERE periodo=?", (selected_period,))
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Prioridades manuales limpiadas',
                    f'Período {selected_period}: se limpió el orden manual.',
                    categoria='operacion',
                    modulo='nuevos_prestamos',
                    entidad='prioridad_colocacion_manual',
                    entidad_id=selected_period,
                    periodo=selected_period,
                    antes=previous_rows,
                )
                flash('El orden manual se limpió para este período.')
                return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, filtro=selected_filter))

            if action in ('subir_prioridad', 'bajar_prioridad', 'enviar_arriba', 'enviar_abajo'):
                member_number = request.form.get('socio_numero', type=int)
                current_order = conn.execute(
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
                    (selected_period, selected_period),
                ).fetchall()
                ordered_numbers = [row['numero'] for row in current_order]
                if member_number in ordered_numbers:
                    previous_order = list(ordered_numbers)
                    index = ordered_numbers.index(member_number)
                    if action == 'subir_prioridad' and index > 0:
                        ordered_numbers[index - 1], ordered_numbers[index] = ordered_numbers[index], ordered_numbers[index - 1]
                    elif action == 'bajar_prioridad' and index < len(ordered_numbers) - 1:
                        ordered_numbers[index + 1], ordered_numbers[index] = ordered_numbers[index], ordered_numbers[index + 1]
                    elif action == 'enviar_arriba' and index > 0:
                        number = ordered_numbers.pop(index)
                        ordered_numbers.insert(0, number)
                    elif action == 'enviar_abajo' and index < len(ordered_numbers) - 1:
                        number = ordered_numbers.pop(index)
                        ordered_numbers.append(number)
                    conn.execute("DELETE FROM prioridad_colocacion_manual WHERE periodo=?", (selected_period,))
                    for priority, number in enumerate(ordered_numbers, start=1):
                        conn.execute(
                            """
                            INSERT INTO prioridad_colocacion_manual(periodo, socio_numero, prioridad)
                            VALUES(?,?,?)
                            """,
                            (selected_period, number, priority),
                        )
                    conn.commit()
                    movement = {
                        'subir_prioridad': 'subida',
                        'bajar_prioridad': 'bajada',
                        'enviar_arriba': 'envío al inicio',
                        'enviar_abajo': 'envío al final',
                    }[action]
                    log_action(
                        session.get('username', 'sistema'),
                        'Prioridad manual reordenada',
                        f'Período {selected_period}: {movement} del socio {member_number}.',
                        categoria='operacion',
                        modulo='nuevos_prestamos',
                        entidad='prioridad_colocacion_manual',
                        entidad_id=member_number,
                        periodo=selected_period,
                        antes=previous_order,
                        despues=ordered_numbers,
                    )
                    flash(f'Orden manual actualizado: {movement}.')
                else:
                    flash('No se encontró el socio para mover la prioridad.')
                return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, filtro=selected_filter))

            if action == 'aplicar_sugerencia':
                period_row = conn.execute("SELECT * FROM periodos WHERE periodo=?", (selected_period,)).fetchone()
                reserved_amount = conn.execute(
                    """
                    SELECT COALESCE(SUM(monto), 0)
                    FROM prestamos_nuevos
                    WHERE periodo=? AND estado IN ('Reservado', 'Aprobado')
                    """,
                    (selected_period,),
                ).fetchone()[0]
                available_fund = max((period_row['total_recaudado'] if period_row else 0) - (reserved_amount or 0), 0)
                created_loans = 0
                last_created_id = None
                suggestions, _ = build_funding_suggestions(conn, selected_period, available_fund, min_installments, max_installments)
                created_summary = []
                for suggestion in suggestions:
                    socio = {'numero': suggestion['numero'], 'nombre': suggestion['nombre']}
                    last_created_id = create_reserved_loan(
                        conn,
                        selected_period,
                        socio,
                        suggestion['monto_sugerido'],
                        suggestion['cuotas_sugeridas'],
                        monthly_rate,
                        f'{selected_period}-29',
                    )
                    created_summary.append(
                        {
                            'prestamo_id': last_created_id,
                            'socio_numero': socio['numero'],
                            'monto': suggestion['monto_sugerido'],
                            'cuotas': suggestion['cuotas_sugeridas'],
                        }
                    )
                    created_loans += 1
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Aplicación de sugerencia automática',
                    f'Periodo {selected_period} - Préstamos creados: {created_loans}',
                    categoria='finanzas',
                    modulo='nuevos_prestamos',
                    entidad='prestamo_nuevo',
                    entidad_id=last_created_id,
                    periodo=selected_period,
                    despues=created_summary,
                )
                if created_loans:
                    flash(f'Sugerencia aplicada correctamente. Préstamos creados: {created_loans}.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, prestamo_id=last_created_id, filtro=selected_filter))
                flash('No hubo préstamos sugeridos para crear en este período.')
                return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, filtro=selected_filter))

            if action in ('aprobar', 'anular'):
                loan_id = request.form.get('prestamo_id', type=int)
                loan_row = conn.execute("SELECT * FROM prestamos_nuevos WHERE id=?", (loan_id,)).fetchone()
                if not loan_row:
                    flash('No se encontró el préstamo seleccionado.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, filtro=selected_filter))

                if action == 'anular':
                    if loan_row['estado'] == 'Aprobado':
                        flash('No puedes anular aquí un préstamo que ya fue aprobado.')
                    elif loan_row['estado'] == 'Anulado':
                        flash('Ese préstamo ya estaba anulado.')
                    else:
                        create_loan_history_record(
                            conn,
                            loan_row['socio_numero'],
                            loan_row['socio_nombre'],
                            loan_row,
                            'Anulacion',
                            'Anulado',
                            'Préstamo nuevo anulado antes de convertirse en cronograma real.',
                            session.get('username', 'sistema'),
                        )
                        conn.execute("UPDATE prestamos_nuevos SET estado='Anulado' WHERE id=?", (loan_id,))
                        updated_loan = conn.execute("SELECT * FROM prestamos_nuevos WHERE id=?", (loan_id,)).fetchone()
                        conn.commit()
                        log_action(
                            session.get('username', 'sistema'),
                            'Préstamo anulado',
                            f'ID {loan_id} - Socio {loan_row["socio_numero"]} - Monto {loan_row["monto"]:.1f}',
                            categoria='finanzas',
                            modulo='nuevos_prestamos',
                            entidad='prestamo_nuevo',
                            entidad_id=loan_id,
                            periodo=selected_period,
                            antes=loan_row,
                            despues=updated_loan,
                        )
                        flash('Préstamo anulado. El fondo quedó liberado nuevamente.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, prestamo_id=loan_id, filtro=selected_filter))

                if loan_row['estado'] == 'Aprobado':
                    flash('Ese préstamo ya fue aprobado.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, prestamo_id=loan_id, filtro=selected_filter))
                if loan_row['estado'] == 'Anulado':
                    flash('No se puede aprobar un préstamo que ya fue anulado.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, prestamo_id=loan_id, filtro=selected_filter))

                schedule_rows = conn.execute(
                    """
                    SELECT *
                    FROM prestamos_nuevos_cronograma
                    WHERE prestamo_nuevo_id=?
                    ORDER BY plazo
                    """,
                    (loan_id,),
                ).fetchall()
                if not schedule_rows:
                    flash('El préstamo no tiene cronograma disponible para aprobar.')
                    return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, prestamo_id=loan_id, filtro=selected_filter))

                previous_member = conn.execute(
                    "SELECT numero, nombre, meses, plazo_balance, fecha_prestamo, saldo FROM socios WHERE numero=?",
                    (loan_row['socio_numero'],),
                ).fetchone()
                previous_installments = conn.execute(
                    "SELECT * FROM cuotas WHERE socio_numero=? ORDER BY plazo, fecha",
                    (loan_row['socio_numero'],),
                ).fetchall()
                create_loan_history_record(
                    conn,
                    loan_row['socio_numero'],
                    loan_row['socio_nombre'],
                    loan_row,
                    'Aprobacion',
                    'Aprobado',
                    'Préstamo aprobado y cronograma anterior reemplazado por uno nuevo.',
                    session.get('username', 'sistema'),
                    snapshot_rows=previous_installments,
                    socio_anterior=previous_member,
                )
                conn.execute("DELETE FROM cuotas WHERE socio_numero=?", (loan_row['socio_numero'],))
                for row in schedule_rows:
                    conn.execute(
                        """
                        INSERT INTO cuotas(socio_numero, plazo, fecha, prestamo, interes, abono_capital, cuota, saldo, hoja_origen)
                        VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            loan_row['socio_numero'],
                            row['plazo'],
                            row['fecha'],
                            row['prestamo'],
                            row['interes'],
                            row['abono_capital'],
                            row['cuota'],
                            row['saldo'],
                            f'prestamo_nuevo:{loan_id}',
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
                        loan_row['cuotas'],
                        loan_row['cuotas'],
                        loan_row['fecha_desembolso'],
                        loan_row['cuota_inicial'],
                        loan_row['socio_numero'],
                    ),
                )
                conn.execute("UPDATE prestamos_nuevos SET estado='Aprobado' WHERE id=?", (loan_id,))
                updated_loan = conn.execute("SELECT * FROM prestamos_nuevos WHERE id=?", (loan_id,)).fetchone()
                conn.commit()
                log_action(
                    session.get('username', 'sistema'),
                    'Préstamo aprobado',
                    f'ID {loan_id} - Socio {loan_row["socio_numero"]} - Monto {loan_row["monto"]:.1f} - Cuotas {loan_row["cuotas"]}',
                    categoria='finanzas',
                    modulo='nuevos_prestamos',
                    entidad='prestamo_nuevo',
                    entidad_id=loan_id,
                    periodo=selected_period,
                    antes=loan_row,
                    despues=updated_loan,
                    metadata={'socio_anterior': dict(previous_member) if previous_member else None, 'cuotas_reemplazadas': len(previous_installments)},
                )
                flash('Préstamo aprobado y aplicado al cronograma real del socio.')
                return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, prestamo_id=loan_id, filtro=selected_filter))

            member_number = request.form.get('socio_numero', type=int)
            amount = request.form.get('monto', type=float)
            installments = request.form.get('cuotas', type=int)
            disbursement_date = request.form.get('fecha_desembolso', '').strip() or f'{selected_period}-29'

            period_row = conn.execute("SELECT * FROM periodos WHERE periodo=?", (selected_period,)).fetchone()
            placement_status = get_period_placement_status(conn, selected_period, period_row['total_recaudado'] if period_row else 0)
            available_fund = placement_status['saldo_por_colocar']
            member = conn.execute("SELECT numero, nombre FROM socios WHERE numero=?", (member_number,)).fetchone()

            error = None
            if not member:
                error = 'Selecciona un socio válido.'
            elif amount is None or amount <= 0:
                error = 'Ingresa un monto válido.'
            elif installments is None or installments < min_installments or installments > max_installments:
                error = f'Las cuotas deben estar entre {min_installments} y {max_installments}.'
            elif amount > available_fund:
                error = f'El monto excede el fondo disponible del período: S/ {available_fund:.1f}.'

            if error:
                flash(error)
                return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, filtro=selected_filter))

            loan_id = create_reserved_loan(
                conn,
                selected_period,
                member,
                amount,
                installments,
                monthly_rate,
                disbursement_date,
            )
            created_loan = conn.execute("SELECT * FROM prestamos_nuevos WHERE id=?", (loan_id,)).fetchone()
            conn.commit()
            log_action(
                session.get('username', 'sistema'),
                'Nuevo préstamo reservado',
                f'Periodo {selected_period} - Socio {member["numero"]} - Monto {amount:.1f} - Cuotas {installments}',
                categoria='finanzas',
                modulo='nuevos_prestamos',
                entidad='prestamo_nuevo',
                entidad_id=loan_id,
                periodo=selected_period,
                despues=created_loan,
            )
            flash('Nuevo préstamo generado correctamente.')
            return redirect(url_for('main.nuevos_prestamos', periodo=selected_period, prestamo_id=loan_id, filtro=selected_filter))

        period_row = conn.execute("SELECT * FROM periodos WHERE periodo=?", (selected_period,)).fetchone()
        official_historical_period = str(get_config_value(conn, 'resumen_financiero_periodo_oficial', '2026-03') or '2026-03')
        next_operational_period = get_next_period(official_historical_period)
        monthly_rate = float(get_config_value(conn, 'tasa_prestamo_mensual', '0.01'))
        min_installments = int(float(get_config_value(conn, 'min_cuotas_prestamo', '12')))
        max_installments = int(float(get_config_value(conn, 'max_cuotas_prestamo', '84')))
        placement_status = get_period_placement_status(conn, selected_period, period_row['total_recaudado'] if period_row else 0)
        reserved_fund = placement_status['total_colocado']
        total_fund = period_row['total_recaudado'] if period_row else 0
        available_fund = placement_status['saldo_por_colocar']
        placement_pct = round((((reserved_fund or 0) / (total_fund or 1)) * 100), 1) if (total_fund or 0) > 0 else 0
        socios = conn.execute("SELECT numero, nombre FROM socios ORDER BY numero").fetchall()
        suggestions, remaining_unsuggested = build_funding_suggestions(
            conn,
            selected_period,
            available_fund,
            min_installments,
            max_installments,
        )
        manual_priorities = {
            row['socio_numero']: row['prioridad']
            for row in conn.execute(
                """
                SELECT socio_numero, prioridad
                FROM prioridad_colocacion_manual
                WHERE periodo=?
                """,
                (selected_period,),
            ).fetchall()
        }
        manual_order_rows = conn.execute(
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
            (selected_period, selected_period),
        ).fetchall()
        filter_counts = {
            'todos': len(manual_order_rows),
            'manuales': sum(1 for row in manual_order_rows if manual_priorities.get(row['numero'])),
            'automaticos': sum(1 for row in manual_order_rows if not manual_priorities.get(row['numero'])),
        }
        priority_summary = {
            'manuales_activos': filter_counts['manuales'],
            'automaticos_activos': filter_counts['automaticos'],
            'socios_sugeridos_mes': len(suggestions),
            'monto_sugerido_mes': round(sum((row.get('monto_sugerido') or 0) for row in suggestions), 1),
            'restante_sin_sugerir': round(remaining_unsuggested or 0, 1),
            'porcentaje_sugerido_fondo': round(
                ((sum((row.get('monto_sugerido') or 0) for row in suggestions) / (available_fund or 1)) * 100),
                1,
            ) if (available_fund or 0) > 0 else 0,
        }
        if selected_filter == 'manuales':
            manual_order_rows = [row for row in manual_order_rows if manual_priorities.get(row['numero'])]
            suggestions = [row for row in suggestions if row.get('prioridad_manual') is not None]
        elif selected_filter == 'automaticos':
            manual_order_rows = [row for row in manual_order_rows if not manual_priorities.get(row['numero'])]
            suggestions = [row for row in suggestions if row.get('prioridad_manual') is None]

        period_loans = conn.execute(
            """
            SELECT *
            FROM prestamos_nuevos
            WHERE periodo=?
            ORDER BY id DESC
            """,
            (selected_period,),
        ).fetchall()
        if not selected_loan_id and period_loans:
            selected_loan_id = period_loans[0]['id']
        selected_loan = conn.execute("SELECT * FROM prestamos_nuevos WHERE id=?", (selected_loan_id,)).fetchone() if selected_loan_id else None
        schedule_rows = conn.execute(
            """
            SELECT *
            FROM prestamos_nuevos_cronograma
            WHERE prestamo_nuevo_id=?
            ORDER BY plazo
            """,
            (selected_loan_id,),
        ).fetchall() if selected_loan_id else []

    return _render_without_cache(
        'nuevos_prestamos.html',
        periodo=selected_period,
        cierre=period_row,
        socios=socios,
        prestamos=period_loans,
        prestamo=selected_loan,
        cronograma=schedule_rows,
        fondo_total=total_fund or 0,
        fondo_reservado=reserved_fund or 0,
        fondo_disponible=available_fund or 0,
        saldo_por_colocar=available_fund or 0,
        colocacion_fuente=placement_status['fuente'],
        colocacion_excel_rows=placement_status['excel_rows'],
        colocacion_detalle_rows=placement_status['detalle_rows'],
        colocacion_origin_counts=placement_status['origin_counts'],
        porcentaje_colocado=placement_pct,
        colocacion_completa=(available_fund or 0) <= 0.0001,
        sugerencias=suggestions,
        restante_sugerido=remaining_unsuggested,
        filtro=selected_filter,
        filtro_counts=filter_counts,
        resumen_prioridades=priority_summary,
        prioridades=manual_priorities,
        socios_orden_manual=manual_order_rows,
        tasa_mensual=monthly_rate,
        min_cuotas=min_installments,
        max_cuotas=max_installments,
        periodo_historico_oficial=official_historical_period,
        siguiente_periodo_operativo=next_operational_period,
        es_periodo_historico_oficial=(selected_period == official_historical_period),
    )
