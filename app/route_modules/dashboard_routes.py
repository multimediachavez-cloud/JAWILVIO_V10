"""Dashboard routes extracted from the legacy monolith."""

from flask import render_template

from app.routes import (
    bp,
    cleanup_legacy_auto_reunion_assignment,
    ensure_monthly_collections,
    get_config_value,
    get_default_period,
    get_financial_snapshot_for_period,
    get_latest_financial_period,
    get_period_placement_status,
    login_required,
)
from app.utils.trends import build_dashboard_visuals, build_period_trend_snapshot, build_variation


@bp.route('/dashboard')
@login_required
def dashboard():
    """Render the executive dashboard using the current operational period."""
    from app.routes import connect_db

    with connect_db() as conn:
        current_period = get_default_period(conn)
        ensure_monthly_collections(conn, current_period)
        cleanup_legacy_auto_reunion_assignment(conn, current_period)

        total_socios = conn.execute("SELECT COUNT(*) FROM socios").fetchone()[0]
        prestamos_activos = conn.execute(
            """
            SELECT COUNT(*)
            FROM obligaciones_mensuales
            WHERE periodo=? AND COALESCE(saldo_actual, 0) > 0
            """,
            (current_period,),
        ).fetchone()[0]
        total_balance = conn.execute(
            """
            SELECT COALESCE(SUM(saldo_actual), 0)
            FROM obligaciones_mensuales
            WHERE periodo=?
            """,
            (current_period,),
        ).fetchone()[0]
        socios = conn.execute(
            """
            SELECT s.*,
                   COALESCE(om.saldo_actual, s.saldo, 0) as saldo_periodo,
                   COALESCE(MAX(CASE WHEN c.plazo BETWEEN 1 AND 240 THEN c.plazo END), s.plazo_balance, s.meses) as plazo_real
            FROM socios s
            LEFT JOIN cuotas c ON c.socio_numero = s.numero
            LEFT JOIN obligaciones_mensuales om ON om.periodo=? AND om.socio_numero=s.numero
            GROUP BY s.id, s.numero, s.nombre, s.meses, s.plazo_balance, s.fecha_prestamo, s.saldo, s.mes_2026, s.reunion, s.permisos
            ORDER BY s.numero
            LIMIT 8
            """,
            (current_period,),
        ).fetchall()
        total_reuniones = conn.execute(
            "SELECT COUNT(*) FROM reuniones_mensuales WHERE periodo=?",
            (current_period,),
        ).fetchone()[0]
        total_permisos = conn.execute(
            "SELECT COUNT(*) FROM permisos_mensuales WHERE periodo=?",
            (current_period,),
        ).fetchone()[0]
        capital_recuperado = conn.execute("SELECT COALESCE(SUM(abono_capital),0) FROM cuotas WHERE plazo > 0").fetchone()[0]
        intereses_proyectados = conn.execute("SELECT COALESCE(SUM(interes),0) FROM cuotas WHERE plazo > 0").fetchone()[0]
        total_cuotas = conn.execute("SELECT COALESCE(SUM(cuota),0) FROM cuotas WHERE plazo > 0").fetchone()[0]
        cuota_promedio = conn.execute("SELECT COALESCE(AVG(cuota),0) FROM cuotas WHERE cuota > 0").fetchone()[0]
        financial_period = get_latest_financial_period(conn) or current_period
        financial_snapshot = get_financial_snapshot_for_period(conn, financial_period)

        if financial_snapshot:
            official_balance_total = float(financial_snapshot['saldo_actual_total'] or 0)
            official_actions_value = float(financial_snapshot['acciones_por_socio'] or 0)
        else:
            try:
                official_balance_total = float(get_config_value(conn, 'saldo_actual_total_oficial', total_balance or 0) or 0)
            except Exception:
                official_balance_total = float(total_balance or 0)
            try:
                official_actions_value = float(get_config_value(conn, 'acciones_por_socio_oficial', 0) or 0)
            except Exception:
                official_actions_value = 0

        try:
            total_prestamo_acumulado = float(get_config_value(conn, 'total_prestamo_acumulado_oficial', total_cuotas or 0) or 0)
        except Exception:
            total_prestamo_acumulado = float(total_cuotas or 0)

        resumen_mensual = conn.execute(
            """
            SELECT substr(fecha,1,7) as periodo,
                   ROUND(SUM(interes),2) as intereses,
                   ROUND(SUM(abono_capital),2) as capital,
                   ROUND(SUM(cuota),2) as cuotas
            FROM cuotas
            WHERE fecha IS NOT NULL
            GROUP BY substr(fecha,1,7)
            ORDER BY periodo DESC
            LIMIT 6
            """
        ).fetchall()
        import_status_row = conn.execute("SELECT valor FROM meta WHERE clave='import_status'").fetchone()
        active_period_row = conn.execute(
            """
            SELECT *
            FROM periodos
            WHERE periodo=?
            """,
            (current_period,),
        ).fetchone()
        placement_status = get_period_placement_status(
            conn,
            current_period,
            active_period_row['total_recaudado'] if active_period_row else 0,
        )
        period_detail = conn.execute(
            """
            SELECT socio_numero, socio_nombre, cuota_prestamo, aporte_mensual, total_mes, saldo_actual
            FROM obligaciones_mensuales
            WHERE periodo=?
            ORDER BY total_mes DESC, socio_numero ASC
            LIMIT 8
            """,
            (current_period,),
        ).fetchall()
        recent_periods = conn.execute(
            """
            SELECT periodo, estado, total_recaudado, total_colocado, saldo_por_colocar
            FROM periodos
            ORDER BY periodo DESC
            LIMIT 6
            """
        ).fetchall()
        period_loans = conn.execute(
            """
            SELECT COUNT(*) as total,
                   COALESCE(SUM(CASE WHEN estado='Reservado' THEN 1 ELSE 0 END), 0) as reservados,
                   COALESCE(SUM(CASE WHEN estado='Aprobado' THEN 1 ELSE 0 END), 0) as aprobados
            FROM prestamos_nuevos
            WHERE periodo=?
            """,
            (current_period,),
        ).fetchone()
        previous_period = conn.execute(
            """
            SELECT periodo, estado, total_recaudado, total_colocado, saldo_por_colocar
            FROM periodos
            WHERE periodo < ?
            ORDER BY periodo DESC
            LIMIT 1
            """,
            (current_period,),
        ).fetchone()
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
            LIMIT 8
            """
        ).fetchall()
        top_balances = conn.execute(
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
            """,
            (current_period,),
        ).fetchall()
        multi_loan_summary = conn.execute(
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
        previous_financial_period_row = conn.execute(
            """
            SELECT periodo
            FROM periodos
            WHERE estado IN ('Colocado', 'Cerrado')
              AND periodo < ?
            ORDER BY periodo DESC
            LIMIT 1
            """,
            (financial_period,),
        ).fetchone()
        previous_financial_snapshot = (
            get_financial_snapshot_for_period(conn, previous_financial_period_row['periodo'])
            if previous_financial_period_row else None
        )

    balance_reference = total_balance or 0
    dashboard_balance_total = official_balance_total or balance_reference
    chart_reference = max(balance_reference, capital_recuperado or 0, intereses_proyectados or 0, total_cuotas or 0, 1)
    bars = {
        'saldo_total': round((balance_reference / chart_reference) * 100, 2),
        'capital_recuperado': round(((capital_recuperado or 0) / chart_reference) * 100, 2),
        'intereses': round(((intereses_proyectados or 0) / chart_reference) * 100, 2),
        'cuotas_total': round(((total_cuotas or 0) / chart_reference) * 100, 2),
    }
    period_summary = {
        'periodo': current_period,
        'estado': active_period_row['estado'] if active_period_row else 'Abierto',
        'socios': active_period_row['total_socios'] if active_period_row else 0,
        'total_prestamos': active_period_row['total_prestamos'] if active_period_row else 0,
        'total_aportes': active_period_row['total_aportes'] if active_period_row else 0,
        'total_recaudado': active_period_row['total_recaudado'] if active_period_row else 0,
        'total_colocado': placement_status['total_colocado'] if placement_status else 0,
        'saldo_por_colocar': placement_status['saldo_por_colocar'] if placement_status else 0,
        'colocacion_completa': (placement_status['saldo_por_colocar'] if placement_status else 0) <= 0.0001,
        'porcentaje_colocado': round(
            (((placement_status['total_colocado'] if placement_status else 0) / ((active_period_row['total_recaudado'] if active_period_row else 0) or 1)) * 100),
            1,
        ) if (active_period_row and (active_period_row['total_recaudado'] or 0) > 0) else 0,
        'promedio_por_socio': round(
            ((active_period_row['total_recaudado'] if active_period_row else 0) / ((active_period_row['total_socios'] if active_period_row else 0) or 1)),
            1,
        ) if (active_period_row and (active_period_row['total_socios'] or 0) > 0) else 0,
    }
    highest_month_charge = period_detail[0] if period_detail else None
    highest_balance = top_balances[0] if top_balances else None
    placement_counts = placement_status['origin_counts'] if placement_status else {'excel': 0, 'app_aprobado': 0, 'app_reservado': 0}
    actions_per_member = official_actions_value or (round((dashboard_balance_total / total_socios), 1) if total_socios else 0)
    active_portfolio_pct = round(((balance_reference or 0) / (total_prestamo_acumulado or 1)) * 100, 1) if (total_prestamo_acumulado or 0) > 0 else 0
    capital_recovered_pct = round(((capital_recuperado or 0) / (total_prestamo_acumulado or 1)) * 100, 1) if (total_prestamo_acumulado or 0) > 0 else 0
    interest_pct = round(((intereses_proyectados or 0) / (total_prestamo_acumulado or 1)) * 100, 1) if (total_prestamo_acumulado or 0) > 0 else 0
    placement_gap_pct = round((((period_summary['saldo_por_colocar'] or 0) / ((period_summary['total_recaudado'] or 0) or 1)) * 100), 1) if (period_summary['total_recaudado'] or 0) > 0 else 0
    finance_panel = {
        'saldo_actual_total': dashboard_balance_total,
        'total_prestamo_acumulado': total_prestamo_acumulado,
        'capital_recuperado': capital_recuperado,
        'intereses_proyectados': intereses_proyectados,
        'fondo_mes': period_summary['total_recaudado'],
        'ya_colocado': period_summary['total_colocado'],
        'por_colocar': period_summary['saldo_por_colocar'],
        'acciones_por_socio': actions_per_member,
        'promedio_periodo': period_summary['promedio_por_socio'],
        'cartera_vigente_pct': active_portfolio_pct,
        'capital_recuperado_pct': capital_recovered_pct,
        'intereses_pct': interest_pct,
        'colocacion_pct': period_summary['porcentaje_colocado'],
        'brecha_colocacion_pct': placement_gap_pct,
    }
    trend_snapshot = build_period_trend_snapshot(trend_period_rows)
    financial_trend = build_variation(
        'Saldo actual total',
        dashboard_balance_total,
        previous_financial_snapshot['saldo_actual_total'] if previous_financial_snapshot else None,
        positive_direction='down',
    )
    dashboard_visuals = build_dashboard_visuals(
        trend_snapshot['series'],
        top_balances,
        period_summary,
        placement_counts,
    )

    return render_template(
        'dashboard_reporte.html',
        total_socios=total_socios,
        prestamos_activos=prestamos_activos,
        saldo_total=total_balance,
        alertas=0,
        socios=socios,
        reuniones=total_reuniones,
        permisos=total_permisos,
        capital_recuperado=capital_recuperado,
        intereses=intereses_proyectados,
        cuotas_total=total_cuotas,
        total_prestamo_acumulado=total_prestamo_acumulado,
        cuota_promedio=cuota_promedio,
        morosos=0,
        top_morosos=[],
        resumen_mensual=resumen_mensual,
        import_status=import_status_row['valor'] if import_status_row else 'Sin importación',
        fondo_total=balance_reference,
        saldo_actual_total_panel=dashboard_balance_total,
        barras=bars,
        periodo_actual_resumen=period_summary,
        detalle_periodo=period_detail,
        historial_periodos=recent_periods,
        prestamos_periodo=period_loans,
        periodo_anterior=previous_period,
        colocacion_detalle_rows=placement_status['detalle_rows'] if placement_status else [],
        colocacion_counts=placement_counts,
        top_saldos=top_balances,
        resumen_prestamos_multiples=multi_loan_summary,
        mayor_total_mes=highest_month_charge,
        mayor_saldo=highest_balance,
        acciones_por_socio=actions_per_member,
        panel_financiero=finance_panel,
        periodo_financiero_panel=financial_period,
        tendencia_periodos=trend_snapshot,
        tendencia_financiera=financial_trend,
        dashboard_visuals=dashboard_visuals,
    )
