"""Helpers to build monthly statistics and trend summaries for the UI."""

from __future__ import annotations


def _to_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def build_variation(label, current, previous=None, *, decimals=1, positive_direction='up'):
    """Return a normalized comparison object for cards and trend badges."""
    current_value = round(_to_float(current), decimals)
    previous_value = None if previous is None else round(_to_float(previous), decimals)

    if previous_value is None:
        delta = 0.0
        delta_pct = None
        direction = 'flat'
        status = 'neutral'
    else:
        delta = round(current_value - previous_value, decimals)
        if abs(delta) < (0.1 ** decimals):
            delta = 0.0
        direction = 'up' if delta > 0 else 'down' if delta < 0 else 'flat'
        if previous_value:
            delta_pct = round((delta / previous_value) * 100, 1)
        else:
            delta_pct = None

        if direction == 'flat':
            status = 'neutral'
        elif positive_direction == 'down':
            status = 'positive' if direction == 'down' else 'negative'
        else:
            status = 'positive' if direction == 'up' else 'negative'

    return {
        'label': label,
        'current': current_value,
        'previous': previous_value,
        'delta': delta,
        'delta_pct': delta_pct,
        'direction': direction,
        'status': status,
        'has_previous': previous_value is not None,
    }


def build_period_trend_snapshot(period_rows):
    """Build cards, summary and time series from operational period rows."""
    rows = [dict(row) for row in (period_rows or [])]
    rows.sort(key=lambda item: item.get('periodo') or '')

    series = []
    max_total = max((_to_float(row.get('total_recaudado')) for row in rows), default=1.0) or 1.0
    max_pending = max((_to_float(row.get('saldo_por_colocar')) for row in rows), default=1.0) or 1.0

    for row in rows:
        total_socios = _to_float(row.get('total_socios'))
        total_recaudado = _to_float(row.get('total_recaudado'))
        total_colocado = _to_float(row.get('total_colocado'))
        saldo_por_colocar = _to_float(row.get('saldo_por_colocar'))
        colocacion_pct = round((total_colocado / total_recaudado) * 100, 1) if total_recaudado > 0 else 0.0
        promedio_socio = round((total_recaudado / total_socios), 1) if total_socios > 0 else 0.0
        series.append(
            {
                'periodo': row.get('periodo'),
                'estado': row.get('estado') or 'Sin estado',
                'total_recaudado': round(total_recaudado, 1),
                'total_colocado': round(total_colocado, 1),
                'saldo_por_colocar': round(saldo_por_colocar, 1),
                'promedio_socio': promedio_socio,
                'colocacion_pct': colocacion_pct,
                'recaudado_bar_pct': round((total_recaudado / max_total) * 100, 1) if max_total > 0 else 0.0,
                'colocado_bar_pct': round((total_colocado / max_total) * 100, 1) if max_total > 0 else 0.0,
                'pendiente_bar_pct': round((saldo_por_colocar / max_pending) * 100, 1) if max_pending > 0 else 0.0,
            }
        )

    latest = series[-1] if series else None
    previous = series[-2] if len(series) > 1 else None
    best_total = max(series, key=lambda item: item['total_recaudado']) if series else None
    best_placement = max(series, key=lambda item: item['colocacion_pct']) if series else None
    highest_average = max(series, key=lambda item: item['promedio_socio']) if series else None

    cards = [
        build_variation(
            'Fondo mensual',
            latest['total_recaudado'] if latest else 0,
            previous['total_recaudado'] if previous else None,
            positive_direction='up',
        ),
        build_variation(
            'Ya colocado',
            latest['total_colocado'] if latest else 0,
            previous['total_colocado'] if previous else None,
            positive_direction='up',
        ),
        build_variation(
            'Saldo por colocar',
            latest['saldo_por_colocar'] if latest else 0,
            previous['saldo_por_colocar'] if previous else None,
            positive_direction='down',
        ),
        build_variation(
            'Promedio por socio',
            latest['promedio_socio'] if latest else 0,
            previous['promedio_socio'] if previous else None,
            positive_direction='up',
        ),
    ]

    summary = {
        'periodos_analizados': len(series),
        'promedio_fondo': round(sum(item['total_recaudado'] for item in series) / len(series), 1) if series else 0.0,
        'promedio_colocacion_pct': round(sum(item['colocacion_pct'] for item in series) / len(series), 1) if series else 0.0,
        'mejor_periodo_fondo': best_total,
        'mejor_periodo_colocacion': best_placement,
        'periodo_promedio_alto': highest_average,
        'periodo_actual': latest['periodo'] if latest else None,
        'periodo_anterior': previous['periodo'] if previous else None,
    }

    return {
        'cards': cards,
        'series': list(reversed(series)),
        'summary': summary,
    }


def _build_svg_line_path(values, width=420, height=180, padding=18, max_value=None):
    """Build a simple SVG polyline path for dashboard trend charts."""
    points = [_to_float(value) for value in (values or [])]
    if not points:
        return ''

    graph_width = max(width - (padding * 2), 1)
    graph_height = max(height - (padding * 2), 1)
    ceiling = _to_float(max_value, max(points, default=1.0) or 1.0) or 1.0

    coordinates = []
    if len(points) == 1:
        x = padding + (graph_width / 2)
        y = height - padding - ((points[0] / ceiling) * graph_height)
        coordinates.append((x, y))
    else:
        step = graph_width / max(len(points) - 1, 1)
        for index, value in enumerate(points):
            x = padding + (step * index)
            y = height - padding - ((_to_float(value) / ceiling) * graph_height)
            coordinates.append((x, y))

    return ' '.join(
        ('M' if index == 0 else 'L') + f' {round(x, 2)} {round(y, 2)}'
        for index, (x, y) in enumerate(coordinates)
    )


def _build_svg_area_path(values, width=420, height=180, padding=18, max_value=None):
    """Build an SVG area path under the line for soft dashboard charts."""
    points = [_to_float(value) for value in (values or [])]
    line_path = _build_svg_line_path(points, width=width, height=height, padding=padding, max_value=max_value)
    if not line_path:
        return ''

    graph_width = max(width - (padding * 2), 1)
    if len(points) == 1:
        first_x = last_x = padding + (graph_width / 2)
    else:
        step = graph_width / max(len(points) - 1, 1)
        first_x = padding
        last_x = padding + (step * (len(points) - 1))

    base_y = height - padding
    return f"{line_path} L {round(last_x, 2)} {base_y} L {round(first_x, 2)} {base_y} Z"


def build_dashboard_visuals(period_series, top_balances, period_summary, placement_counts):
    """Build chart-ready structures for the executive dashboard."""
    series = sorted((dict(row) for row in (period_series or [])), key=lambda item: item.get('periodo') or '')
    trend_values = [
        max(
            _to_float(row.get('total_recaudado')),
            _to_float(row.get('total_colocado')),
            _to_float(row.get('saldo_por_colocar')),
        )
        for row in series
    ]
    max_trend_value = max(trend_values, default=1.0) or 1.0

    labels = [row.get('periodo') or '-' for row in series]
    recaudado_values = [_to_float(row.get('total_recaudado')) for row in series]
    colocado_values = [_to_float(row.get('total_colocado')) for row in series]
    pendiente_values = [_to_float(row.get('saldo_por_colocar')) for row in series]

    total_periodo = _to_float(period_summary.get('total_recaudado'))
    total_cuotas = _to_float(period_summary.get('total_prestamos'))
    total_aportes = _to_float(period_summary.get('total_aportes'))
    total_colocado = _to_float(period_summary.get('total_colocado'))
    total_pendiente = _to_float(period_summary.get('saldo_por_colocar'))
    denominator = total_periodo or 1.0

    composition = [
        {
            'label': 'Cuotas del mes',
            'value': round(total_cuotas, 1),
            'pct': round((total_cuotas / denominator) * 100, 1) if total_periodo > 0 else 0.0,
            'tone': 'primary',
        },
        {
            'label': 'Aportes fijos',
            'value': round(total_aportes, 1),
            'pct': round((total_aportes / denominator) * 100, 1) if total_periodo > 0 else 0.0,
            'tone': 'neutral',
        },
        {
            'label': 'Ya colocado',
            'value': round(total_colocado, 1),
            'pct': round((total_colocado / denominator) * 100, 1) if total_periodo > 0 else 0.0,
            'tone': 'positive',
        },
        {
            'label': 'Saldo por colocar',
            'value': round(total_pendiente, 1),
            'pct': round((total_pendiente / denominator) * 100, 1) if total_periodo > 0 else 0.0,
            'tone': 'warning',
        },
    ]

    origin_values = [
        ('Excel', _to_float((placement_counts or {}).get('excel'))),
        ('App aprobada', _to_float((placement_counts or {}).get('app_aprobado'))),
        ('App reservada', _to_float((placement_counts or {}).get('app_reservado'))),
    ]
    origin_max = max((value for _, value in origin_values), default=1.0) or 1.0
    origins = [
        {
            'label': label,
            'value': int(value),
            'pct': round((value / origin_max) * 100, 1) if origin_max > 0 else 0.0,
        }
        for label, value in origin_values
    ]

    balances = [dict(row) for row in (top_balances or [])]
    max_balance = max((_to_float(row.get('saldo_actual')) for row in balances), default=1.0) or 1.0
    top_balance_bars = [
        {
            'label': f"{row.get('numero')} - {row.get('nombre')}",
            'value': round(_to_float(row.get('saldo_actual')), 1),
            'pct': round((_to_float(row.get('saldo_actual')) / max_balance) * 100, 1) if max_balance > 0 else 0.0,
        }
        for row in balances
    ]

    return {
        'timeline': {
            'labels': labels,
            'width': 420,
            'height': 180,
            'recaudado_path': _build_svg_line_path(recaudado_values, max_value=max_trend_value),
            'recaudado_area': _build_svg_area_path(recaudado_values, max_value=max_trend_value),
            'colocado_path': _build_svg_line_path(colocado_values, max_value=max_trend_value),
            'pendiente_path': _build_svg_line_path(pendiente_values, max_value=max_trend_value),
            'has_data': bool(series),
        },
        'composition': composition,
        'origins': origins,
        'top_balance_bars': top_balance_bars,
    }
