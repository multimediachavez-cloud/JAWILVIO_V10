"""REST API routes for the operational modules already refactored."""

from flask import request, session

from app.services.association_service import (
    build_caja_payload,
    create_permiso,
    create_socio,
    delete_permiso,
    delete_socio,
    get_permiso,
    get_reunion,
    get_socio,
    list_permisos,
    list_reuniones,
    list_socios,
    update_permiso,
    update_socio,
    upsert_reunion,
)
from app.utils.validation import safe_int
from app.routes import (
    api_error,
    api_request_data,
    api_response,
    bp,
    cleanup_legacy_auto_reunion_assignment,
    ensure_monthly_collections,
    get_default_period,
    get_period_placement_status,
    login_required,
    log_action,
)


@bp.route('/api/socios', methods=['GET', 'POST'])
@login_required
def api_socios_collection():
    from app.routes import connect_db

    with connect_db() as conn:
        periodo = request.args.get('periodo', '').strip() or get_default_period(conn)
        ensure_monthly_collections(conn, periodo)

        if request.method == 'GET':
            q = request.args.get('q', '').strip()
            items = list_socios(conn, periodo, q)
            return api_response({'ok': True, 'periodo': periodo, 'count': len(items), 'items': items})

        numero, error = create_socio(conn, api_request_data(), request.files.get('foto_archivo'))
        if error:
            return api_error(error, status=409 if 'Ya existe' in error else 400)
        conn.commit()
        item = get_socio(conn, numero, periodo)
        log_action(session.get('username', 'api'), 'API socios', f"Socio creado por API: {numero} - {item['nombre']}")
        return api_response({'ok': True, 'item': item}, status=201)


@bp.route('/api/socios/<int:numero>', methods=['GET', 'PUT', 'PATCH', 'DELETE'])
@login_required
def api_socio_detail(numero):
    from app.routes import connect_db

    with connect_db() as conn:
        periodo = request.args.get('periodo', '').strip() or get_default_period(conn)
        ensure_monthly_collections(conn, periodo)
        item = get_socio(conn, numero, periodo)
        if not item:
            return api_error('No se encontró el socio solicitado.', status=404)

        if request.method == 'GET':
            return api_response({'ok': True, 'periodo': periodo, 'item': item})

        if request.method == 'DELETE':
            deleted, error = delete_socio(conn, numero)
            if error:
                return api_error(error, status=404)
            conn.commit()
            log_action(session.get('username', 'api'), 'API socios', f"Socio eliminado por API: {deleted['numero']} - {deleted['nombre']}")
            return api_response({'ok': True, 'message': 'Socio eliminado correctamente.', 'item': deleted})

        _, error = update_socio(conn, numero, api_request_data(), request.files.get('foto_archivo'))
        if error:
            return api_error(error, status=400)
        conn.commit()
        item = get_socio(conn, numero, periodo)
        log_action(session.get('username', 'api'), 'API socios', f"Socio actualizado por API: {numero} - {item['nombre']}")
        return api_response({'ok': True, 'item': item})


@bp.route('/api/reuniones', methods=['GET', 'POST'])
@login_required
def api_reuniones_collection():
    from app.routes import connect_db

    with connect_db() as conn:
        if request.method == 'GET':
            periodo = request.args.get('periodo', '').strip()
            if periodo:
                ensure_monthly_collections(conn, periodo)
                cleanup_legacy_auto_reunion_assignment(conn, periodo)
                item = get_reunion(conn, periodo)
                return api_response({'ok': True, 'periodo': periodo, 'item': item})
            items = list_reuniones(conn)
            return api_response({'ok': True, 'count': len(items), 'items': items})

        data = api_request_data()
        periodo = (data.get('periodo') or '').strip() or get_default_period(conn)
        ensure_monthly_collections(conn, periodo)
        cleanup_legacy_auto_reunion_assignment(conn, periodo)
        item, error = upsert_reunion(conn, periodo, data, session.get('username', 'api'))
        if error:
            return api_error(error, status=400)
        conn.commit()
        log_action(session.get('username', 'api'), 'API reuniones', f'Período {periodo}: reunión actualizada por API.')
        return api_response({'ok': True, 'item': item}, status=201)


@bp.route('/api/reuniones/<periodo>', methods=['GET', 'PUT', 'PATCH', 'DELETE'])
@login_required
def api_reunion_detail(periodo):
    from app.routes import connect_db

    with connect_db() as conn:
        ensure_monthly_collections(conn, periodo)
        cleanup_legacy_auto_reunion_assignment(conn, periodo)
        item = get_reunion(conn, periodo)
        if request.method == 'GET':
            if not item:
                return api_error('No se encontró la reunión del período solicitado.', status=404)
            return api_response({'ok': True, 'item': item})
        if request.method == 'DELETE':
            if not item:
                return api_error('No se encontró la reunión del período solicitado.', status=404)
            conn.execute("DELETE FROM reuniones_mensuales WHERE periodo=?", (periodo,))
            conn.commit()
            log_action(session.get('username', 'api'), 'API reuniones', f'Período {periodo}: reunión eliminada por API.')
            return api_response({'ok': True, 'message': 'Reunión eliminada correctamente.'})
        item, error = upsert_reunion(conn, periodo, api_request_data(), session.get('username', 'api'))
        if error:
            return api_error(error, status=400)
        conn.commit()
        log_action(session.get('username', 'api'), 'API reuniones', f'Período {periodo}: reunión editada por API.')
        return api_response({'ok': True, 'item': item})


@bp.route('/api/permisos', methods=['GET', 'POST'])
@login_required
def api_permisos_collection():
    from app.routes import connect_db

    with connect_db() as conn:
        if request.method == 'GET':
            periodo = request.args.get('periodo', '').strip() or None
            socio_numero = safe_int(request.args.get('socio_numero'))
            items = list_permisos(conn, periodo=periodo, socio_numero=socio_numero)
            return api_response({'ok': True, 'count': len(items), 'items': items})

        data = api_request_data()
        periodo = (data.get('periodo') or '').strip() or get_default_period(conn)
        ensure_monthly_collections(conn, periodo)
        item, error = create_permiso(conn, periodo, data, session.get('username', 'api'), request.files.get('documento_permiso'))
        if error:
            return api_error(error, status=400)
        conn.commit()
        log_action(session.get('username', 'api'), 'API permisos', f"Período {periodo}: permiso registrado por API #{item['id']}.")
        return api_response({'ok': True, 'item': item}, status=201)


@bp.route('/api/permisos/<int:permiso_id>', methods=['GET', 'PUT', 'PATCH', 'DELETE'])
@login_required
def api_permiso_detail(permiso_id):
    from app.routes import connect_db

    with connect_db() as conn:
        item = get_permiso(conn, permiso_id)
        if not item:
            return api_error('No se encontró el permiso solicitado.', status=404)
        if request.method == 'GET':
            return api_response({'ok': True, 'item': item})
        if request.method == 'DELETE':
            deleted, error = delete_permiso(conn, permiso_id)
            if error:
                return api_error(error, status=404)
            conn.commit()
            log_action(session.get('username', 'api'), 'API permisos', f"Permiso eliminado por API #{permiso_id}.")
            return api_response({'ok': True, 'message': 'Permiso eliminado correctamente.', 'item': deleted})
        item, error = update_permiso(conn, permiso_id, api_request_data(), request.files.get('documento_permiso'))
        if error:
            return api_error(error, status=400)
        conn.commit()
        log_action(session.get('username', 'api'), 'API permisos', f"Permiso actualizado por API #{permiso_id}.")
        return api_response({'ok': True, 'item': item})


@bp.route('/api/caja', methods=['GET'])
@login_required
def api_caja_collection():
    from app.routes import connect_db

    with connect_db() as conn:
        periodo = request.args.get('periodo', '').strip() or get_default_period(conn)
        item = build_caja_payload(conn, periodo, False, ensure_monthly_collections, get_period_placement_status)
        if not item:
            return api_error('No se encontró información de caja para el período solicitado.', status=404)
        item['periodos_disponibles'] = conn.execute("SELECT periodo FROM periodos ORDER BY periodo DESC").fetchall()
        item['periodos_disponibles'] = [row['periodo'] for row in item['periodos_disponibles']]
        return api_response({'ok': True, 'item': item})


@bp.route('/api/caja/<periodo>', methods=['GET'])
@login_required
def api_caja_detail(periodo):
    from app.routes import connect_db

    with connect_db() as conn:
        item = build_caja_payload(conn, periodo, False, ensure_monthly_collections, get_period_placement_status)
        if not item:
            return api_error('No se encontró información de caja para el período solicitado.', status=404)
        return api_response({'ok': True, 'item': item})


@bp.route('/api/caja/<periodo>/detalle', methods=['GET'])
@login_required
def api_caja_detail_items(periodo):
    from app.routes import connect_db

    with connect_db() as conn:
        item = build_caja_payload(conn, periodo, True, ensure_monthly_collections, get_period_placement_status)
        if not item:
            return api_error('No se encontró información de caja para el período solicitado.', status=404)
        return api_response({
            'ok': True,
            'periodo': periodo,
            'count': len(item.get('items', [])),
            'resumen': item['resumen'],
            'items': item.get('items', []),
        })
