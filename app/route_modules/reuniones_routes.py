"""Routes for monthly meetings and permission requests."""

from flask import flash, redirect, render_template, request, session, url_for

from app.services.association_service import create_permiso, delete_permiso, get_reunion, list_permisos, upsert_reunion
from app.utils.uploads import build_static_upload_url
from app.routes import (
    PERMISO_MOTIVOS,
    REUNION_TIPOS_VIA,
    bp,
    cleanup_legacy_auto_reunion_assignment,
    ensure_monthly_collections,
    ensure_period_is_writable,
    get_default_period,
    login_required,
    log_action,
    role_can_access_endpoint,
)


@bp.route('/reuniones', methods=['GET', 'POST'])
@login_required
def reuniones():
    """Manage the meeting owner and period permissions in one workflow."""
    from app.routes import connect_db

    with connect_db() as conn:
        selected_period = request.args.get('periodo', '').strip()
        if not selected_period:
            selected_period = get_default_period(conn)
            return redirect(url_for('main.reuniones', periodo=selected_period))

        ensure_monthly_collections(conn, selected_period)
        cleanup_legacy_auto_reunion_assignment(conn, selected_period)

        if request.method == 'POST':
            if not role_can_access_endpoint(session.get('role'), 'main.reuniones', write=True):
                flash('Tu rol no tiene permiso para realizar esta acción.')
                return redirect(url_for('main.reuniones', periodo=selected_period))

            is_writable, message, _ = ensure_period_is_writable(conn, selected_period)
            if not is_writable:
                flash(message)
                return redirect(url_for('main.reuniones', periodo=selected_period))

            action = request.form.get('action', '').strip()
            if action == 'guardar_reunion':
                previous_meeting = get_reunion(conn, selected_period)
                reunion, error = upsert_reunion(conn, selected_period, request.form, session.get('username', 'sistema'))
                if error:
                    flash(error)
                else:
                    conn.commit()
                    log_action(
                        session.get('username', 'sistema'),
                        'Control de reuniones',
                        f'Período {selected_period}: reunión actualizada.',
                        categoria='operacion',
                        modulo='reuniones',
                        entidad='reunion_mensual',
                        entidad_id=selected_period,
                        periodo=selected_period,
                        antes=previous_meeting,
                        despues=reunion,
                    )
                    flash('Reunión del período actualizada correctamente.')
                return redirect(url_for('main.reuniones', periodo=selected_period))

            if action == 'registrar_permiso':
                permiso, error = create_permiso(
                    conn,
                    selected_period,
                    request.form,
                    session.get('username', 'sistema'),
                    request.files.get('documento_permiso'),
                )
                if error:
                    flash(error)
                else:
                    conn.commit()
                    log_action(
                        session.get('username', 'sistema'),
                        'Control de permisos',
                        f"Período {selected_period}: permiso registrado #{permiso['id']}.",
                        categoria='operacion',
                        modulo='permisos',
                        entidad='permiso_mensual',
                        entidad_id=permiso['id'],
                        periodo=selected_period,
                        despues=permiso,
                    )
                    flash('Permiso registrado correctamente.')
                return redirect(url_for('main.reuniones', periodo=selected_period))

            if action == 'eliminar_permiso':
                permiso_id = int(request.form.get('permiso_id', '0') or 0)
                permiso, error = delete_permiso(conn, permiso_id)
                if error:
                    flash(error)
                else:
                    conn.commit()
                    log_action(
                        session.get('username', 'sistema'),
                        'Control de permisos',
                        f"Período {selected_period}: permiso eliminado #{permiso['id']}.",
                        categoria='operacion',
                        modulo='permisos',
                        entidad='permiso_mensual',
                        entidad_id=permiso['id'],
                        periodo=selected_period,
                        antes=permiso,
                    )
                    flash('Permiso eliminado correctamente.')
                return redirect(url_for('main.reuniones', periodo=selected_period))

        period_meeting = get_reunion(conn, selected_period)
        socios = conn.execute("SELECT numero, nombre FROM socios ORDER BY numero").fetchall()
        period_rows = conn.execute("SELECT periodo FROM periodos ORDER BY periodo DESC").fetchall()
        registered_permissions = list_permisos(conn, periodo=selected_period)
        permission_summary = conn.execute(
            """
            SELECT socio_numero, socio_nombre, COUNT(*) as total_permisos
            FROM permisos_mensuales
            WHERE periodo=?
            GROUP BY socio_numero, socio_nombre
            ORDER BY total_permisos DESC, socio_numero ASC
            """,
            (selected_period,),
        ).fetchall()
        period_obligations = conn.execute(
            """
            SELECT socio_numero, socio_nombre, total_mes
            FROM obligaciones_mensuales
            WHERE periodo=?
            ORDER BY socio_numero
            """,
            (selected_period,),
        ).fetchall()
        meeting_location = period_meeting['lugar_reunion'] if period_meeting else '-'

    return render_template(
        'reuniones_control.html',
        periodo=selected_period,
        reunion_periodo=period_meeting,
        socios=socios,
        periodos=period_rows,
        permisos_registrados=registered_permissions,
        permisos_resumen=permission_summary,
        obligaciones=period_obligations,
        reunion_tipos_via=REUNION_TIPOS_VIA,
        lugar_reunion=meeting_location,
        permiso_motivos=PERMISO_MOTIVOS,
        build_permiso_document_url=build_static_upload_url,
        puede_editar=role_can_access_endpoint(session.get('role'), 'main.reuniones', write=True),
    )
