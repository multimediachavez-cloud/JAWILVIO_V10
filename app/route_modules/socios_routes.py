"""Routes for member directory maintenance."""

from flask import flash, redirect, render_template, request, session, url_for

from app.services.association_service import create_socio, delete_socio, get_socio, list_socios, update_socio
from app.routes import bp, ensure_monthly_collections, get_default_period, log_action, login_required


@bp.route('/socios', methods=['GET', 'POST'])
@login_required
def socios():
    """Manage members while keeping the list and editor on one screen."""
    search_query = request.args.get('q', '').strip()
    from app.routes import connect_db

    with connect_db() as conn:
        current_period = get_default_period(conn)
        ensure_monthly_collections(conn, current_period)
        if request.method == 'POST':
            if session.get('role') != 'Administrador':
                flash('Solo el administrador puede realizar esta acción.')
                return redirect(url_for('main.socios', q=search_query))

            action = request.form.get('action', '').strip()
            if action == 'create':
                numero, error = create_socio(conn, request.form, request.files.get('foto_archivo'))
                if error:
                    flash(error)
                    return redirect(url_for('main.socios', q=search_query))
                conn.commit()
                socio = get_socio(conn, numero, current_period)
                log_action(session.get('username', 'admin'), 'Gestión de socios', f"Socio creado: {numero} - {socio['nombre']}")
                flash('Socio creado correctamente.')
                return redirect(url_for('main.socios', q=search_query))

            if action == 'update':
                numero = int(request.form.get('numero', '0') or 0)
                _, error = update_socio(conn, numero, request.form, request.files.get('foto_archivo'))
                if error:
                    flash(error)
                    return redirect(url_for('main.socios', q=search_query, edit=numero))
                conn.commit()
                socio = get_socio(conn, numero, current_period)
                log_action(session.get('username', 'admin'), 'Gestión de socios', f"Socio actualizado: {numero} - {socio['nombre']}")
                flash('Socio actualizado correctamente.')
                return redirect(url_for('main.socios', q=search_query))

            if action == 'delete':
                numero = int(request.form.get('numero', '0') or 0)
                socio, error = delete_socio(conn, numero)
                if error:
                    flash(error)
                    return redirect(url_for('main.socios', q=search_query))
                conn.commit()
                log_action(session.get('username', 'admin'), 'Gestión de socios', f"Socio eliminado: {socio['numero']} - {socio['nombre']}")
                flash('Socio eliminado correctamente.')
                return redirect(url_for('main.socios', q=search_query))

        socios_rows = list_socios(conn, current_period, search_query)
        edit_number_raw = request.args.get('edit', '').strip()
        edit_number = int(edit_number_raw) if edit_number_raw.isdigit() else None
        edit_socio = get_socio(conn, edit_number, current_period) if edit_number is not None else None
    return render_template('socios_gestion.html', socios=socios_rows, q=search_query, edit_socio=edit_socio)
