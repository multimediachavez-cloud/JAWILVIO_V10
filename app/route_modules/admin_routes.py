"""Administrative routes extracted from the legacy monolith."""

import os
from collections import deque
from datetime import datetime

from flask import current_app, flash, redirect, render_template, request, send_file, session, url_for

from app.core.logging_config import log_financial_event, log_system_event
from app.routes import (
    admin_required,
    bp,
    build_branding_logo_url,
    clear_pending_two_factor_session,
    get_config_value,
    log_action,
    login_required,
)
from app.utils.security import hash_password, verify_password
from app.utils.totp import build_totp_uri, generate_totp_secret, verify_totp_code
from app.utils.uploads import delete_local_upload, save_branding_logo


SYSTEM_CONFIGURATION_KEYS = [
    'nombre_asociacion',
    'ubicacion',
    'aporte_mensual',
    'saldo_actual_total_oficial',
    'total_prestamo_acumulado_oficial',
    'multa_inasistencia',
    'multa_tardanza',
]

AVAILABLE_SYSTEM_LOGS = {
    'system': {'filename': 'system.log', 'label': 'Sistema'},
    'user_actions': {'filename': 'user_actions.log', 'label': 'Acciones de usuarios'},
    'finance': {'filename': 'finance.log', 'label': 'Finanzas'},
}


def _read_log_tail(file_path, limit):
    """Read only the last N lines from a log file in a memory-safe way."""
    if not os.path.exists(file_path):
        return []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as log_file:
        return [line.rstrip('\n') for line in deque(log_file, maxlen=limit)]


def _resolve_log_selection():
    """Resolve the selected log key and tail limit from request values."""
    selected_log = request.values.get('log', 'system').strip().lower()
    if selected_log not in AVAILABLE_SYSTEM_LOGS:
        selected_log = 'system'

    try:
        selected_limit = int(request.values.get('limit', '200'))
    except (TypeError, ValueError):
        selected_limit = 200
    selected_limit = max(50, min(selected_limit, 1000))
    return selected_log, selected_limit


def _build_log_summary(selected_log, selected_limit):
    """Build the current log file metadata and tail preview."""
    logs_dir = current_app.config.get('LOGS_DIR') or os.path.join(current_app.instance_path, 'logs')
    log_config = AVAILABLE_SYSTEM_LOGS[selected_log]
    file_path = os.path.join(logs_dir, log_config['filename'])

    lines = _read_log_tail(file_path, selected_limit)
    file_exists = os.path.exists(file_path)
    last_modified = None
    file_size_kb = 0.0
    if file_exists:
        stat_info = os.stat(file_path)
        file_size_kb = round(stat_info.st_size / 1024, 1)
        last_modified = datetime.fromtimestamp(stat_info.st_mtime).strftime('%Y-%m-%d %H:%M:%S')

    summary = {
        'exists': file_exists,
        'line_count': len(lines),
        'last_modified': last_modified or '-',
        'file_size_kb': file_size_kb,
        'filename': log_config['filename'],
        'path': file_path,
    }
    return lines, summary


@bp.route('/usuarios', methods=['GET', 'POST'])
@admin_required
def usuarios():
    """Manage user accounts and security status from the admin console."""
    from app.routes import connect_db

    with connect_db() as conn:
        if request.method == 'POST':
            action = request.form.get('action', '').strip()

            if action == 'create_user':
                username = request.form.get('username', '').strip()
                password = request.form.get('password', '').strip()
                role_name = request.form.get('role', '').strip()
                if not username or not password or not role_name:
                    flash('Completa usuario, contraseña y rol.')
                else:
                    exists = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
                    if exists:
                        flash('Ese nombre de usuario ya existe.')
                    else:
                        conn.execute(
                            "INSERT INTO users(username,password,role,estado) VALUES(?,?,?, 'Activo')",
                            (username, hash_password(password), role_name),
                        )
                        conn.commit()
                        log_action(session.get('username', 'admin'), 'Gestión de usuarios', f'Usuario creado: {username} ({role_name})')
                        flash('Usuario creado correctamente.')
                return redirect(url_for('main.usuarios'))

            if action == 'toggle_status':
                user_id = request.form.get('user_id', '').strip()
                target_user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
                if not target_user:
                    flash('No se encontró el usuario solicitado.')
                elif target_user['id'] == session.get('user_id'):
                    flash('No puedes suspender tu propio usuario desde esta pantalla.')
                else:
                    next_status = 'Activo' if target_user['estado'] == 'Suspendido' else 'Suspendido'
                    conn.execute("UPDATE users SET estado=? WHERE id=?", (next_status, target_user['id']))
                    conn.commit()
                    log_action(session.get('username', 'admin'), 'Gestión de usuarios', f'Estado actualizado: {target_user["username"]} -> {next_status}')
                    flash(f'Estado actualizado: {target_user["username"]} ahora está {next_status.lower()}.')
                return redirect(url_for('main.usuarios'))

            if action == 'reset_password':
                user_id = request.form.get('user_id', '').strip()
                new_password = request.form.get('new_password', '').strip()
                target_user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
                if not target_user:
                    flash('No se encontró el usuario solicitado.')
                elif len(new_password) < 4:
                    flash('La nueva contraseña debe tener al menos 4 caracteres.')
                else:
                    conn.execute("UPDATE users SET password=? WHERE id=?", (hash_password(new_password), target_user['id']))
                    conn.commit()
                    log_action(session.get('username', 'admin'), 'Gestión de usuarios', f'Contraseña restablecida: {target_user["username"]}')
                    flash(f'Contraseña restablecida para {target_user["username"]}.')
                return redirect(url_for('main.usuarios'))

            if action == 'reset_2fa':
                user_id = request.form.get('user_id', '').strip()
                target_user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
                if not target_user:
                    flash('No se encontró el usuario solicitado.')
                else:
                    conn.execute(
                        "UPDATE users SET two_factor_enabled=0, two_factor_secret=NULL WHERE id=?",
                        (target_user['id'],),
                    )
                    conn.commit()
                    log_action(session.get('username', 'admin'), 'Gestión de usuarios', f'2FA restablecido: {target_user["username"]}')
                    flash(f'Se desactivó la autenticación de dos factores para {target_user["username"]}.')
                return redirect(url_for('main.usuarios'))

        users = conn.execute(
            """
            SELECT id, username, role, estado, creado_en, ultimo_acceso, two_factor_enabled
            FROM users
            ORDER BY username
            """
        ).fetchall()
    return render_template('usuarios.html', users=users)


@bp.route('/mi-cuenta', methods=['GET', 'POST'])
@login_required
def mi_cuenta():
    """Allow each user to update their own password and 2FA settings."""
    from app.routes import connect_db

    with connect_db() as conn:
        current_user = conn.execute(
            """
            SELECT id, username, role, estado, creado_en, ultimo_acceso, password, two_factor_enabled, two_factor_secret
            FROM users
            WHERE id=?
            """,
            (session.get('user_id'),),
        ).fetchone()
        if not current_user:
            session.clear()
            flash('No se encontró tu usuario. Inicia sesión nuevamente.')
            return redirect(url_for('main.login'))

        if request.method == 'POST':
            action = request.form.get('action', 'change_password').strip()

            if action == 'change_password':
                current_password = request.form.get('current_password', '').strip()
                new_password = request.form.get('new_password', '').strip()
                confirm_password = request.form.get('confirm_password', '').strip()
                if not verify_password(current_user['password'], current_password):
                    flash('La contraseña actual no coincide.')
                elif len(new_password) < 4:
                    flash('La nueva contraseña debe tener al menos 4 caracteres.')
                elif new_password != confirm_password:
                    flash('La confirmación de la nueva contraseña no coincide.')
                else:
                    conn.execute(
                        "UPDATE users SET password=? WHERE id=?",
                        (hash_password(new_password), current_user['id']),
                    )
                    conn.commit()
                    log_action(session.get('username', 'usuario'), 'Cambio de contraseña', 'Actualizó su contraseña de acceso.')
                    flash('Tu contraseña fue actualizada correctamente.')
                    return redirect(url_for('main.mi_cuenta'))

            if action == 'prepare_2fa':
                session['pending_2fa_setup_secret'] = generate_totp_secret()
                session['pending_2fa_setup_user_id'] = current_user['id']
                flash('Se generó una nueva clave para activar la verificación en dos pasos.')
                return redirect(url_for('main.mi_cuenta'))

            if action == 'enable_2fa':
                setup_secret = session.get('pending_2fa_setup_secret', '').strip()
                pending_user_id = session.get('pending_2fa_setup_user_id')
                verification_code = request.form.get('verification_code', '').strip()
                if not setup_secret or pending_user_id != current_user['id']:
                    flash('Primero genera una clave nueva para activar la verificación en dos pasos.')
                elif not verify_totp_code(setup_secret, verification_code):
                    flash('El código de verificación no es válido. Revisa tu app autenticadora.')
                else:
                    conn.execute(
                        "UPDATE users SET two_factor_enabled=1, two_factor_secret=? WHERE id=?",
                        (setup_secret, current_user['id']),
                    )
                    conn.commit()
                    session.pop('pending_2fa_setup_secret', None)
                    session.pop('pending_2fa_setup_user_id', None)
                    log_action(session.get('username', 'usuario'), 'Activación de 2FA', 'Se habilitó la autenticación de dos factores.')
                    flash('La autenticación de dos factores quedó activada correctamente.')
                    return redirect(url_for('main.mi_cuenta'))

            if action == 'disable_2fa':
                current_password = request.form.get('disable_current_password', '').strip()
                if not verify_password(current_user['password'], current_password):
                    flash('La contraseña actual no coincide. No se pudo desactivar el 2FA.')
                else:
                    conn.execute(
                        "UPDATE users SET two_factor_enabled=0, two_factor_secret=NULL WHERE id=?",
                        (current_user['id'],),
                    )
                    conn.commit()
                    session.pop('pending_2fa_setup_secret', None)
                    session.pop('pending_2fa_setup_user_id', None)
                    clear_pending_two_factor_session()
                    log_action(session.get('username', 'usuario'), 'Desactivación de 2FA', 'Se desactivó la autenticación de dos factores.')
                    flash('La autenticación de dos factores fue desactivada.')
                    return redirect(url_for('main.mi_cuenta'))

        issuer_name = str(get_config_value(conn, 'nombre_asociacion', 'Asociación JAWILVIO') or 'Asociación JAWILVIO')

    pending_setup_secret = ''
    if session.get('pending_2fa_setup_user_id') == current_user['id']:
        pending_setup_secret = session.get('pending_2fa_setup_secret', '').strip()

    active_secret = pending_setup_secret or (current_user['two_factor_secret'] or '').strip()
    provisioning_uri = build_totp_uri(active_secret, current_user['username'], issuer_name) if active_secret else ''

    return render_template(
        'mi_cuenta.html',
        user=current_user,
        pending_setup_secret=pending_setup_secret,
        provisioning_uri=provisioning_uri,
    )


@bp.route('/configuracion', methods=['GET', 'POST'])
@admin_required
def configuracion():
    """Edit the institutional branding and finance defaults in one place."""
    from app.routes import connect_db

    with connect_db() as conn:
        if request.method == 'POST':
            current_config = {row['clave']: row['valor'] for row in conn.execute("SELECT clave, valor FROM configuracion").fetchall()}
            logo_file = request.files.get('logo_institucional')
            uploaded_logo = save_branding_logo(logo_file)
            if uploaded_logo is False:
                flash('El logo debe estar en formato SVG, PNG, JPG, JPEG o WEBP.')
                return redirect(url_for('main.configuracion'))

            saved_logo = current_config.get('logo_institucional', '')
            if request.form.get('restaurar_logo', '').strip() == '1':
                delete_local_upload(saved_logo)
                saved_logo = ''
            elif uploaded_logo:
                delete_local_upload(saved_logo)
                saved_logo = uploaded_logo

            for config_key in SYSTEM_CONFIGURATION_KEYS:
                conn.execute(
                    "INSERT OR REPLACE INTO configuracion(clave, valor) VALUES(?,?)",
                    (config_key, request.form.get(config_key, '').strip()),
                )
            conn.execute(
                "INSERT OR REPLACE INTO configuracion(clave, valor) VALUES(?,?)",
                ('logo_institucional', saved_logo),
            )
            conn.commit()
            log_action(session.get('username', 'admin'), 'Configuración', 'Actualización de parámetros institucionales')
            flash('Configuración guardada correctamente.')
            return redirect(url_for('main.configuracion'))

        config_values = {row['clave']: row['valor'] for row in conn.execute("SELECT clave, valor FROM configuracion").fetchall()}
    return render_template(
        'configuracion.html',
        cfg=config_values,
        logo_actual_url=build_branding_logo_url((config_values.get('logo_institucional') or '').strip()),
        usa_logo_personalizado=bool((config_values.get('logo_institucional') or '').strip()),
    )


@bp.route('/auditoria')
@admin_required
def auditoria():
    """Show the latest audit trail for operational traceability."""
    from app.routes import connect_db

    with connect_db() as conn:
        rows = conn.execute("SELECT * FROM auditoria ORDER BY id DESC LIMIT 200").fetchall()
    return render_template('auditoria.html', rows=rows)


@bp.route('/logs-sistema', methods=['GET', 'POST'])
@admin_required
def logs_sistema():
    """Expose a compact admin console to inspect the latest application logs."""
    selected_log, selected_limit = _resolve_log_selection()
    _, summary = _build_log_summary(selected_log, selected_limit)

    if request.method == 'POST':
        action = request.form.get('action', '').strip()
        file_path = summary['path']
        filename = summary['filename']

        if action == 'download':
            if not summary['exists']:
                flash('El log seleccionado todavía no existe.')
                return redirect(url_for('main.logs_sistema', log=selected_log, limit=selected_limit))
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            return send_file(
                file_path,
                as_attachment=True,
                download_name=f'jawilvio_{selected_log}_{timestamp}.log',
                mimetype='text/plain; charset=utf-8',
            )

        if action == 'clear':
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w', encoding='utf-8'):
                pass

            admin_user = session.get('username', 'admin')
            if selected_log == 'system':
                log_system_event('Log del sistema limpiado manualmente', administrador=admin_user, archivo=filename)
            elif selected_log == 'finance':
                log_financial_event('Log financiero limpiado manualmente', administrador=admin_user, archivo=filename)

            log_action(
                admin_user,
                'Logs del sistema',
                f'Se limpió el archivo {filename}.',
            )
            flash(f'Log limpiado correctamente: {filename}.')
            return redirect(url_for('main.logs_sistema', log=selected_log, limit=selected_limit))

    lines, summary = _build_log_summary(selected_log, selected_limit)

    return render_template(
        'logs_sistema.html',
        available_logs=AVAILABLE_SYSTEM_LOGS,
        selected_log=selected_log,
        selected_limit=selected_limit,
        lines=lines,
        summary=summary,
    )
