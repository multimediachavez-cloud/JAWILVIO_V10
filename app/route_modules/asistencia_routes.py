"""Routes for attendance capture and correction."""

from datetime import datetime

from flask import flash, redirect, render_template, request, session, url_for

from app.services.association_service import (
    create_attendance,
    delete_attendance,
    get_attendance,
    list_recent_attendance,
    update_attendance,
)
from app.routes import (
    bp,
    ensure_period_is_writable,
    login_required,
    log_action,
    normalize_attendance_state,
    sync_attendance_fines_for_period,
)


@bp.route('/asistencia', methods=['GET', 'POST'])
@login_required
def asistencia():
    """Register attendance and let admins fix the latest movements."""
    from app.routes import connect_db

    with connect_db() as conn:
        if request.method == 'POST':
            action = request.form.get('action', '').strip() or 'create'

            if action == 'create':
                target_period = str(request.form['fecha'])[:7]
                is_writable, message, _ = ensure_period_is_writable(conn, target_period)
                if not is_writable:
                    flash(message)
                    return redirect(url_for('main.asistencia'))

                payload, error = create_attendance(
                    conn,
                    int(request.form['socio_numero']),
                    request.form['fecha'],
                    normalize_attendance_state(request.form['estado']),
                    request.form.get('observacion', ''),
                )
                if error:
                    flash(error)
                else:
                    sync_attendance_fines_for_period(conn, payload['fecha'][:7])
                    conn.commit()
                    log_action(
                        session.get('username', 'sistema'),
                        'Registro de asistencia',
                        f"Socio {payload['socio_numero']} - {payload['estado']}",
                        categoria='operacion',
                        modulo='asistencia',
                        entidad='asistencia',
                        entidad_id=payload['id'],
                        periodo=payload['fecha'][:7],
                        despues=payload,
                    )
                    flash('Asistencia registrada correctamente.')
                return redirect(url_for('main.asistencia'))

            record_id = request.form.get('registro_id', '').strip()
            attendance_record = get_attendance(conn, int(record_id)) if record_id.isdigit() else None
            if not attendance_record:
                flash('No se encontró el registro de asistencia solicitado.')
                return redirect(url_for('main.asistencia'))
            if session.get('role') != 'Administrador':
                flash('Solo el administrador puede editar o eliminar los últimos movimientos.')
                return redirect(url_for('main.asistencia'))

            if action == 'update':
                target_period = str(request.form['fecha'])[:7]
                for period in {str(attendance_record['fecha'])[:7], target_period}:
                    if not period or len(period) != 7:
                        continue
                    is_writable, message, _ = ensure_period_is_writable(conn, period)
                    if not is_writable:
                        flash(message)
                        return redirect(url_for('main.asistencia'))

                result, error = update_attendance(
                    conn,
                    attendance_record['id'],
                    int(request.form['socio_numero']),
                    request.form['fecha'],
                    normalize_attendance_state(request.form['estado']),
                    request.form.get('observacion', ''),
                )
                if error:
                    flash(error)
                else:
                    for period in {str(result['anterior']['fecha'])[:7], str(result['actual']['fecha'])[:7]}:
                        if period and len(period) == 7:
                            sync_attendance_fines_for_period(conn, period)
                    conn.commit()
                    log_action(
                        session.get('username', 'sistema'),
                        'Edición de asistencia',
                        f"Registro #{attendance_record['id']} actualizado para socio {result['actual']['socio_numero']}.",
                        categoria='operacion',
                        modulo='asistencia',
                        entidad='asistencia',
                        entidad_id=attendance_record['id'],
                        periodo=str(result['actual']['fecha'])[:7],
                        antes=result['anterior'],
                        despues=result['actual'],
                    )
                    flash('Movimiento de asistencia actualizado correctamente.')
                return redirect(url_for('main.asistencia'))

            if action == 'delete':
                target_period = str(attendance_record['fecha'])[:7]
                is_writable, message, _ = ensure_period_is_writable(conn, target_period)
                if not is_writable:
                    flash(message)
                    return redirect(url_for('main.asistencia'))

                payload, error = delete_attendance(conn, attendance_record['id'])
                if error:
                    flash(error)
                else:
                    period = str(payload['fecha'])[:7]
                    if period and len(period) == 7:
                        sync_attendance_fines_for_period(conn, period)
                    conn.commit()
                    log_action(
                        session.get('username', 'sistema'),
                        'Eliminación de asistencia',
                        f"Registro #{attendance_record['id']} eliminado.",
                        categoria='operacion',
                        modulo='asistencia',
                        entidad='asistencia',
                        entidad_id=attendance_record['id'],
                        periodo=period,
                        antes=payload,
                    )
                    flash('Movimiento de asistencia eliminado correctamente.')
                return redirect(url_for('main.asistencia'))

            return redirect(url_for('main.asistencia'))

        socios = conn.execute("SELECT numero, nombre FROM socios ORDER BY numero").fetchall()
        recent_records = list_recent_attendance(conn, 50)
        edit_id = request.args.get('edit', '').strip()
        edit_registro = get_attendance(conn, int(edit_id)) if edit_id.isdigit() else None

    return render_template(
        'asistencia.html',
        socios=socios,
        registros=recent_records,
        hoy=datetime.now().strftime('%Y-%m-%d'),
        edit_registro=edit_registro,
        puede_administrar_movimientos=(session.get('role') == 'Administrador'),
    )
