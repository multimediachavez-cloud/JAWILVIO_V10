"""Business services for the refactored association modules."""

from app.repositories import association_repository as repo
from app.utils.uploads import build_static_upload_url, delete_local_upload, save_permiso_document, save_socio_photo
from app.utils.validation import safe_float, safe_int


PERMISO_MOTIVOS = ['Personal', 'Familiar', 'Salud', 'Trabajo', 'Otros']


def list_socios(conn, periodo, query=''):
    """Return member rows already serialized for the web and API layers."""
    return [serialize_socio(item) for item in repo.list_socios(conn, periodo, query)]


def get_socio(conn, numero, periodo):
    member = repo.get_socio(conn, numero, periodo)
    return serialize_socio(member) if member else None


def create_socio(conn, data, foto_file=None):
    member_number = safe_int(data.get('numero'))
    member_name = (data.get('nombre') or '').strip()
    dni = (data.get('dni') or '').strip() or None
    installments_raw = data.get('meses')
    installments = safe_int(installments_raw)
    loan_date = (data.get('fecha_prestamo') or '').strip() or None
    balance_raw = data.get('saldo')
    balance = safe_float(balance_raw, 0.0 if balance_raw in (None, '') else None)
    month_marker = (data.get('mes_2026') or '').strip() or None

    if member_number is None or not member_name:
        return None, 'Completa al menos número y nombre del socio.'
    if installments_raw not in (None, '') and installments is None:
        return None, 'Ingresa un valor válido para cuotas o meses.'
    if balance is None:
        return None, 'Ingresa un saldo válido.'
    if repo.get_socio_raw(conn, member_number):
        return None, 'Ya existe un socio con ese número.'

    photo_path = save_socio_photo(foto_file, member_number)
    if photo_path is False:
        return None, 'La foto debe ser JPG, JPEG, PNG o WEBP.'

    repo.upsert_socio(conn, member_number, member_name, dni, photo_path, installments, loan_date, balance, month_marker)
    return member_number, None


def update_socio(conn, numero, data, foto_file=None):
    current_member = repo.get_socio_raw(conn, numero)
    if not current_member:
        return None, 'No se encontró el socio solicitado.'

    member_name = (data.get('nombre') if 'nombre' in data else current_member['nombre'] or '').strip()
    dni = (data.get('dni') if 'dni' in data else current_member['dni']) or None
    installments_raw = data.get('meses') if 'meses' in data else current_member['meses']
    installments = safe_int(installments_raw)
    loan_date = (data.get('fecha_prestamo') if 'fecha_prestamo' in data else current_member['fecha_prestamo']) or None
    balance_raw = data.get('saldo') if 'saldo' in data else current_member['saldo']
    balance = safe_float(balance_raw, None)
    month_marker = (data.get('mes_2026') if 'mes_2026' in data else current_member['mes_2026']) or None

    if not member_name:
        return None, 'El nombre del socio es obligatorio.'
    if installments_raw not in (None, '') and installments is None:
        return None, 'Ingresa un valor válido para cuotas o meses.'
    if balance is None:
        return None, 'Ingresa un saldo válido.'

    photo_path = current_member['foto']
    new_photo_path = save_socio_photo(foto_file, numero)
    if new_photo_path is False:
        return None, 'La foto debe ser JPG, JPEG, PNG o WEBP.'
    if new_photo_path:
        delete_local_upload(photo_path)
        photo_path = new_photo_path

    repo.upsert_socio(conn, numero, member_name, dni, photo_path, installments, loan_date, balance, month_marker)
    return numero, None


def delete_socio(conn, numero):
    member = repo.delete_socio_related(conn, numero)
    if not member:
        return None, 'No se encontró el socio solicitado.'
    delete_local_upload(member['foto'])
    return {'numero': member['numero'], 'nombre': member['nombre']}, None


def upsert_reunion(conn, periodo, data, usuario):
    member_number = safe_int(data.get('socio_numero'))
    member = repo.get_socio_raw(conn, member_number)
    if not member:
        return None, 'Selecciona un socio válido para la reunión del período.'

    repo.save_reunion(
        conn,
        periodo,
        member_number,
        member['nombre'],
        (data.get('estado') or 'Pendiente').strip() or 'Pendiente',
        (data.get('fecha_programada') or '').strip() or f'{periodo}-01',
        (data.get('fecha_realizada') or '').strip() or None,
        (data.get('tipo_via') or '').strip() or None,
        (data.get('direccion_reunion') or '').strip() or None,
        (data.get('observacion') or '').strip(),
        usuario or 'sistema',
    )
    meeting = repo.get_reunion(conn, periodo)
    return serialize_reunion(meeting), None


def delete_reunion(conn, periodo):
    meeting = repo.get_reunion(conn, periodo)
    if not meeting:
        return None, 'No se encontró la reunión del período solicitado.'
    repo.delete_reunion(conn, periodo)
    return meeting.to_dict(), None


def list_reuniones(conn):
    return [serialize_reunion(item) for item in repo.list_reuniones(conn)]


def get_reunion(conn, periodo):
    meeting = repo.get_reunion(conn, periodo)
    return serialize_reunion(meeting) if meeting else None


def list_permisos(conn, periodo=None, socio_numero=None):
    return [serialize_permiso(item) for item in repo.list_permisos(conn, periodo, socio_numero)]


def get_permiso(conn, permiso_id):
    permiso = repo.get_permiso(conn, permiso_id)
    return serialize_permiso(permiso) if permiso else None


def create_permiso(conn, periodo, data, usuario, documento_file=None):
    member_number = safe_int(data.get('socio_numero'))
    member = repo.get_socio_raw(conn, member_number)
    reason = (data.get('motivo') or '').strip() or 'Otros'
    if not member:
        return None, 'Selecciona un socio válido para registrar el permiso.'
    if reason not in PERMISO_MOTIVOS:
        return None, 'Selecciona un motivo válido para el permiso.'

    document_path = save_permiso_document(documento_file, periodo, member_number)
    if document_path is False:
        return None, 'El documento del permiso debe ser PDF, DOC o DOCX.'

    permiso = repo.insert_permiso(
        conn,
        periodo,
        member_number,
        member['nombre'],
        (data.get('fecha_permiso') or '').strip() or f'{periodo}-01',
        reason,
        document_path,
        (data.get('observacion') or '').strip(),
        usuario or 'sistema',
    )
    return serialize_permiso(permiso), None


def update_permiso(conn, permiso_id, data, documento_file=None):
    current_permission = repo.get_permiso(conn, permiso_id)
    if not current_permission:
        return None, 'No se encontró el permiso solicitado.'

    payload = current_permission.to_dict()
    reason = (data.get('motivo') or payload['motivo'] or 'Otros').strip() or 'Otros'
    if reason not in PERMISO_MOTIVOS:
        return None, 'Selecciona un motivo válido para el permiso.'

    document_path = payload['documento']
    updated_document = save_permiso_document(documento_file, payload['periodo'], payload['socio_numero'])
    if updated_document is False:
        return None, 'El documento del permiso debe ser PDF, DOC o DOCX.'
    if updated_document:
        delete_local_upload(document_path)
        document_path = updated_document

    repo.update_permiso(
        conn,
        permiso_id,
        (data.get('fecha_permiso') or payload['fecha_permiso']),
        reason,
        document_path,
        (data.get('observacion') if 'observacion' in data else payload['observacion']) or '',
    )
    return get_permiso(conn, permiso_id), None


def delete_permiso(conn, permiso_id):
    current_permission = repo.get_permiso(conn, permiso_id)
    if not current_permission:
        return None, 'No se encontró el permiso solicitado.'

    payload = serialize_permiso(current_permission)
    delete_local_upload(payload['documento'])
    repo.delete_permiso(conn, permiso_id)
    return payload, None


def list_recent_attendance(conn, limit=50):
    return [item.to_dict() for item in repo.list_recent_attendance(conn, limit)]


def get_attendance(conn, registro_id):
    attendance = repo.get_attendance_record(conn, registro_id)
    return attendance.to_dict() if attendance else None


def create_attendance(conn, socio_numero, fecha, estado, observacion):
    member = repo.get_socio_raw(conn, socio_numero)
    if not member:
        return None, 'Selecciona un socio válido.'
    attendance = repo.insert_attendance(conn, socio_numero, member['nombre'], fecha, estado, observacion)
    return attendance.to_dict(), None


def update_attendance(conn, registro_id, socio_numero, fecha, estado, observacion):
    current_attendance = repo.get_attendance_record(conn, registro_id)
    if not current_attendance:
        return None, 'No se encontró el registro de asistencia solicitado.'

    member = repo.get_socio_raw(conn, socio_numero)
    if not member:
        return None, 'Selecciona un socio válido.'

    updated_attendance = repo.update_attendance(conn, registro_id, socio_numero, member['nombre'], fecha, estado, observacion)
    return {'anterior': current_attendance.to_dict(), 'actual': updated_attendance.to_dict()}, None


def delete_attendance(conn, registro_id):
    current_attendance = repo.get_attendance_record(conn, registro_id)
    if not current_attendance:
        return None, 'No se encontró el registro de asistencia solicitado.'
    repo.delete_attendance(conn, registro_id)
    return current_attendance.to_dict(), None


def build_caja_payload(conn, periodo, include_items, ensure_monthly_fn, placement_status_fn):
    """Build the cash summary payload shared by the web UI and REST API."""
    ensure_monthly_fn(conn, periodo)
    period_row = repo.get_periodo_row(conn, periodo)
    if not period_row:
        return None

    close_row = repo.get_cierre_row(conn, periodo)
    meeting = repo.get_reunion(conn, periodo)
    permission_total = repo.count_permisos(conn, periodo)
    placement = placement_status_fn(conn, periodo, float(period_row['total_recaudado'] or 0))
    payload = {
        'periodo': periodo,
        'estado': period_row['estado'],
        'resumen': {
            'total_socios': int(period_row['total_socios'] or 0),
            'total_prestamos': round(float(period_row['total_prestamos'] or 0), 1),
            'total_aportes': round(float(period_row['total_aportes'] or 0), 1),
            'total_recaudado': round(float(period_row['total_recaudado'] or 0), 1),
            'total_intereses': round(float(period_row['total_intereses'] or 0), 1),
            'total_capital': round(float(period_row['total_capital'] or 0), 1),
            'total_colocado': round(float(period_row['total_colocado'] or 0), 1),
            'saldo_por_colocar': round(float(period_row['saldo_por_colocar'] or 0), 1),
            'aporte_mensual_base': round(float(period_row['aporte_mensual_base'] or 0), 1),
        },
        'colocacion': {
            'fuente': placement['fuente'],
            'total_colocado': round(float(placement['total_colocado'] or 0), 1),
            'saldo_por_colocar': round(float(placement['saldo_por_colocar'] or 0), 1),
            'registros_detectados': len(placement['detalle_rows'] or []),
        },
        'reunion': serialize_reunion(meeting) if meeting else None,
        'permisos_total': int(permission_total or 0),
        'cierre': {
            'total_socios': int(close_row['total_socios'] or 0) if close_row else 0,
            'total_prestamos': round(float(close_row['total_prestamos'] or 0), 1) if close_row else 0,
            'total_aportes': round(float(close_row['total_aportes'] or 0), 1) if close_row else 0,
            'total_recaudado': round(float(close_row['total_recaudado'] or 0), 1) if close_row else 0,
            'total_intereses': round(float(close_row['total_intereses'] or 0), 1) if close_row else 0,
            'total_capital': round(float(close_row['total_capital'] or 0), 1) if close_row else 0,
            'aporte_mensual_base': round(float(close_row['aporte_mensual_base'] or 0), 1) if close_row else 0,
        },
    }
    if include_items:
        payload['items'] = [item.to_dict() for item in repo.list_obligaciones(conn, periodo)]
    return payload


def serialize_socio(item):
    data = item.to_dict()
    data['foto_url'] = build_static_upload_url(item.foto)
    return data


def serialize_reunion(item):
    return item.to_dict()


def serialize_permiso(item):
    data = item.to_dict()
    data['documento_url'] = build_static_upload_url(item.documento)
    return data
