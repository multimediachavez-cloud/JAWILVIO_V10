import os
from datetime import datetime

from flask import current_app, url_for
from werkzeug.utils import secure_filename


SOCIO_PHOTO_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp'}
PERMISO_DOCUMENT_EXTENSIONS = {'.pdf', '.doc', '.docx'}
BRANDING_LOGO_EXTENSIONS = {'.svg', '.png', '.jpg', '.jpeg', '.webp'}


def build_static_upload_url(path):
    if not path:
        return None
    if path.startswith('http://') or path.startswith('https://') or path.startswith('/'):
        return path
    return url_for('static', filename=path.replace('\\', '/'))


def save_uploaded_file(file_storage, upload_dir, final_name, allowed_extensions):
    if not file_storage or not file_storage.filename:
        return None
    filename = secure_filename(file_storage.filename)
    if not filename:
        return None
    _, ext = os.path.splitext(filename)
    ext = ext.lower()
    if ext not in allowed_extensions:
        return False
    os.makedirs(upload_dir, exist_ok=True)
    final_path = os.path.join(upload_dir, f'{final_name}{ext}')
    file_storage.save(final_path)
    return final_path


def save_socio_photo(file_storage, socio_numero):
    upload_dir = current_app.config['SOCIOS_PHOTO_UPLOAD_DIR']
    final_name = f"socio_{socio_numero}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    saved = save_uploaded_file(file_storage, upload_dir, final_name, SOCIO_PHOTO_EXTENSIONS)
    if not saved or saved is False:
        return saved
    return f"uploads/socios/{os.path.basename(saved)}"


def save_permiso_document(file_storage, periodo, socio_numero):
    upload_dir = current_app.config['PERMISOS_UPLOAD_DIR']
    final_name = f"permiso_{periodo}_{socio_numero}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    saved = save_uploaded_file(file_storage, upload_dir, final_name, PERMISO_DOCUMENT_EXTENSIONS)
    if not saved or saved is False:
        return saved
    return f"uploads/permisos/{os.path.basename(saved)}"


def save_branding_logo(file_storage):
    upload_dir = current_app.config['BRANDING_UPLOAD_DIR']
    final_name = f"logo_institucional_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    saved = save_uploaded_file(file_storage, upload_dir, final_name, BRANDING_LOGO_EXTENSIONS)
    if not saved or saved is False:
        return saved
    return f"uploads/branding/{os.path.basename(saved)}"


def delete_local_upload(path):
    if not path or path.startswith('http://') or path.startswith('https://') or path.startswith('/'):
        return
    if str(path).startswith('img/'):
        return
    full_path = os.path.join(current_app.static_folder, path.replace('/', os.sep))
    if os.path.exists(full_path):
        try:
            os.remove(full_path)
        except OSError:
            pass
