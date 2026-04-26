from flask import Flask
import glob
import os
import shutil
import sqlite3
import tempfile
from .core.database import configure_database
from .core.logging_config import configure_logging, log_system_event, register_error_logging
from .db import init_db, import_excel_if_needed, seed_saldos_historicos
from .routes import bp


def _project_base_dir():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def _resolve_instance_path():
    explicit_instance_path = (
        os.getenv('JAWILVIO_INSTANCE_PATH')
        or os.getenv('INSTANCE_PATH')
        or os.getenv('RENDER_DISK_PATH')
    )
    if explicit_instance_path:
        return os.path.abspath(explicit_instance_path)
    return os.path.join(_project_base_dir(), 'instance')


def _bundled_excel_path(app):
    return os.path.abspath(os.path.join(app.root_path, '..', 'data', 'asociacion_jawilvio.xlsx'))


def _resolve_excel_path(app):
    explicit_excel_path = (os.getenv('EXCEL_PATH') or '').strip()
    bundled_excel = _bundled_excel_path(app)
    if explicit_excel_path:
        desired_path = os.path.abspath(explicit_excel_path)
        if os.path.exists(desired_path):
            return desired_path
        if os.path.exists(bundled_excel):
            os.makedirs(os.path.dirname(desired_path), exist_ok=True)
            try:
                shutil.copy2(bundled_excel, desired_path)
                return desired_path
            except OSError:
                return bundled_excel
        return desired_path
    downloads_dir = os.path.join(os.path.expanduser('~'), 'Downloads')
    exact_file = os.path.join(downloads_dir, 'este.xlsx')
    if os.path.exists(exact_file):
        return exact_file
    candidates = glob.glob(os.path.join(downloads_dir, 'PRESTAMOS JAWILVIO*.xlsx'))
    if candidates:
        return max(candidates, key=os.path.getmtime)
    return bundled_excel


def _resolve_database_path(app):
    explicit_database_path = (os.getenv('DATABASE_PATH') or '').strip()
    if explicit_database_path:
        return os.path.abspath(explicit_database_path)
    return os.path.join(app.instance_path, 'jawilvio_v10.db')


def _resolve_uploads_root(app):
    explicit_uploads_path = (os.getenv('JAWILVIO_UPLOADS_PATH') or '').strip()
    if explicit_uploads_path:
        return os.path.abspath(explicit_uploads_path)
    render_disk_path = (os.getenv('RENDER_DISK_PATH') or '').strip()
    if render_disk_path:
        return os.path.join(os.path.abspath(render_disk_path), 'uploads')
    return os.path.join(app.static_folder, 'uploads')


def _bootstrap_database(app):
    primary_db = _resolve_database_path(app)
    fallback_db = os.path.join(tempfile.gettempdir(), 'jawilvio_v10_recovered.db')

    app.config['DATABASE'] = primary_db
    app.config['DATABASE_PRIMARY'] = primary_db
    app.config['DATABASE_FALLBACK'] = fallback_db
    app.config['DATABASE_DSN'] = primary_db

    try:
        init_db(app)
        import_excel_if_needed(app)
        seed_saldos_historicos(app)
        app.config['DATABASE_RECOVERED'] = False
        log_system_event(
            'Base de datos inicializada correctamente',
            database=primary_db,
            excel_path=app.config.get('EXCEL_PATH'),
            recovered=False,
        )
    except sqlite3.OperationalError:
        log_system_event(
            'No se pudo abrir la base principal. Se activará la base de recuperación.',
            level=40,
            database=primary_db,
            fallback_database=fallback_db,
            excel_path=app.config.get('EXCEL_PATH'),
            exc_info=True,
        )
        app.config['DATABASE'] = fallback_db
        app.config['DATABASE_DSN'] = fallback_db
        init_db(app)
        import_excel_if_needed(app)
        seed_saldos_historicos(app)
        app.config['DATABASE_RECOVERED'] = True
        log_system_event(
            'Base de recuperación activada correctamente',
            level=30,
            database=fallback_db,
            excel_path=app.config.get('EXCEL_PATH'),
            recovered=True,
        )


def create_app(test_config=None):
    app = Flask(__name__, instance_path=_resolve_instance_path(), instance_relative_config=False)
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'jawilvio-v10-secret')
    app.config['EXCEL_PATH'] = _resolve_excel_path(app)
    app.config['UPLOADS_ROOT'] = _resolve_uploads_root(app)
    app.config['SOCIOS_PHOTO_UPLOAD_DIR'] = os.path.join(app.config['UPLOADS_ROOT'], 'socios')
    app.config['PERMISOS_UPLOAD_DIR'] = os.path.join(app.config['UPLOADS_ROOT'], 'permisos')
    app.config['BRANDING_UPLOAD_DIR'] = os.path.join(app.config['UPLOADS_ROOT'], 'branding')
    if test_config:
        app.config.update(test_config)
    os.makedirs(app.instance_path, exist_ok=True)
    configure_logging(app)
    register_error_logging(app)
    configure_database(app)
    os.makedirs(app.config['SOCIOS_PHOTO_UPLOAD_DIR'], exist_ok=True)
    os.makedirs(app.config['PERMISOS_UPLOAD_DIR'], exist_ok=True)
    os.makedirs(app.config['BRANDING_UPLOAD_DIR'], exist_ok=True)
    if app.config.get('TESTING_SKIP_BOOTSTRAP'):
        app.config.setdefault('DATABASE', _resolve_database_path(app))
        app.config.setdefault('DATABASE_PRIMARY', app.config['DATABASE'])
        app.config.setdefault('DATABASE_FALLBACK', app.config['DATABASE'])
        app.config['DATABASE_DSN'] = app.config.get('DATABASE_DSN') or app.config['DATABASE']
        init_db(app)
        app.config['DATABASE_RECOVERED'] = False
    else:
        _bootstrap_database(app)
    app.register_blueprint(bp)
    log_system_event(
        'Aplicación Flask iniciada',
        database=app.config.get('DATABASE'),
        excel_path=app.config.get('EXCEL_PATH'),
        recovered=bool(app.config.get('DATABASE_RECOVERED')),
    )
    return app
