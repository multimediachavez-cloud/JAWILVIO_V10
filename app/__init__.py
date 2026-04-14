from flask import Flask
import os
import sqlite3
import tempfile
import glob
from .db import init_db, import_excel_if_needed, seed_saldos_historicos
from .routes import bp


def _resolve_excel_path(app):
    downloads_dir = os.path.join(os.path.expanduser('~'), 'Downloads')
    exact_file = os.path.join(downloads_dir, 'este.xlsx')
    if os.path.exists(exact_file):
        return exact_file
    candidates = glob.glob(os.path.join(downloads_dir, 'PRESTAMOS JAWILVIO*.xlsx'))
    if candidates:
        return max(candidates, key=os.path.getmtime)
    return os.path.join(app.root_path, '..', 'data', 'asociacion_jawilvio.xlsx')


def _bootstrap_database(app):
    primary_db = os.path.join(app.instance_path, 'jawilvio_v10.db')
    fallback_db = os.path.join(tempfile.gettempdir(), 'jawilvio_v10_recovered.db')

    app.config['DATABASE'] = primary_db
    app.config['DATABASE_PRIMARY'] = primary_db
    app.config['DATABASE_FALLBACK'] = fallback_db

    try:
        init_db(app)
        import_excel_if_needed(app)
        seed_saldos_historicos(app)
        app.config['DATABASE_RECOVERED'] = False
    except sqlite3.OperationalError:
        app.config['DATABASE'] = fallback_db
        init_db(app)
        import_excel_if_needed(app)
        seed_saldos_historicos(app)
        app.config['DATABASE_RECOVERED'] = True


def create_app():
    app = Flask(__name__, instance_relative_config=True)
    app.config['SECRET_KEY'] = 'jawilvio-v10-secret'
    app.config['EXCEL_PATH'] = _resolve_excel_path(app)
    app.config['SOCIOS_PHOTO_UPLOAD_DIR'] = os.path.join(app.static_folder, 'uploads', 'socios')
    app.config['PERMISOS_UPLOAD_DIR'] = os.path.join(app.static_folder, 'uploads', 'permisos')
    os.makedirs(app.instance_path, exist_ok=True)
    os.makedirs(app.config['SOCIOS_PHOTO_UPLOAD_DIR'], exist_ok=True)
    os.makedirs(app.config['PERMISOS_UPLOAD_DIR'], exist_ok=True)
    _bootstrap_database(app)
    app.register_blueprint(bp)
    return app
