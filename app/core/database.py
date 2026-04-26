"""Database bootstrap helpers shared across the Flask app."""

import os
import sqlite3
from dataclasses import dataclass

from flask import current_app


@dataclass(frozen=True)
class DatabaseSettings:
    """Minimal settings object that keeps the active engine and DSN together."""

    engine: str
    dsn: str


def configure_database(app):
    """Centralize DB settings so the app can swap engines more easily later."""
    env_engine = (os.getenv('DB_ENGINE') or 'sqlite').strip().lower() or 'sqlite'
    env_dsn = (os.getenv('DATABASE_DSN') or os.getenv('DATABASE_URL') or '').strip()
    app.config.setdefault('DB_ENGINE', env_engine)
    app.config.setdefault('DATABASE_DSN', env_dsn or app.config.get('DATABASE', ''))


def get_database_settings(app=None) -> DatabaseSettings:
    """Read the active database configuration from Flask app settings."""
    app = app or current_app
    engine = str(app.config.get('DB_ENGINE') or 'sqlite').strip().lower()
    dsn = str(app.config.get('DATABASE_DSN') or app.config.get('DATABASE') or '').strip()
    return DatabaseSettings(engine=engine, dsn=dsn)


def get_connection(app=None):
    """Open a connection for the configured engine while keeping callers agnostic."""
    settings = get_database_settings(app)
    if settings.engine == 'sqlite':
        conn = sqlite3.connect(settings.dsn)
        conn.row_factory = sqlite3.Row
        return conn
    raise RuntimeError(
        "DB_ENGINE no soportado todavía. La arquitectura ya está preparada para PostgreSQL, "
        "pero solo SQLite está habilitado en esta versión."
    )
