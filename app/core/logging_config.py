"""Central logging setup for application, audit and finance events."""

from __future__ import annotations

import json
import logging
import os
from logging.handlers import RotatingFileHandler

from flask import has_request_context, request, session
from flask.signals import got_request_exception
from werkzeug.exceptions import HTTPException


class RequestContextFilter(logging.Filter):
    """Inject request and user context into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.username = '-'
        record.role = '-'
        record.endpoint = '-'
        record.method = '-'
        record.path = '-'
        record.remote_addr = '-'

        if has_request_context():
            record.username = session.get('username', '-') or '-'
            record.role = session.get('role', '-') or '-'
            record.endpoint = request.endpoint or '-'
            record.method = request.method or '-'
            record.path = request.path or '-'
            record.remote_addr = request.headers.get('X-Forwarded-For', request.remote_addr or '-')
        return True


def _build_handler(file_path: str, level: int) -> RotatingFileHandler:
    """Create a rotating UTF-8 file handler with contextual formatting."""
    handler = RotatingFileHandler(
        file_path,
        maxBytes=2_000_000,
        backupCount=5,
        encoding='utf-8',
    )
    handler.setLevel(level)
    handler.addFilter(RequestContextFilter())
    handler.setFormatter(
        logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | '
            'user=%(username)s role=%(role)s endpoint=%(endpoint)s '
            'method=%(method)s path=%(path)s ip=%(remote_addr)s | %(message)s'
        )
    )
    return handler


def _configure_named_logger(name: str, level: int, handlers: list[logging.Handler]) -> logging.Logger:
    """Attach a clean set of handlers to a named logger."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    logger.handlers.clear()
    for handler in handlers:
        logger.addHandler(handler)
    return logger


def _compose_message(message: str, **details) -> str:
    """Append structured details to the log message when provided."""
    sanitized = {key: value for key, value in details.items() if value is not None and value != ''}
    if not sanitized:
        return message
    return f"{message} | detalles={json.dumps(sanitized, ensure_ascii=False, default=str, sort_keys=True)}"


def configure_logging(app) -> None:
    """Configure application-wide rotating log files."""
    if app.config.get('LOGGING_CONFIGURED'):
        return

    logs_dir = os.path.join(app.instance_path, 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    app.config['LOGS_DIR'] = logs_dir

    system_handler = _build_handler(os.path.join(logs_dir, 'system.log'), logging.INFO)
    error_handler = _build_handler(os.path.join(logs_dir, 'errors.log'), logging.ERROR)
    actions_handler = _build_handler(os.path.join(logs_dir, 'user_actions.log'), logging.INFO)
    finance_handler = _build_handler(os.path.join(logs_dir, 'finance.log'), logging.INFO)

    _configure_named_logger('jawilvio.system', logging.INFO, [system_handler, error_handler])
    _configure_named_logger('jawilvio.audit', logging.INFO, [actions_handler, error_handler])
    _configure_named_logger('jawilvio.finance', logging.INFO, [finance_handler, error_handler])

    app.logger.handlers.clear()
    app.logger.setLevel(logging.INFO)
    app.logger.propagate = False
    app.logger.addHandler(system_handler)
    app.logger.addHandler(error_handler)

    werkzeug_logger = logging.getLogger('werkzeug')
    werkzeug_logger.setLevel(logging.INFO)
    if not any(getattr(handler, 'baseFilename', None) == getattr(system_handler, 'baseFilename', None) for handler in werkzeug_logger.handlers):
        werkzeug_logger.addHandler(system_handler)
        werkzeug_logger.addHandler(error_handler)

    app.config['LOGGING_CONFIGURED'] = True


def register_error_logging(app) -> None:
    """Capture unhandled exceptions without altering Flask responses."""
    if app.config.get('ERROR_LOGGING_REGISTERED'):
        return

    def _log_exception(sender, exception, **extra):
        if isinstance(exception, HTTPException) and exception.code and exception.code < 500:
            return
        logging.getLogger('jawilvio.system').exception(
            _compose_message(
                'Excepción no controlada',
                exception_type=type(exception).__name__,
                error=str(exception),
            )
        )

    got_request_exception.connect(_log_exception, app, weak=False)
    app.config['ERROR_LOGGING_REGISTERED'] = True


def log_system_event(message: str, level: int = logging.INFO, exc_info=False, **details) -> None:
    """Write a general system event to the central application log."""
    logging.getLogger('jawilvio.system').log(level, _compose_message(message, **details), exc_info=exc_info)


def log_user_action_event(action: str, level: int = logging.INFO, **details) -> None:
    """Write an operational user action to the audit log."""
    logging.getLogger('jawilvio.audit').log(level, _compose_message(action, **details))


def log_financial_event(operation: str, level: int = logging.INFO, exc_info=False, **details) -> None:
    """Write a financial operation event to the finance log."""
    logging.getLogger('jawilvio.finance').log(level, _compose_message(operation, **details), exc_info=exc_info)
