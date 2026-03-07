"""Centralized logging configuration for the application."""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

MAX_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 3
LOG_FILENAME = "app.txt"
FILE_LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
CONSOLE_LOG_FORMAT = "%(levelname)s | %(message)s"
PROJECT_LOGGER_PREFIXES = ("app", "app_flask", "run", "src")
FILE_HANDLER_MARKER = "personal_finances_file_handler"
CONSOLE_HANDLER_MARKER = "personal_finances_console_handler"


class _ProjectConsoleFilter(logging.Filter):
    """Show project logs in console and keep external logs only for errors."""

    def filter(self, record: logging.LogRecord) -> bool:
        is_project_log = record.name == "root" or record.name.startswith(PROJECT_LOGGER_PREFIXES)
        return is_project_log or record.levelno >= logging.ERROR


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _resolve_log_level(env_var: str, default: int) -> int:
    value = os.getenv(env_var, "").strip().upper()
    if not value:
        return default
    resolved = logging.getLevelName(value)
    return resolved if isinstance(resolved, int) else default


def _find_handler(root_logger: logging.Logger, marker: str) -> logging.Handler | None:
    for handler in root_logger.handlers:
        if getattr(handler, "_pf_handler_marker", None) == marker:
            return handler
    return None


def setup_logging() -> Path:
    """Configure root logger with concise console logs and detailed file logs."""
    file_level = _resolve_log_level("LOG_FILE_LEVEL", logging.DEBUG)
    console_level = _resolve_log_level("LOG_CONSOLE_LEVEL", logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(min(file_level, console_level))

    logs_dir = _project_root() / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / LOG_FILENAME

    file_handler = _find_handler(root_logger, FILE_HANDLER_MARKER)
    if (
        file_handler is None
        or not isinstance(file_handler, RotatingFileHandler)
        or Path(file_handler.baseFilename) != log_path
    ):
        if file_handler is not None:
            root_logger.removeHandler(file_handler)
            file_handler.close()
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=MAX_BYTES,
            backupCount=BACKUP_COUNT,
            encoding="utf-8",
        )
        setattr(file_handler, "_pf_handler_marker", FILE_HANDLER_MARKER)
        root_logger.addHandler(file_handler)
    file_handler.setLevel(file_level)
    file_handler.setFormatter(logging.Formatter(FILE_LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))

    console_handler = _find_handler(root_logger, CONSOLE_HANDLER_MARKER)
    if console_handler is None or not isinstance(console_handler, logging.StreamHandler):
        if console_handler is not None:
            root_logger.removeHandler(console_handler)
            console_handler.close()
        console_handler = logging.StreamHandler()
        setattr(console_handler, "_pf_handler_marker", CONSOLE_HANDLER_MARKER)
        root_logger.addHandler(console_handler)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(logging.Formatter(CONSOLE_LOG_FORMAT))
    console_handler.filters = [
        active_filter
        for active_filter in console_handler.filters
        if not isinstance(active_filter, _ProjectConsoleFilter)
    ]
    console_handler.addFilter(_ProjectConsoleFilter())

    return log_path
