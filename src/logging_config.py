"""Centralized logging configuration for the application."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

MAX_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 3
LOG_FILENAME = "app.txt"
LOG_FORMAT = (
    "[%(asctime)s] [%(levelname)-7s] "
    "[%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
)


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def setup_logging(level: int = logging.DEBUG) -> Path:
    """Configure root logger with a rotating txt file handler."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    logs_dir = _project_root() / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / LOG_FILENAME

    for handler in root_logger.handlers:
        if isinstance(handler, RotatingFileHandler) and Path(handler.baseFilename) == log_path:
            return log_path

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
    root_logger.addHandler(file_handler)
    return log_path
