"""
统一日志配置：控制台 Rich 输出 + JSONL 落盘。
"""

from __future__ import annotations

import json
import logging
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from rich.logging import RichHandler

from meme_detector.config import settings

_LOG_CONTEXT: ContextVar[dict[str, Any]] = ContextVar("log_context", default={})
_LOGGING_CONFIGURED = False
_RESERVED_RECORD_ATTRS = set(logging.makeLogRecord({}).__dict__.keys())


class LogContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        context = _LOG_CONTEXT.get()
        for key, value in context.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }
        for key in (
            "event",
            "job_name",
            "run_id",
            "trigger_mode",
            "word",
            "bvid",
            "conversation_id",
            "chunk_index",
            "candidate_count",
            "raw_summary",
            "result_count",
            "accepted_count",
            "rejected_count",
            "failed_count",
        ):
            value = getattr(record, key, None)
            if value not in (None, ""):
                payload[key] = value

        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _RESERVED_RECORD_ATTRS
            and key not in payload
            and not key.startswith("_")
        }
        if extras:
            payload["extra"] = extras

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging() -> None:
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    log_level_name = str(settings.log_level or "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    log_dir = Path(settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / settings.log_json_filename

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level)

    context_filter = LogContextFilter()

    console_handler = RichHandler(
        rich_tracebacks=True,
        show_path=False,
        markup=True,
    )
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    console_handler.addFilter(context_filter)

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(JsonLogFormatter())
    file_handler.addFilter(context_filter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    logging.captureWarnings(True)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(log_level)

    _LOGGING_CONFIGURED = True
    get_logger(__name__).info(
        "logging initialized",
        extra={
            "event": "logging_initialized",
            "log_dir": str(log_dir),
            "log_json_filename": settings.log_json_filename,
            "log_level": log_level_name,
        },
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def bind_log_context(**kwargs: Any) -> Token:
    current = dict(_LOG_CONTEXT.get())
    current.update({key: value for key, value in kwargs.items() if value not in (None, "")})
    return _LOG_CONTEXT.set(current)


def reset_log_context(token: Token) -> None:
    _LOG_CONTEXT.reset(token)


def clear_log_context() -> None:
    _LOG_CONTEXT.set({})
