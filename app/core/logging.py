"""Structured logging — structlog + stdlib."""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

import structlog
from structlog.types import FilteringBoundLogger

from app.core.config import settings


def _configure_stdlib() -> None:
    log_dir = Path(settings.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    file_h = logging.handlers.TimedRotatingFileHandler(
        log_dir / "gse-scraper.log", when="midnight", backupCount=30, encoding="utf-8"
    )
    err_h = logging.handlers.TimedRotatingFileHandler(
        log_dir / "gse-errors.log", when="midnight", backupCount=60, encoding="utf-8"
    )
    err_h.setLevel(logging.ERROR)
    handlers += [file_h, err_h]

    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, settings.log_level),
        handlers=handlers,
        force=True,
    )
    for noisy in ("httpx", "httpcore", "asyncio", "sqlalchemy.engine"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def configure_logging() -> None:
    _configure_stdlib()

    shared = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    renderer = (
        structlog.processors.JSONRenderer(serializer=lambda obj, **kw: __import__("orjson").dumps(obj).decode("utf-8"))
        if (settings.log_json or settings.environment == "production")
        else structlog.dev.ConsoleRenderer(colors=True)
    )

    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.log_level)
        ),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
    )
    for handler in logging.root.handlers:
        handler.setFormatter(formatter)


def get_logger(name: str = __name__) -> FilteringBoundLogger:
    return structlog.get_logger(name)