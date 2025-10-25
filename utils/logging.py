"""Logging utilities for SK Watch Bot."""
from __future__ import annotations

import os
import sys
from typing import Optional

from loguru import logger

DEFAULT_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)


def setup_logging(extra_sink: Optional[str] = None) -> None:
    """Configure Loguru logging for console and optional file output."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    logger.remove()
    logger.add(sys.stdout, colorize=True, level=log_level, format=DEFAULT_FORMAT)

    sink = extra_sink or os.getenv("LOG_FILE")
    if sink:
        logger.add(
            sink,
            level=log_level,
            format=DEFAULT_FORMAT,
            rotation=os.getenv("LOG_ROTATION", "10 MB"),
            retention=os.getenv("LOG_RETENTION", "14 days"),
            encoding="utf-8",
        )


__all__ = ["logger", "setup_logging"]
