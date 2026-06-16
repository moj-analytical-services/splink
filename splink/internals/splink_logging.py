from __future__ import annotations

import logging
import sys
from typing import TextIO

DEFAULT_FORMAT = "%(message)s"
VERBOSE = 15
PIPELINE = 7
SQL = 5

_SPLINK_DEFAULT_HANDLER_ATTR = "_splink_default_handler"


def _validate_level(level: int | str) -> None:
    if isinstance(level, bool):
        raise TypeError("level must be an int, str, or None; use None to disable")


def enable(
    level: int | str = logging.INFO,
    *,
    stream: TextIO | None = None,
    format: str = DEFAULT_FORMAT,
) -> None:
    """Configure logging for Splink messages without changing root logging."""
    _validate_level(level)

    splink_logger = logging.getLogger("splink")
    splink_logger.setLevel(level)

    if splink_logger.hasHandlers():
        return

    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(logging.Formatter(format))
    setattr(handler, _SPLINK_DEFAULT_HANDLER_ATTR, True)
    splink_logger.addHandler(handler)
    splink_logger.propagate = False


def disable() -> None:
    """Remove Splink's default handler, leaving user-provided handlers alone."""
    splink_logger = logging.getLogger("splink")
    for handler in list(splink_logger.handlers):
        if getattr(handler, _SPLINK_DEFAULT_HANDLER_ATTR, False):
            splink_logger.removeHandler(handler)
            handler.close()


logging.addLevelName(VERBOSE, "VERBOSE")
logging.addLevelName(PIPELINE, "PIPELINE")
logging.addLevelName(SQL, "SQL")
