"""Shared logging configuration for both services."""

from __future__ import annotations

import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(level: str = "INFO") -> None:
    """Send readable, timestamped logs to stderr.

    Both services use the same format so their output can be read side by side
    in two terminals. ``force=True`` replaces any handler a library installed
    first, which keeps the format consistent under uvicorn.

    Args:
        level: A level name such as ``DEBUG`` or ``INFO``. An unrecognised
            value falls back to ``INFO`` rather than raising, so a typo in an
            environment variable cannot stop a service from starting.
    """
    resolved = getattr(logging, str(level).upper(), None)
    if not isinstance(resolved, int):
        resolved = logging.INFO

    logging.basicConfig(
        level=resolved,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        stream=sys.stderr,
        force=True,
    )
