"""Logging utilities."""
from __future__ import annotations

import logging
from pathlib import Path

LOGGER_NAME = "sumo_linear_corridor"


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)


def configure_logger(log_path: Path, *, console: bool = True, level: int = logging.INFO) -> logging.Logger:
    """Replicate the legacy logger configuration with structured handlers."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False

    for handler in list(logger.handlers):
        try:
            handler.flush()
            handler.close()
        finally:
            logger.removeHandler(handler)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(fmt)
        logger.addHandler(console_handler)

    return logger
