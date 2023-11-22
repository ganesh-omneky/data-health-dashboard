import logging
import os
import sys
from typing import Optional

LOG_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def get_logger(
    logger_name: Optional[str], logging_prefix="", mode="dev"
) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    if os.environ.get("LOG_LEVEL") == "DEBUG":
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    handlers = []

    # stdout stream
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    if mode == "dev":
        if logging_prefix and len(logging_prefix) > 0:
            format_str = f"%(asctime)s - [{logging_prefix}][%(name)s][%(funcName)s:%(lineno)d][%(levelname)s] %(message)s"
        else:
            format_str = "%(asctime)s - [%(name)s][%(funcName)s:%(lineno)d][%(levelname)s] %(message)s"
    else:
        format_str = (
            "[test] %(asctime)s - [%(name)s][%(funcName)s:%(lineno)d] %(message)s"
        )
    formatter = logging.Formatter(format_str, datefmt=LOG_DATE_FMT)
    stdout_handler.setFormatter(formatter)
    handlers.append(stdout_handler)

    logger.handlers = []

    # Add more handlers here
    for handler in handlers:
        logger.addHandler(handler)

    return logger
