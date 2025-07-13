import logging
import sys

from src.util.config import Config


def setup_logger(name):
    logger = logging.getLogger(name)

    # Convert string log level to integer constant
    log_level_str = Config.LOG_LEVEL.upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)

    # Add handler only if it doesn't already have one to prevent duplicate messages
    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
