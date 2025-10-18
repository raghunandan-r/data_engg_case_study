import logging
import os
import sys


def configure_logging(run_log_path: str, logger_name: str = None):
    """
    Configure root logging to stream to stdout and write to run_log_path.
    Returns a logger (named if provided, else root) and a formatter to reuse
    for any additional per-file handlers.
    """
    os.makedirs(os.path.dirname(run_log_path), exist_ok=True)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clear existing handlers (avoid duplicates in reruns/tests)
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    stream_handler = logging.StreamHandler(
        stream=sys.stdout
    )  # stdout for visibility in parent
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(run_log_path)
    file_handler.setFormatter(formatter)

    root_logger.addHandler(stream_handler)
    root_logger.addHandler(file_handler)

    # Return named logger (propagates to root) or root
    if logger_name:
        return logging.getLogger(logger_name), formatter
    return root_logger, formatter
