from __future__ import annotations

import logging
import os
import sys
from typing import Optional


_DEFAULT_LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))
_DEFAULT_LOG_PATH = os.path.join(_DEFAULT_LOG_DIR, "findcare.log")
_CONFIGURED = False


class _TeeStream:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, message: str) -> None:
        for stream in self._streams:
            try:
                stream.write(message)
            except Exception:
                pass

    def flush(self) -> None:
        for stream in self._streams:
            try:
                stream.flush()
            except Exception:
                pass


def configure_logging(name: str = "findcare", log_path: Optional[str] = None) -> logging.Logger:
    """
    Configure logging to file + console and tee stdout/stderr to the file.
    Safe to call multiple times.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return logging.getLogger(name)

    effective_path = log_path or os.getenv("FINDCARE_LOG_PATH", _DEFAULT_LOG_PATH)
    os.makedirs(os.path.dirname(effective_path), exist_ok=True)

    file_handler = logging.FileHandler(effective_path, encoding="utf-8")
    stream_handler = logging.StreamHandler()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[file_handler, stream_handler],
    )

    # Route stdout/stderr to the log while preserving console output.
    sys.stdout = _TeeStream(sys.stdout, file_handler.stream)
    sys.stderr = _TeeStream(sys.stderr, file_handler.stream)

    _CONFIGURED = True
    return logging.getLogger(name)
