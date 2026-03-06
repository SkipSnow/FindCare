# -------------------------------------------------------------------------------
# File: ChatHealthyMongoUtilities.py
# Author: Skip Snow
# Copyright (c) 2025 Skip Snow. All rights reserved.
# -------------------------------------------------------------------------------

from __future__ import annotations

import time
import logging
from typing import Optional

try:
    from pymongo.mongo_client import MongoClient
    from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "pymongo is not installed in the active venv. Run: pip install pymongo"
    ) from exc

logger = logging.getLogger(__name__)

# Default timeouts (ms) - shorter for faster failure detection
DEFAULT_CONNECT_TIMEOUT_MS = 10000
DEFAULT_SERVER_SELECTION_TIMEOUT_MS = 15000


def _format_mongo_error(e: Exception) -> str:
    """Produce a clearer error message with likely causes."""
    err_str = str(e)
    if "timed out" in err_str.lower() or "timeout" in err_str.lower():
        return (
            f"MongoDB connection timeout: {e}\n\n"
            "Likely causes:\n"
            "1. IP WHITELIST: MongoDB Atlas blocks connections by default. Add 0.0.0.0/0 to "
            "Network Access → IP Access List to allow HuggingFace Spaces (or add your deployment IPs).\n"
            "2. Network/firewall: The host (e.g. HuggingFace) may block outbound connections to MongoDB ports.\n"
            "3. Wrong connection string or cluster paused."
        )
    return str(e)


class ChatHealthyMongoUtilities:
    """
    Manages a MongoDB connection with lazy init, retry, and clearer error handling.

    - Lazy connection: connects on first getConnection() call, not at import
    - Retry with exponential backoff on transient failures
    - Configurable timeouts for faster failure detection
    - Clearer error messages (e.g. IP whitelist hint for Atlas)
    """

    def __init__(
        self,
        connection_string: str,
        *,
        connect_timeout_ms: int = DEFAULT_CONNECT_TIMEOUT_MS,
        server_selection_timeout_ms: int = DEFAULT_SERVER_SELECTION_TIMEOUT_MS,
        max_retries: int = 2,
        retry_delay_sec: float = 2.0,
    ) -> None:
        if not connection_string or not isinstance(connection_string, str):
            raise ValueError("A valid MongoDB connection string must be provided.")

        self._connection_string = connection_string
        self._connect_timeout_ms = connect_timeout_ms
        self._server_selection_timeout_ms = server_selection_timeout_ms
        self._max_retries = max_retries
        self._retry_delay_sec = retry_delay_sec
        self._client: Optional[MongoClient] = None
        self._connection_failed: Optional[Exception] = None  # Last failure, if any

    def _create_and_validate_client(self) -> None:
        last_error: Optional[Exception] = None
        for attempt in range(self._max_retries + 1):
            try:
                client = MongoClient(
                    self._connection_string,
                    connectTimeoutMS=self._connect_timeout_ms,
                    serverSelectionTimeoutMS=self._server_selection_timeout_ms,
                )
                client.admin.command("ping")
                self._client = client
                self._connection_failed = None
                logger.info("MongoDB connection established and validated.")
                return
            except ServerSelectionTimeoutError as e:
                last_error = e
                logger.warning(
                    "MongoDB connection attempt %d/%d timed out: %s",
                    attempt + 1,
                    self._max_retries + 1,
                    e,
                )
            except PyMongoError as e:
                last_error = e
                logger.warning("MongoDB connection attempt %d/%d failed: %s", attempt + 1, self._max_retries + 1, e)

            if attempt < self._max_retries:
                time.sleep(self._retry_delay_sec * (attempt + 1))

        self._connection_failed = last_error
        raise ConnectionError(_format_mongo_error(last_error)) from last_error

    def getConnection(self) -> MongoClient:
        """Returns the active MongoClient. Connects lazily on first call."""
        if self._client is None:
            if self._connection_failed is not None:
                raise ConnectionError(_format_mongo_error(self._connection_failed)) from self._connection_failed
            self._create_and_validate_client()
        return self._client

    def is_available(self) -> bool:
        """Returns True if MongoDB is connected and reachable."""
        if self._client is None and self._connection_failed is not None:
            return False
        try:
            self.getConnection().admin.command("ping")
            return True
        except Exception:
            return False

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
                logger.info("MongoDB connection closed.")
            finally:
                self._client = None
