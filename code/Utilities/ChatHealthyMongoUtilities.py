# -------------------------------------------------------------------------------
# File: MongoDBConnectionManager.py
# Author: Skip Snow
# Co-Author: GPT-5
# Copyright (c) 2025 Skip Snow. All rights reserved.
# -------------------------------------------------------------------------------

from __future__ import annotations

from typing import Optional

try:
    from pymongo.mongo_client import MongoClient
    from pymongo.errors import PyMongoError
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "pymongo is not installed in the active venv. Run: pip install pymongo"
    ) from exc


class ChatHealthyMongoUtilities:
    """
    Manages a MongoDB connection with lifecycle control.

    Behavior:
    - Constructor creates and validates a MongoClient via ping
    - get_connection() returns the existing client after ping validation
    - Raises exceptions if connection or ping fails
    - Automatically closes the client when the object is destroyed
    - Supports context-manager usage (with ...)
    """

    def __init__(self, connection_string: str) -> MongoClient:
        if not connection_string or not isinstance(connection_string, str):
            raise ValueError("A valid MongoDB connection string must be provided.")

        self._connection_string: str = connection_string
        self._client: Optional[MongoClient] = None

        self._create_and_validate_client()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _create_and_validate_client(self) -> None:
        try:
            client = MongoClient(self._connection_string)

            # Lightweight health check
            client.admin.command("ping")

            self._client = client
            print("MongoDB connection successfully established and validated.")

        except PyMongoError as e:
            raise ConnectionError(
                f"Failed to create or validate MongoDB connection: {e}"
            ) from e

    def _validate_existing_client(self) -> None:
        if self._client is None:
            raise ConnectionError("MongoDB client is not initialized.")

        try:
            self._client.admin.command("ping")
        except PyMongoError as e:
            raise ConnectionError(
                f"Existing MongoDB connection failed ping check: {e}"
            ) from e

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def getConnection(self) -> MongoClient:
        """
        Returns the active MongoClient after validating it with ping.
        """
        self._validate_existing_client()
        return self._client

    def close(self) -> None:
        """
        Explicitly closes the MongoDB client.
        """
        if self._client is not None:
            try:
                self._client.close()
                print("MongoDB connection closed.")
            finally:
                self._client = None

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------
    def __enter__(self) -> MongoClient:
        return self.get_connection()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Destructor (best-effort cleanup)
    # ----------------------------------
