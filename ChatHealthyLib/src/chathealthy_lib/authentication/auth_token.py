# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).

from ..exceptions import ChatHealthyException

from ..exceptions import ChatHealthyException
from ..logging_service import ChatHealthyLoggingService
from typing import Any, Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field

from .nonce import Nonce
from .session_token import (
    SessionToken,
    SessionTokenVerification,
)


log = ChatHealthyLoggingService()
class AuthToken:
    def __init__(self, session_token: SessionToken, origin: str):
        if not isinstance(session_token, SessionToken):
            raise ChatHealthyException(
            mode="type_error",
            component="auth_token",
            message=f"AuthToken requires a SessionToken, got {type(session_token).__name__}")
        self._st = session_token
        self._origin = origin

    @property
    def origin(self) -> str:
        return self._origin

    @property
    def guid(self) -> str:
        return self._st.get_auth_token()

    @property
    def latest_stamp(self) -> str:
        return Nonce.latest_stamp(self._st.get_nonce())

    @property
    def original_stamp(self) -> str:
        return Nonce.original_stamp(self._st.get_nonce())

    def update_nonce(self) -> None:
        self._st.put_nonce(self._origin)

    def to_wire(self) -> SessionToken:
        return self._st

    def verify(self) -> bool:
        """Check the signature against the signer's registered certificate.

        The expected signer is a constant this process holds. It is not read
        off the token: a token that names its own expected origin chooses
        which key verifies it.
        """
        from .session_token import TOKEN_SIGNER  # noqa: PLC0415
        return self._st.verify(expected_origin=TOKEN_SIGNER)

    def to_verification(self, valid: bool, server_env: Optional[str]) -> SessionTokenVerification:
        sig = self._st.signature or ""
        return SessionTokenVerification(
            token_received=self._st.token or "",
            signature_received=(sig[:40] + "...") if sig else "none",
            origin=self._origin,
            verified=valid,
            created_at=self._st.created_at or "",
            server_env=server_env if server_env is not None else self._st.server_env,
            guid=self.guid,
            nonce=self._st.get_nonce(),
        )

    @classmethod
    def handle_session(cls, body: Any, origin: str, server_env: str) -> SessionToken:
        st_in = getattr(body, "session_token", None)
        if not st_in:
            raise ChatHealthyException(
                mode="http_error",
                component="auth_token",
                message="session_token is required",
                status_code=400)
        at = cls(st_in, origin)
        at.update_nonce()
        wire = at.to_wire()
        wire.server_env = server_env
        return wire

    @classmethod
    def handle_verify(cls, body: Any, origin: str, server_env: str) -> "VerifyTokenResponse":
        st_in = getattr(body, "session_token", None)
        if not st_in:
            raise ChatHealthyException(
                mode="http_error",
                component="auth_token",
                message="session_token is required",
                status_code=400)
        at = cls(st_in, origin)
        try:
            valid = at.verify()
        except ValueError as e:
            raise ChatHealthyException(
                mode="http_error",
                component="auth_token",
                message=str(e),
                status_code=400,
            exception=e)
        return VerifyTokenResponse(
            status="verified" if valid else "failed",
            session_token=at.to_verification(valid=valid, server_env=server_env),
        )


class SessionRestampRequest(BaseModel):
    session_token: Optional[SessionToken] = Field(default=None)


class VerifyTokenResponse(BaseModel):
    status: str
    session_token: SessionTokenVerification
