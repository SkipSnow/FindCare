# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).

import base64

from ..exceptions import ChatHealthyException
import os
from datetime import datetime, timezone
from typing import Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.x509 import load_pem_x509_certificate
from pydantic import BaseModel, Field

from ..exceptions import ChatHealthyException
from ..logging_service import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException  # noqa: E402
from .nonce import Nonce


log = ChatHealthyLoggingService()


TOKEN_PREFIX = "CH"
GUID_SIZE = 32
TOKEN_SIZE = len(TOKEN_PREFIX) + Nonce.SIZE + GUID_SIZE
NONCE_OFFSET = len(TOKEN_PREFIX)
GUID_OFFSET = NONCE_OFFSET + Nonce.SIZE


class TokenWidgetData(BaseModel):
    signed_token: str
    nonce: str
    guid: str
    origin: str
    verified: Optional[bool] = None
    server_origin: str
    server_env: str
    time_iso: str


class SessionTokenVerification(BaseModel):
    token_received: str
    signature_received: str
    origin: str
    verified: bool
    created_at: str = ""
    server_env: Optional[str] = None
    guid: str = ""
    nonce: str = ""


# The identity a front-end process reads the registry and the vault as.
# All three services authenticate as this today; when each node has its
# own identity it comes from the process's own binding rather than here.
DEFAULT_READER = os.environ.get("CH_REGISTRY_READER", "frontendUser")

# Every session token is minted by SharedServices, so the signer a peer
# expects is a constant the peer holds -- never a value read off the token
# being checked.
TOKEN_SIGNER = "SharedServices"


class SessionToken(BaseModel):
    origin: str
    token: str
    signature: str
    created_at: str
    signed: bool
    server_env: Optional[str] = None
    last_used: Optional[str] = None

    def get_auth_token(self) -> str:
        if len(self.token) < TOKEN_SIZE:
            raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message=f"token length {len(self.token)} < {TOKEN_SIZE}; cannot extract GUID")
        return self.token[GUID_OFFSET:]

    def get_nonce(self) -> str:
        if len(self.token) < GUID_OFFSET:
            raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message=f"token length {len(self.token)} < {GUID_OFFSET}; cannot extract nonce")
        return self.token[NONCE_OFFSET:GUID_OFFSET]

    def session_guid(self) -> str:
        """The session this token names.

        The GUID is the key of the session document, so any server holding
        a token can reach the session: it is a row in Users.sessions and
        this is its _id. Named for what it is, because the method that
        extracts it is called get_auth_token and a caller reaching for a
        session guid should not have to know that.
        """
        return self.get_auth_token()

    def put_nonce(self, origin: str) -> None:
        """Restamp the nonce for this hop, and sign as the server doing it.

        The requesting server signs. Every verifier holds the public half of
        whichever server may request of it, selected by origin -- so the
        number of keypairs is the number of servers that request, which is
        one while the Gate routes everything.
        """
        if len(self.token) < TOKEN_SIZE or not self.token.startswith(TOKEN_PREFIX):
            raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message=f"malformed token; cannot restamp: {self.token!r}")
        guid = self.get_auth_token()
        new_nonce_field = Nonce.restamp(self.get_nonce())

        self.origin = origin
        self.token = f"{TOKEN_PREFIX}{new_nonce_field}{guid}"
        self.created_at = datetime.now(timezone.utc).isoformat()
        self.sign(reader=DEFAULT_READER)

    def sign(self, reader: str = DEFAULT_READER) -> None:
        """Sign as this token's origin, with that server's own key."""
        from .signing_credential import signing_key  # noqa: PLC0415

        guid = self.get_auth_token()
        original_stamp = Nonce.original_stamp(self.get_nonce())
        pem = signing_key(self.origin, reader)
        private_key = serialization.load_pem_private_key(
            pem.encode(), password=None)
        payload = f"{self.origin}:{original_stamp}:{guid}".encode()
        sig_bytes = private_key.sign(payload, padding.PKCS1v15(), hashes.SHA256())
        self.signature = base64.b64encode(sig_bytes).decode()
        self.signed = True
        log.info("signature written: origin=%s guid=%s sig=%s",
                 self.origin, guid[:8], self.signature[:16])

    def verify(self, expected_origin: str,
               reader: str = DEFAULT_READER) -> bool:
        if not self.signed:
            raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message=f"verify: signed == {self.signed!r}")
        if self.origin != expected_origin:
            raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message=f"verify: origin={self.origin!r} expected={expected_origin!r}")
        if not self.token:
            raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message="verify: empty token")
        if not self.signature:
            raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message="verify: empty signature")
        if len(self.token) < TOKEN_SIZE:
            raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message=f"verify: token length {len(self.token)} < {TOKEN_SIZE}")

        nonce_field = self.get_nonce()
        original_stamp = Nonce.original_stamp(nonce_field)
        guid = self.get_auth_token()

        from .signing_credential import verifying_cert  # noqa: PLC0415

        log.info("signature read: origin=%s guid=%s sig=%s",
                 self.origin, guid[:8], self.signature[:16])
        try:
            cert = load_pem_x509_certificate(
                verifying_cert(self.origin, reader).encode())
        except ChatHealthyException:
            raise
        except Exception as exc:
            raise ChatHealthyException(
                mode="token_infrastructure",
                component="session_token",
                message=(f"the registered certificate for {self.origin!r} did "
                         f"not load: {type(exc).__name__}: {exc}"),
            exception=exc)

        public_key = cert.public_key()
        payload = f"{self.origin}:{original_stamp}:{guid}".encode()
        try:
            sig_bytes = base64.b64decode(self.signature)
        except Exception as exc:
            raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message=f"verify: signature is not valid base64: {type(exc).__name__}: {exc}",
            exception=exc) from exc

        try:
            public_key.verify(sig_bytes, payload, padding.PKCS1v15(), hashes.SHA256())
        except InvalidSignature:
            return False
        except Exception as exc:
            raise ChatHealthyException(
                mode="token_infrastructure",
                component="session_token",
                message=f"crypto.verify raised {type(exc).__name__}: {exc}",
            exception=exc)

        self.last_used = datetime.now(timezone.utc).isoformat()
        return True

    def to_widget_data(self, server_env: str, server_origin: str,
                       verified: Optional[bool] = None) -> TokenWidgetData:
        now = datetime.now(timezone.utc)
        return TokenWidgetData(
            signed_token=self.token,
            nonce=self.get_nonce(),
            guid=self.get_auth_token(),
            origin=self.origin,
            verified=verified,
            server_origin=server_origin,
            server_env=server_env,
            time_iso=now.isoformat(),
        )
