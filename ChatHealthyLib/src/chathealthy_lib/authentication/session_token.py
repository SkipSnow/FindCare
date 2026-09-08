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



CERTS_DIR = os.environ.get("CERTS_DIR", "/certs")


SERVICE_TO_CERT_NAME = {
    "FindCare":       "findcare",
    "EvaluateCare":   "evalcare",
    "SharedServices": "shared",
}


def cert_basename(origin: str) -> str:
    if origin not in SERVICE_TO_CERT_NAME:
        raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message=f"Invalid token origin {origin!r}; "
            f"must be one of {sorted(SERVICE_TO_CERT_NAME)}.")
    return SERVICE_TO_CERT_NAME[origin]


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
        if len(self.token) < TOKEN_SIZE or not self.token.startswith(TOKEN_PREFIX):
            raise ChatHealthyException(
            mode="value_error",
            component="session_token",
            message=f"malformed token; cannot restamp: {self.token!r}")
        guid = self.get_auth_token()
        new_nonce_field = Nonce.restamp(self.get_nonce())
        original_stamp = Nonce.original_stamp(new_nonce_field)

        certs_dir = os.environ.get("CERTS_DIR", CERTS_DIR)
        key_path = os.path.join(certs_dir, f"{cert_basename(origin)}.key")
        if not os.path.exists(key_path):
            raise ChatHealthyException(
                mode="file_missing",
                component="session_token",
                message=f"signing key not found: {key_path}")
        with open(key_path, "rb") as f:
            private_key = serialization.load_pem_private_key(f.read(), password=None)

        payload = f"{origin}:{original_stamp}:{guid}".encode()
        sig_bytes = private_key.sign(payload, padding.PKCS1v15(), hashes.SHA256())

        now = datetime.now(timezone.utc)
        self.origin = origin
        self.token = f"{TOKEN_PREFIX}{new_nonce_field}{guid}"
        self.signature = base64.b64encode(sig_bytes).decode()
        self.created_at = now.isoformat()
        self.signed = True

    def verify(self, expected_origin: str) -> bool:
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

        certs_dir = os.environ.get("CERTS_DIR", CERTS_DIR)
        cert_path = os.path.join(certs_dir, f"{cert_basename(self.origin)}.crt")
        if not os.path.exists(cert_path):
            raise ChatHealthyException(
                mode="token_infrastructure",
                component="session_token",
                message=f"cert file missing at {cert_path} (CERTS_DIR={certs_dir})")
        try:
            with open(cert_path, "rb") as f:
                cert_pem = f.read()
            cert = load_pem_x509_certificate(cert_pem)
        except Exception as exc:
            raise ChatHealthyException(
                mode="token_infrastructure",
                component="session_token",
                message=f"failed to load cert at {cert_path}: {type(exc).__name__}: {exc}",
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
