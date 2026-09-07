# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""OAuth Login Tool — EPIC-002-F-003-S-004.

ChatHealthyTool that owns the OAuth login flow. The verify path is one
code path regardless of env: POST to TOKEN_ENDPOINT_URL, extract id_token,
verify signature against TRUSTED_KEYS. The fake IdP signs real JWTs;
no claim-fabrication code exists in the verification path.

Env conditionals are module-load constants only (TOKEN_ENDPOINT_URL,
_LOCAL_EXTRA_KEYS, TLS_VERIFY, PRE_ALPHA_ALLOW_LIST). Runtime code is
one path.
"""
from __future__ import annotations

import os
import secrets as _secrets
from datetime import datetime, timezone
from typing import Literal, Optional
from urllib.parse import urlencode

import httpx
import jwt
from jwt import PyJWKClient, PyJWKClientError
from pydantic import BaseModel, Field

from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException

from chathealthy_lib.authentication.agent_deps import AgentDeps
from chathealthy_lib.authentication.chathealthy_tool import ChatHealthyTool
from authentication.google_oauth_endpoint import (
    GOOGLE_AUTHZ_URL,
    build_state,
    client_id as _google_client_id,
    redirect_uri as _google_redirect_uri,
)

log = ChatHealthyLoggingService()


GOOGLE_JWKS_URL = "https://www.googleapis.com/oauth2/v3/certs"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
TRUSTED_ISSUERS = ("https://accounts.google.com",)
_google_jwks_client = PyJWKClient(GOOGLE_JWKS_URL, cache_keys=True)


class _KidResolver(dict):
    def __missing__(self, kid):
        try:
            key = _google_jwks_client.get_signing_key(kid).key
        except PyJWKClientError as exc:
            raise ChatHealthyException(
                mode="oauth_login_unknown_signing_kid",
                message=f"OAuthLoginTool: unknown signing kid {kid!r}: {exc}",
                component="OAuthLoginTool",
                exception=exc,
            ) from exc
        val = (key, "RS256")
        self[kid] = val
        return val


_KID_TABLE = _KidResolver()


def _real_google_authz_url(state: str, flow: str) -> str:
    params = {
        "client_id":     _google_client_id(),
        "redirect_uri":  _google_redirect_uri(os.getenv("ENV_PREFIX", "dev")),
        "response_type": "code",
        "scope":         "openid email",
        "state":         state,
        "prompt":        "select_account",
        "access_type":   "online",
    }
    return GOOGLE_AUTHZ_URL + "?" + urlencode(params)


_LOCAL_EXTRA_KEYS: dict = {}
TOKEN_ENDPOINT_URL = GOOGLE_TOKEN_URL
TLS_VERIFY = True
_LOCAL_TEST_IDENTITIES: tuple = ()
AUTHZ_URL_BUILDER = _real_google_authz_url


PRE_ALPHA_ALLOW_LIST = frozenset(
    e.lower() for e in (("skip.snow@gmail.com",) + _LOCAL_TEST_IDENTITIES)
)


def message_new_user_success(email: str) -> str:
    return (
        f"You're logged in, {email}. ChatHealthy is glad to have you as a "
        "pre-alpha user. Welcome aboard."
    )


def message_returning_user_success(email: str, last_login_iso: str) -> str:
    return f"Welcome back, {email}. You last logged in at {last_login_iso}."


def message_login_failed(email: Optional[str]) -> str:
    who = f" for {email}" if email else ""
    return (
        f"Login failed{who}. In this pre-alpha state Google demands that "
        "ChatHealthy registers them as pre-alpha users. Please email "
        "skip@chathealthy.ai, or text Skip at 01 (646) 408-8999 to be "
        "added to the pre-alpha list."
    )


class Request(BaseModel):
    model_config = {"extra": "ignore"}
    phase: Literal["start", "callback"] = Field(
        description="start begins the login and returns the provider URL to "
                    "send the browser to; callback completes it with what the "
                    "provider sent back.")
    identity_provider: str = Field(
        default="Google",
        description="Which identity provider is being used.")
    server_env: str = Field(
        min_length=1,
        description="Environment this login belongs to, so the callback "
                    "returns to the right one.")
    session_guid: str = Field(
        min_length=32, max_length=32,
        description="The session being logged into, so the identity lands on "
                    "the session that started the flow.")
    oauth_code: Optional[str] = Field(
        default=None,
        description="Callback only: the provider's authorization code, "
                    "exchanged for the identity.")
    oauth_state: Optional[str] = Field(
        default=None,
        description="Callback only: the value sent at start, returned to "
                    "prove the callback belongs to this flow.")
    flow: str = Field(
        default="login",
        description="Whether this is a sign-in or another account action.")


class Response(BaseModel):
    outcome: Literal["redirect", "success", "fail"]
    authz_url: Optional[str] = None
    user_id: Optional[str] = None
    public_username: Optional[str] = None
    last_login_at: Optional[str] = None
    user_facing_message: str = ""
    google_claims: Optional[dict] = None
    fail_reason: Optional[str] = None


def _client_id() -> str:
    cid = os.getenv("GOOGLE_OAUTH_CLIENT_ID", "")
    if not cid:
        raise ChatHealthyException(
            mode="oauth_login_missing_google_client_id",
            message="GOOGLE_OAUTH_CLIENT_ID not configured on this Space.",
            component="OAuthLoginTool",
        )
    return cid


def _env_to_redirect_uri(server_env: str) -> str:
    table = {
        "dev":   "https://dev-hf.chathealthy.ai/auth/google/callback",
        "qa":    "https://skipsnow-qa-sharedservicesspace.hf.space/auth/google/callback",
        "prod":  "https://skipsnow-sharedservicesspace.hf.space/auth/google/callback",
        "local": "https://localhost:8002/auth/google/callback",
    }
    uri = table.get(server_env)
    if not uri:
        raise ChatHealthyException(
            mode="oauth_login_unknown_env",
            message=f"OAuthLoginTool: unknown server_env {server_env!r}.",
            component="OAuthLoginTool",
        )
    return uri


def _is_allowed_prealpha(email: str) -> bool:
    return email.lower() in PRE_ALPHA_ALLOW_LIST


def _verify_id_token(token: str, audience: str) -> dict:
    kid = jwt.get_unverified_header(token).get("kid")
    key, algo = _KID_TABLE[kid]
    try:
        return jwt.decode(
            token,
            key=key,
            audience=audience,
            issuer=list(TRUSTED_ISSUERS),
            algorithms=[algo],
            options={"require": ["iss", "sub", "aud", "exp", "iat"]},
        )
    except jwt.PyJWTError as exc:
        raise ChatHealthyException(
            mode="oauth_login_id_token_verification_failed",
            message=f"OAuthLoginTool: id_token verification failed: {exc}",
            component="OAuthLoginTool",
            exception=exc,
        ) from exc


async def _exchange_and_verify(code: str, server_env: str) -> dict:
    audience = _client_id()
    client_secret = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "")
    async with httpx.AsyncClient(timeout=10.0, verify=TLS_VERIFY) as client:
        r = await client.post(
            TOKEN_ENDPOINT_URL,
            data={
                "code":          code,
                "client_id":     audience,
                "client_secret": client_secret,
                "redirect_uri":  _env_to_redirect_uri(server_env),
                "grant_type":    "authorization_code",
            },
        )
    if r.status_code != 200:
        raise ChatHealthyException(
            mode="oauth_login_token_exchange_failed",
            message=(
                f"OAuthLoginTool: token exchange returned HTTP "
                f"{r.status_code}: {r.text[:300]}"
            ),
            component="OAuthLoginTool",
        )
    tokens = r.json()
    id_token_str = tokens.get("id_token")
    if not id_token_str:
        raise ChatHealthyException(
            mode="oauth_login_missing_id_token",
            message="OAuthLoginTool: token response had no id_token.",
            component="OAuthLoginTool",
        )
    return _verify_id_token(id_token_str, audience)


def _persist_login(
    sessions_coll, users_coll, claims: dict, session_guid: str,
) -> tuple[str, Optional[str]]:
    from chathealthy_lib.authentication.user_object import UserObject, OAuthIdentity
    from chathealthy_lib.authentication.agent_deps import append_action
    email = claims["email"]
    sub = claims["sub"]
    identity_provider = "Google"
    now_iso = datetime.now(timezone.utc).isoformat()

    guest_doc = sessions_coll.find_one({"_id": session_guid}) or {}
    guest_payload = {k: v for k, v in guest_doc.items() if k != "_id"}
    if not guest_payload:
        raise ChatHealthyException(
            mode="oauth_login_no_session_for_callback",
            message=f"OAuthLoginTool: no session_doc for _id={session_guid[:8]}...",
            component="OAuthLoginTool",
        )
    guest = UserObject.model_validate(guest_payload)

    existing = users_coll.find_one({
        "user_object.OAuthIdentities": {
            "$elemMatch": {
                "identity_provider": identity_provider,
                "identity_provider_user_id": sub,
            },
        },
    })

    if existing is None:
        user_id = "u-" + _secrets.token_urlsafe(16)
        stored = UserObject(
            current_session_token="NULL",
            expires_at=guest.expires_at,
            is_registered=True,
            user_id=user_id,
            user_type="Prospect",
            public_username=email,
            OAuthIdentities=[OAuthIdentity(
                identity_provider=identity_provider,
                identity_provider_user_id=sub,
                email=email,
            )],
        )
        prior_last_login = None
        first_login_at = now_iso
    else:
        user_id = existing["user_id"]
        prior_last_login = existing.get("last_login_at")
        first_login_at = existing.get("first_login_at", now_iso)
        stored = UserObject.model_validate(existing.get("user_object", {}))
        for ident in stored.OAuthIdentities:
            if (ident.identity_provider == identity_provider
                    and ident.identity_provider_user_id == sub):
                ident.email = email

    merged = stored.merge(guest)
    append_action(
        merged,
        tool_name="oauth_login",
        input_json={
            "identity_provider": identity_provider,
            "email": email,
            "session_guid_prefix": session_guid[:8] + "...",
        },
        output_json={
            "user_id": user_id,
            "outcome": "success",
            "new_user": prior_last_login is None,
        },
    )
    merged_dump = merged.model_dump(mode="json", exclude_none=True)

    users_result = users_coll.update_one(
        {"user_id": user_id},
        {"$set": {
            "user_id": user_id,
            "user_object": merged_dump,
            "first_login_at": first_login_at,
            "last_login_at": now_iso,
        }},
        upsert=True,
    )
    if not (users_result.matched_count == 1 or users_result.upserted_id is not None):
        raise ChatHealthyException(
            mode="oauth_login_users_doc_write_failed",
            message=f"OAuthLoginTool: Users.users write failed for user_id={user_id}.",
            component="OAuthLoginTool",
        )

    sessions_result = sessions_coll.replace_one(
        {"_id": session_guid},
        {"_id": session_guid, **merged_dump},
        upsert=True,
    )
    if not (sessions_result.matched_count == 1 or sessions_result.upserted_id is not None):
        raise ChatHealthyException(
            mode="oauth_login_session_doc_write_failed",
            message=(
                f"OAuthLoginTool: session_doc write failed for _id="
                f"{session_guid[:8]}..."
            ),
            component="OAuthLoginTool",
        )
    return user_id, prior_last_login


def _build_authz_url(server_env: str, session_guid: str, flow: str) -> str:
    state = build_state(server_env, session_guid=session_guid)
    return AUTHZ_URL_BUILDER(state, flow)


class OAuthLoginTool(ChatHealthyTool):
    """EPIC-002-F-003-S-004 Login & Registration via OAuth."""
    TOOL_NAME = "oauth_login"
    Request = Request
    Response = Response

    async def run(self, deps: AgentDeps, request: "Request") -> "Response":
        session_prefix = (request.session_guid or "")[:8] + "..."
        log.info(
            "OAUTH-AUDIT attempt phase=%s identity_provider=%s server_env=%s "
            "session_guid_prefix=%s flow=%s has_oauth_code=%s has_oauth_state=%s",
            request.phase, request.identity_provider, deps.server_env,
            session_prefix, request.flow,
            bool(request.oauth_code), bool(request.oauth_state),
        )

        if request.phase == "start":
            response = self._run_start(deps, request)
        else:
            response = await self._run_callback(deps, request)

        log.info(
            "OAUTH-AUDIT result phase=%s outcome=%s public_username=%s "
            "user_id=%s fail_reason=%s session_guid_prefix=%s "
            "authz_url_built=%s",
            request.phase, response.outcome, response.public_username,
            response.user_id, response.fail_reason, session_prefix,
            bool(response.authz_url),
        )
        return response

    def _run_start(self, deps: AgentDeps, request: "Request") -> "Response":
        authz_url = _build_authz_url(
            deps.server_env, request.session_guid, request.flow,
        )
        return Response(outcome="redirect", authz_url=authz_url)

    async def _run_callback(
        self, deps: AgentDeps, request: "Request",
    ) -> "Response":
        if not request.oauth_code:
            log.debug(
                "OAUTH-CB missing_oauth_code session_guid_prefix=%s",
                (request.session_guid or "")[:8] + "...",
            )
            return Response(
                outcome="fail",
                user_facing_message=message_login_failed(None),
                fail_reason="missing_oauth_code",
            )
        log.debug(
            "OAUTH-CB exchange begin token_endpoint=%s code_prefix=%s",
            TOKEN_ENDPOINT_URL, request.oauth_code[:12] + "...",
        )
        claims = await _exchange_and_verify(request.oauth_code, deps.server_env)
        log.debug(
            "OAUTH-CB exchange ok iss=%s aud=%s sub=%s email=%s email_verified=%s "
            "iat=%s exp=%s",
            claims.get("iss"), claims.get("aud"), claims.get("sub"),
            claims.get("email"), claims.get("email_verified"),
            claims.get("iat"), claims.get("exp"),
        )

        email = claims.get("email", "")
        if not email or not claims.get("email_verified"):
            log.debug(
                "OAUTH-CB email_not_verified email=%r email_verified=%s",
                email, claims.get("email_verified"),
            )
            return Response(
                outcome="fail",
                user_facing_message=message_login_failed(email or None),
                fail_reason="email_not_verified",
                google_claims=claims,
            )

        on_list = _is_allowed_prealpha(email)
        log.debug(
            "OAUTH-CB allow_list_check email_lower=%s on_list=%s list_size=%d",
            email.lower(), on_list, len(PRE_ALPHA_ALLOW_LIST),
        )
        if not on_list:
            return Response(
                outcome="fail",
                user_facing_message=message_login_failed(email),
                fail_reason="not_on_prealpha_allow_list",
                google_claims=claims,
            )

        sessions_coll = deps.mongo_frontend["Users"]["sessions"]
        users_coll = deps.mongo_frontend["Users"]["users"]
        user_id, prior_last_login = _persist_login(
            sessions_coll, users_coll, claims, request.session_guid,
        )
        log.debug(
            "OAUTH-CB persist ok user_id=%s new_user=%s prior_last_login=%s "
            "session_guid_prefix=%s",
            user_id, prior_last_login is None, prior_last_login,
            (request.session_guid or "")[:8] + "...",
        )

        if prior_last_login is None:
            user_facing = message_new_user_success(email)
        else:
            user_facing = message_returning_user_success(email, prior_last_login)
        log.debug(
            "OAUTH-CB success email=%s user_id=%s message_preview=%r",
            email, user_id, user_facing[:80],
        )

        return Response(
            outcome="success",
            user_id=user_id,
            public_username=email,
            last_login_at=prior_last_login,
            user_facing_message=user_facing,
            google_claims=claims,
        )


TOOL = OAuthLoginTool()
