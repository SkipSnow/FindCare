# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""AuthorizationsAndAuthentications tool — pure session-data work.

The tool exposes the canonical *_tool.py contract (TOOL_NAME, Request,
Response, run()) plus a persist() method.

run() is a three-way switch on `request.intent`:

  * "manufacture_session" — mint a fresh SessionToken, stamp it onto
    the incoming user_object's current_session_token field (which
    arrived as the sentinel "NULL"), bump expires_at, return.

  * "manage_session" — the incoming user_object carries a real
    SessionToken (the gateway loaded it from Users.sessions). Restamp
    the nonce in place, bump expires_at, return.

  * "login" — the incoming user_object carries a real SessionToken
    AND an OAuthIdentities[0] entry populated by the OAuth callback.
    Register-or-merge the users record in Users.users, mirror the
    resulting user_object back into Users.sessions, return.

The tool has no knowledge of HTTP, URLs, or callers. It does not
dispatch on cookie state, request shape, or any input outside its
typed Request. The gateway decides the intent; the tool executes it.
"""
from __future__ import annotations

from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

from chathealthy_lib.authentication.agent_deps import AuthnDeps
from chathealthy_lib.authentication.chathealthy_tool import ChatHealthyTool
from authentication.mintable_auth_token import MintableAuthToken
from chathealthy_lib.authentication.user_object import UserObject
from chathealthy_lib.authentication.session_token import SessionToken

log = ChatHealthyLoggingService()


ORIGIN = "SharedServices"
SESSION_TTL_SECONDS = 300
SESSION_DB = "Users"
SESSION_COLLECTION = "sessions"
USERS_DB = "Users"
USERS_COLLECTION = "users"

indexes_ensured: bool = False


class Request(BaseModel):
    """The auth tool's input. The gateway picks `intent` from the three
    permitted operations and hands a `user_object` populated to the
    degree appropriate for that operation:

      * manufacture_session: user_object.current_session_token == "NULL"
        (sentinel — no session exists yet). The branch mints the token.
      * manage_session: user_object.current_session_token is a real
        SessionToken loaded from Users.sessions by the gateway.
      * login: user_object.current_session_token is a real SessionToken
        AND user_object.OAuthIdentities[0] is the newly asserted
        identity from the OAuth callback.
    """
    intent: Literal["manufacture_session", "manage_session"] = Field(
        description="manufacture_session mints a new session; manage_session "
                    "carries an existing one forward.")
    user_object: UserObject = Field(
        description="The session being minted or carried.")


class Response(BaseModel):
    user_object: UserObject
    fresh_mint: bool


def auth_token_to_session_token(auth_token: Any) -> SessionToken:
    if isinstance(auth_token, SessionToken):
        return auth_token
    to_wire = getattr(auth_token, "to_wire", None)
    if callable(to_wire):
        wired = to_wire()
        if isinstance(wired, SessionToken):
            return wired
    raise ChatHealthyException(
        mode="security_violation",
        component="AuthorizationsAndAuthentications",
        message=(
            f"MintableAuthToken.manufacture returned {type(auth_token).__name__!r}; "
            "no SessionToken accessible via to_wire()"
        ),
    )


def ensure_indexes(coll) -> None:
    global indexes_ensured
    if indexes_ensured:
        return
    coll.create_index("expires_at", expireAfterSeconds=0, name="session_ttl_idx")
    indexes_ensured = True


def reload(coll, guid: str) -> Optional[UserObject]:
    doc = coll.find_one({"_id": guid})
    if not doc:
        return None
    try:
        return UserObject.model_validate({k: v for k, v in doc.items() if k != "_id"})
    except Exception as exc:
        log.warning("could not reconstitute UserObject for %s: %s", guid[:8], exc, exc=ChatHealthyException(
                                                                                    mode="user_object_reload_failed",
                                                                                    message=f"could not reconstitute UserObject for {guid[:8]}: {exc}",
                                                                                    component="AuthorizationsAndAuthentications",
                                                                                    exception=exc,
                                                                                ), if_not_debug_log=True)
        return None


def manufacture_session(user_object: UserObject, server_env: str) -> UserObject:
    """Mint a SessionToken in place. The incoming user_object's
    current_session_token is the sentinel "NULL"; replace it with a
    real SessionToken and set expires_at to NOW + TTL.

    Action #1 of every new session is the on_load marker, appended
    here so every session has a deterministic anchor in the actions
    bucket (Skip 2026-06-10)."""
    from chathealthy_lib.authentication.agent_deps import append_action
    now = datetime.now(timezone.utc)
    minted = MintableAuthToken.manufacture(server_env=server_env)
    user_object.current_session_token = auth_token_to_session_token(minted)
    user_object.expires_at = now + timedelta(seconds=SESSION_TTL_SECONDS)
    append_action(
        user_object,
        tool_name="on_load",
        input_json={"server_env": server_env},
        output_json={"guid": user_object.current_session_token.get_auth_token()},
    )
    return user_object


def manage_session(user_object: UserObject) -> UserObject:
    """Restamp the nonce on the existing SessionToken, bump expires_at.
    The incoming user_object carries a real SessionToken — the gateway
    loaded it from Users.sessions."""
    now = datetime.now(timezone.utc)
    if not isinstance(user_object.current_session_token, SessionToken):
        raise ChatHealthyException(
            mode="security_violation",
            component="AuthorizationsAndAuthentications",
            message=(
                "manage_session requires a real SessionToken on the incoming "
                "user_object; got sentinel/None."
            ),
        )
    user_object.current_session_token.put_nonce(ORIGIN)
    user_object.expires_at = now + timedelta(seconds=SESSION_TTL_SECONDS)
    return user_object


def user_id_for_guid(users_coll, guid: str) -> Optional[str]:
    """Return user_id whose stored user_object holds this session's GUID."""
    doc = users_coll.find_one(
        {"user_object.current_session_token.auth_token": guid},
        {"user_id": 1},
    )
    return doc["user_id"] if doc else None


def write_session_record(coll, users_coll, user_object: UserObject, fresh_mint: bool) -> None:
    """Persist the session, and mirror to Users.users when registered.

    Not the only writer, and it carries no parameter. Every server writes
    the parameters it owns when it writes them -- FindCare straight to the
    page it mined, this process through user_parameters_tool -- so the
    parameters in the document are always the newest, and a write-back of
    this process's copy could only be older. What this writes is the rest
    of the session: the token, the conversation, the intent, the selections.

    It sets rather than replaces for the same reason: a replace would carry
    this copy's view of everything over whatever another server wrote while
    the turn ran.

    A fresh mint is the exception and still replaces: there is no document
    to preserve and nothing else has written one.
    """
    ensure_indexes(coll)
    guid = user_object.current_session_token.get_auth_token()
    body = user_object.model_dump(mode="json", exclude_none=True)
    if fresh_mint:
        coll.replace_one(
            {"_id": guid},
            {"_id": guid, **body},
            upsert=True,
        )
    else:
        body.pop("userParameters", None)
        coll.update_one({"_id": guid}, {"$set": body}, upsert=True)

    # Mirror to users collection when is_registered == True (REQ-T-010).
    if user_object.is_registered is True:
        user_id = user_id_for_guid(users_coll, guid)
        if user_id:
            users_coll.update_one(
                {"user_id": user_id},
                {"$set": {"user_object": body}},
            )


class AuthorizationsAndAuthenticationsTool(ChatHealthyTool):
    """Pure session-data work. Three operations on `user_object`:

      * `run(Request(intent="manufacture_session", user_object=...))`
        Mint a SessionToken in place. user_object.current_session_token
        arrives as the sentinel "NULL"; leaves as a real SessionToken.

      * `run(Request(intent="manage_session", user_object=...))`
        Restamp the nonce + bump expires_at. Gateway already loaded the
        session record from Users.sessions and hydrated user_object.

      * `run(Request(intent="login", user_object=...))`
        Register-or-merge the OAuth identity carried on
        user_object.OAuthIdentities[0]. Mirror result into
        Users.sessions AND Users.users.

    `persist()` writes this process's user_object back to Users.sessions
    (and mirrors to Users.users when is_registered). It is not the only
    writer of that document and it carries no parameter: every server
    writes the parameters it owns as it writes them, so the write-back
    sets the rest of the session and never a page.
    """
    TOOL_NAME = "authn"
    Request = Request
    Response = Response

    async def run(self, deps: AuthnDeps, request: "Request") -> "Response":
        if request.intent == "manufacture_session":
            user_object = manufacture_session(request.user_object, deps.server_env)
            return self.Response(user_object=user_object, fresh_mint=True)
        # intent == "manage_session"
        user_object = manage_session(request.user_object)
        return self.Response(user_object=user_object, fresh_mint=False)

    async def persist(self, deps: AuthnDeps, user_object, fresh_mint: bool) -> None:
        coll = deps.mongo_frontend[SESSION_DB][SESSION_COLLECTION]
        users_coll = deps.mongo_frontend[USERS_DB][USERS_COLLECTION]
        write_session_record(coll, users_coll, user_object, fresh_mint)


TOOL = AuthorizationsAndAuthenticationsTool()


def get_mongo_frontend():
    """Front-end cluster client (Users.sessions + Users.users) via the canonical utility."""
    from chathealthy_lib.mongo_utilities import ChatHealthyMongoUtilities
    return ChatHealthyMongoUtilities().getConnection("frontendUser", "ChatHealthyFrontEnd")
