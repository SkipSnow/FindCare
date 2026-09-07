# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# SharedServices — FastAPI app on port 8002.
#
# Owner: EPIC-002-F-003 (Authorizations and Authentications).
#
# The /gate route is the universal entrance for the application. Every
# request flows through TWO PydanticAI-shaped tools in sequence:
#
#   1. authorizations_and_authentications_tool.run(AuthnDeps)
#         -> user_object (mint-or-restore + persist admin.sessions)
#   2. universal_navigation_tool.run(AgentDeps, NavRequest(op, payload))
#         -> dispatches to the graph node for `op` (splash,
#            record_ux_event, utterance, ...). Emits stream events as it
#            runs; final result is the last NDJSON line on the wire.
#
# Trivial ops (peer_urls, peer_health, session, verify_token,
# transfer_to_findcare) are dispatched inline by /gate without invoking
# the heavy nav-tool graph. They satisfy EPIC-002-F-004-S-001 (universal
# entrance) by keeping all client traffic on /gate.

# Establish which component this process is and what the library will let it
# load, before any other library capability is imported. The finder installed
# here refuses a forbidden module at import, so a late import inside a
# function is caught the same as one at the top of a file -- which only holds
# if nothing has been imported ahead of this call.
from chathealthy_lib.permissions import initialize as _ch_permissions_init
_ch_permissions_init()

import base64
from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
import os
import sys
import tempfile
import time

from fastapi import Body, FastAPI, Form as FormBody, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

log = ChatHealthyLoggingService()


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def _decode_cert_pem(env_var: str, b64: str, component: str) -> bytes:
    """Decode one PEM from base64. Raises, never logs.

    Extracted so bootstrap_certs_from_env can keep its operational logging
    without also being a raising function -- Rule-005 statement 3: the
    thrower does not log, the catcher does.
    """
    try:
        return base64.b64decode(b64.strip())
    except Exception as e:
        raise ChatHealthyException(
            mode="startup_invalid_base64",
            message=f"STARTUP: {env_var} not valid base64: {e}",
            component=component,
            exception=e,
        )


def bootstrap_certs_from_env():
    runtime_dir = os.path.join(tempfile.gettempdir(), "ch_certs")
    mapping = {
        "FINDCARE_CERT_PEM":      "findcare.crt",
        "SHARED_CERT_PEM":        "shared.crt",
        "SHARED_SIGNING_KEY_PEM": "shared.key",
        "CA_CERT_PEM":            "ca.crt",
    }
    wrote = []
    for env_var, filename in mapping.items():
        b64 = os.environ.get(env_var)
        if not b64:
            continue
        pem = _decode_cert_pem(env_var, b64, "SharedServices")
        os.makedirs(runtime_dir, exist_ok=True)
        path = os.path.join(runtime_dir, filename)
        with open(path, "wb") as f:
            f.write(pem)
        try:
            os.chmod(path, 0o600)
        except Exception as _exc:
            # Mode 1 (REQ-B-008): best-effort startup chmod; system continues.
            # log.info + default debug-gated.
            log.info("STARTUP: chmod 0600 on %s failed (continuing): %s", path, _exc, exc=ChatHealthyException(
                                                                                          mode="startup_chmod_failed",
                                                                                          message=f"STARTUP: chmod 0600 on {path} failed (continuing): {_exc}",
                                                                                          component="SharedServices",
                                                                                          exception=_exc,
                                                                                      ), if_not_debug_log=True)
        wrote.append(filename)
    if wrote:
        os.environ["CERTS_DIR"] = runtime_dir
        log.info("startup bootstrap: wrote %s to %s", ",".join(wrote), runtime_dir)



# This service acts as frontendUser, including when it writes its own
# logs. The Mongo log handler refuses to build without an identity, and
# nothing else in this process sets one.
from chathealthy_lib.logging_service import set_mongo_log_identity
set_mongo_log_identity("frontendUser")
bootstrap_certs_from_env()

app = FastAPI(title="ChatHealthy.ai Shared Services", version="0.1.5")

# Every runtime is rebindable, not only the one that happens to read
# versioned collections today. /admin/swap is how a data version is
# activated, and a service that does not expose it cannot be told which
# collection generation to serve -- so a version activation would silently
# cover part of the application and report success. Mounting the router costs
# nothing where no slot is bound: the endpoint answers and the swap is a
# no-op for a target the binding document does not name.
from chathealthy_lib.runtime_data_collections import (  # noqa: E402
    router as data_collections_router,
)

app.include_router(data_collections_router)



@app.exception_handler(ChatHealthyException)
async def _chathealthy_exception_to_response(request, exc: ChatHealthyException):
    """Return the response the raise site asked for.

    Raising ChatHealthyException instead of HTTPException moves the status
    code into the exception's context. This turns it back into the same
    response the client used to receive: same code, same detail body. Any
    other mode is an unhandled fault and answers 500.
    """
    # The boundary logs. Throwers were stripped of their log calls because
    # the rule says the catcher logs, and this is the catcher: without this
    # line a converted failure reaches the client as a status code and
    # leaves no trace anywhere of what happened.
    status = (int(exc.context.get("status_code", 500))
              if exc.mode == "http_error" else 500)
    # exc= takes a constructed ChatHealthyException or one bound by an
    # except clause; a parameter annotated as one is neither, so the facts
    # go in the line itself rather than bending the rule to fit this frame.
    log.error("%s %s -> %s  mode=%s component=%s  %s",
              request.method, request.url.path, status,
              exc.mode, exc.component or "-", exc.message)
    # The mode travels with the response. It is what the panel that has to
    # say what was being attempted selects its wording from, so the wording
    # is decided where the failure happened and not on the client.
    return JSONResponse(status_code=status,
                        content={"detail": exc.message, "mode": exc.mode})

import datetime as dt



@app.exception_handler(Exception)
async def fatal(request: Request, exc: Exception):
    # Safety net for UNHANDLED exceptions per EPIC-008-F-002-S-009-REQ-B-008
    # Mode 3 (unhandled, not expected). Reaching here is always user-fatal
    # (503 to the user) — that IS the Mode 3 definition — so tag fatal_error
    # True. The architectural goal is for Mode 3 occurrences to be RARE; each
    # one observed in the log MUST be moved to a local catch with Mode 1 or
    # Mode 2 handling.
    log.exception("unhandled exception on %s", request.url.path,
                  extra={"fatal_error": True})
    return JSONResponse(
        status_code=503,
        content={"service": "SharedServices", "source": "unhandled",
                 "time": dt.datetime.now(dt.timezone.utc).isoformat()},
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://localhost", "https://localhost:443", "https://localhost:3000",
                   "https://localhost:8080", "https://localhost:8081",
                   "https://chathealthy.ai", "https://dev.chathealthy.ai"],
    allow_origin_regex=r"https://localhost(:\d+)?$|https://[a-zA-Z0-9-]+\.chathealthy\.ai$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Routes ──────────────────────────────────────────────────────────

from healthcheck.health_endpoint import HealthEndpoint
from displayChrome.transfer_to_findcare_endpoint import TransferToFindCareEndpoint
from secretsManager.secrets_endpoint import SecretsEndpoint
from chathealthy_lib.authentication import (
    AuthToken, SessionRestampRequest, SessionToken, VerifyTokenResponse,
)
from chathealthy_lib.authentication.user_object import UserObject
from authentication.mintable_auth_token import MintableAuthToken
from chathealthy_lib.authentication.agent_deps import AuthnDeps
from authentication.google_oauth_endpoint import GoogleOAuthEndpoint

# New architecture: two tools chained inside /gate.
from authentication import (
    authorizations_and_authentications_tool as authn,
    universal_navigation_tool as nav,
)
AUTHN_TOOL = authn.TOOL
UNIVERSAL_NAV_TOOL = nav.TOOL


ORIGIN = "SharedServices"
ENV = os.getenv("ENV_PREFIX", "dev")

# Browser-facing peer URLs the wrapper consumes from /gate (op=peer_urls
# op=peer_urls) so iframe.src and peer-health lookups never
# need build-time substitution into the wrapper bytes. Set by deploy.
CH_BROWSER_PEER_URL_FINDCARE = os.getenv("CH_BROWSER_PEER_URL_FINDCARE", "https://localhost:7860")
CH_BROWSER_PEER_URL_EVALCARE = os.getenv("CH_BROWSER_PEER_URL_EVALCARE", "https://localhost:8001")
# Server-to-server peer URLs SS uses to proxy peer_health.
FINDCARE_INTERNAL_URL_FOR_HEALTH = os.getenv("FINDCARE_INTERNAL_URL", "https://ch-findcare:7860")
EVALCARE_INTERNAL_URL_FOR_HEALTH = os.getenv("EVALCARE_INTERNAL_URL", "https://ch-evalcare:7860")


def impl(cls_name, file_subpath):
    return {
        "x-implementing-class": cls_name,
        "x-implementing-file": f"sharedServices/Code/{file_subpath}",
    }


@app.post("/health", operation_id="HealthEndpoint",
          openapi_extra=impl("HealthEndpoint", "healthcheck/health_endpoint.py"))
def health():
    # v2.2 Part B 7.7 — when the Mongo client is unreachable, return 503
    # (not 200). The Website fetch wrapper at Website/index.html lines
    # 670-688 paints chFatalError on any 503; that turns this endpoint
    # into the visible operator surface that the rotation-as-operational-
    # response model depends on. The JSON body is preserved so the
    # non-prod banner still renders degraded state.
    payload = HealthEndpoint()()
    if payload.get("db") != "connected":
        log.error("/health returning 503 — db not connected; payload=%s",
                  payload, extra={"fatal_error": True})
        return JSONResponse(status_code=503, content=payload)
    return payload


# ─────────────────────────────────────────────────────────────────────
# /gate — the universal entrance. Streams NDJSON.
# ─────────────────────────────────────────────────────────────────────

_TRIVIAL_GATE_OPS = frozenset({
    "peer_urls", "peer_health", "session", "verify_token", "transfer_to_findcare",
})



# Module-level helpers for /gate response instrumentation. Kept out of
# the gate() body so Rule-005 (no log call in a function body that also
# raises ChatHealthyException) does not trip.
async def _gate_instrumented_stream(inner, op_name):
    import json as _json
    bytes_sent = 0
    lines_sent = 0
    kinds: list = []
    buf = b""

    def _ingest_line(line_bytes: bytes) -> None:
        ln = line_bytes.strip()
        if not ln:
            return
        try:
            obj = _json.loads(ln)
            kinds.append(str(obj.get("kind") or "?"))
        except Exception:
            # Mode 1 (REQ-B-008): instrumentation-only parse; on failure
            # we tag the kind as "PARSE_ERR" and the stream continues.
            # Deliberately silent (no log) — this is forensic kind-list
            # accounting, not the canonical error channel for the stream.
            kinds.append("PARSE_ERR")

    try:
        async for chunk in inner:
            if isinstance(chunk, (bytes, bytearray)):
                b = bytes(chunk)
            else:
                b = str(chunk).encode("utf-8")
            bytes_sent += len(b)
            lines_sent += b.count(b"\n")
            buf += b
            parts = buf.split(b"\n")
            buf = parts[-1]
            for ln_bytes in parts[:-1]:
                _ingest_line(ln_bytes)
            yield chunk
        if buf.strip():
            _ingest_line(buf)
        log.info(
            "/gate stream COMPLETE op=%s bytes=%d lines=%d kinds=%s",
            op_name, bytes_sent, lines_sent, kinds,
        )
    except Exception as exc:
        # Mode 2 (REQ-B-008): instrumentation catch — records the partial
        # stream state (op + bytes/lines emitted + kinds seen) for forensic
        # context, then re-raises so the actual exception continues to
        # whatever handles it upstream (a local catch, or the catch-all
        # safety net). The user-affecting outcome is owned by the upstream
        # handler; this catch only adds observability.
        # Pre-existing Rule-005 deviation: no exc=ChatHealthyException
        # wrapping here. Left as-is per scope of REQ-B-008 catch pass —
        # converting this to the canonical exc= shape would change the
        # exception object propagating upstream.
        log.error(
            "/gate stream BROKE op=%s bytes_emitted=%d lines_emitted=%d kinds=%s exc=%s: %s",
            op_name, bytes_sent, lines_sent, kinds,
            type(exc).__name__, exc,
        )
        raise


def _gate_log_ndjson_bytes_complete(op: str, body) -> None:
    byte_len = len(body) if isinstance(body, (bytes, bytearray)) else len(str(body).encode("utf-8"))
    line_count = body.count(b"\n") if isinstance(body, (bytes, bytearray)) else str(body).count("\n")
    log.info(
        "/gate ndjson_bytes COMPLETE op=%s bytes=%d lines=%d",
        op, byte_len, line_count,
    )


def _gate_log_json_complete(op: str) -> None:
    log.info("/gate json COMPLETE op=%s", op)


def _verify_session_or_401(op: str, session_token_dict) -> tuple[SessionToken, str]:
    """Validate the /gate body's session_token and return (SessionToken, GUID).

    Raises HTTPException(401) on missing/invalid/unverified token. Lives
    outside gate() so its log calls are not co-located with gate()'s own
    catch-all ChatHealthyException raise (Rule-005-B-010: the catcher
    logs, not the thrower).
    """
    if not isinstance(session_token_dict, dict) or not session_token_dict:
        raise ChatHealthyException(
            mode="http_error",
            component="app",
            message="session_token is required for non-trivial /gate ops",
            status_code=401)
    try:
        st_in = SessionToken.model_validate(session_token_dict)
        at = AuthToken(st_in, origin=ORIGIN)
        valid = at.verify()
    except (ValueError, TypeError) as _e:
        raise ChatHealthyException(
            mode="http_error",
            component="app",
            message=f"session_token invalid: {_e}",
            status_code=401,
            exception=_e)
    if not valid:
        raise ChatHealthyException(
            mode="http_error",
            component="app",
            message="session_token verification failed",
            status_code=401)
    return st_in, st_in.get_auth_token()


def _log_gate_entry(op: str, intent, body_keys: list) -> None:
    """Emit the /gate entry-log line from outside gate() so gate()'s own
    ChatHealthyException raise does not co-locate with a log call
    (Rule-005-B-010)."""
    log.debug("/gate ENTRY op=%s intent=%r body_keys=%s", op, intent, body_keys)


async def _dispatch_trivial_gate_op(op: str, payload: dict) -> dict:
    """Inline op handlers for /gate trivial ops.

    Returns a plain dict that /gate wraps in JSONResponse. These ops do
    NOT pass through universal_navigation_tool — they are the gateway's
    own short-circuit branches for client work that has no LLM/graph
    component (peer URL lookup, peer health proxy, AuthToken stamping,
    ownership-transfer ack).
    """
    if op == "peer_urls":
        return {
            "findcare":       CH_BROWSER_PEER_URL_FINDCARE,
            "evaluatecare":   CH_BROWSER_PEER_URL_EVALCARE,
            "sharedservices": "",   # the wrapper already knows its own /gate origin
        }
    if op == "peer_health":
        peer = (payload or {}).get("peer", "").lower()
        target_url = {
            "findcare":       FINDCARE_INTERNAL_URL_FOR_HEALTH,
            "evaluatecare":   EVALCARE_INTERNAL_URL_FOR_HEALTH,
            "sharedservices": "self",
        }.get(peer)
        if target_url is None:
            raise ChatHealthyException(
                mode="gate_peer_health_unknown_peer",
                message=f"/gate peer_health: unknown peer {peer!r}",
                component="SharedServices",
            )
        if target_url == "self":
            payload_out = HealthEndpoint()()
            return payload_out
        import httpx
        try:
            async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                r = await client.post(target_url + "/health")
                r.raise_for_status()
                return r.json()
        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError,
                httpx.HTTPStatusError) as exc:
            return {
                "status": "unreachable",
                "service": peer,
                "error": f"{type(exc).__name__}: {exc}",
            }
    if op == "session":
        body_obj = SessionRestampRequest.model_validate(payload or {})
        return AuthToken.handle_session(body_obj, origin=ORIGIN, server_env=ENV).model_dump()
    if op == "verify_token":
        body_obj = SessionRestampRequest.model_validate(payload or {})
        return AuthToken.handle_verify(body_obj, origin=ORIGIN, server_env=ENV).model_dump()
    if op == "transfer_to_findcare":
        return TransferToFindCareEndpoint()()
    raise ChatHealthyException(
        mode="gate_trivial_op_unregistered",
        message=f"_dispatch_trivial_gate_op: op {op!r} is in _TRIVIAL_GATE_OPS but has no handler branch",
        component="SharedServices",
    )


@app.post("/gate", operation_id="UniversalGate",
          openapi_extra=impl(
              "AuthorizationsAndAuthenticationsTool + UniversalNavigationTool",
              "AuthorizationsAndAuthentications/"
              "universal_navigation_tool.py",
          ))
async def gate(
    request: Request,
    body: dict | None = Body(default=None),
):
    """Single entrance for every client call.

    HTTP plumbing only: parse the POST body, hand off to
    UniversalNavigationTool.handle_gate for all orchestration, then
    shape the returned GateResponse into a FastAPI response (Streaming,
    bytes-NDJSON, or JSON).

    Session continuity comes from the body-level `session_token` field
    ClientRouter threads from its in-memory `_sessionToken`. On every
    non-trivial op /gate verifies the token's signature; if verification
    passes, the session GUID is extracted from the verified token to
    hydrate the user_object. Trivial ops (peer_urls, peer_health, etc.)
    remain pre-auth and do not require the token. Cookies are not used —
    HuggingFace Spaces' edge proxy strips Access-Control-Allow-Credentials
    from OPTIONS preflights.
    """
    payload = dict(body or {})
    # A call names its gesture. This defaulted to `boot`, an op that no
    # longer exists: session establishment is /auth/issue and nothing
    # else, so a body with no op is a caller error rather than a boot.
    op = str(payload.get("op") or "")
    op_payload = payload.get("payload") or {}
    intent = payload.get("intent")
    _log_gate_entry(op, intent, sorted(list(payload.keys())))

    accept = (request.headers.get("accept") or "").lower()
    want_ndjson = "application/x-ndjson" in accept or "text/event-stream" in accept

    # Client IP for safety-lockout hydration. X-Forwarded-For wins because
    # Cloudflare and HF proxies put the real client there; bare
    # request.client.host falls back when there's no proxy (local docker).
    xff = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    client_ip = xff or (request.client.host if request.client else "")

    # Trivial ops dispatched inline (EPIC-002-F-004-S-001): no graph
    # invocation, no nonce machinery. peer_urls + peer_health + session +
    # verify_token + transfer_to_findcare all return immediately.
    if op in _TRIVIAL_GATE_OPS:
        body_dict = await _dispatch_trivial_gate_op(op, op_payload)
        return JSONResponse(content=body_dict)

    # Every non-trivial /gate call MUST carry a valid signed SessionToken.
    # /gate is the ONLY session-validation site in the system; downstream
    # services trust this verification and do not re-validate.
    st_in, session_guid = _verify_session_or_401(op, payload.get("session_token"))

    try:
        gate_req = nav.GateRequest(
            op=op,
            payload=op_payload,
            intent=intent,
            session_guid=session_guid,
            want_ndjson=want_ndjson,
            client_ip=client_ip,
        )
        gate_resp = await UNIVERSAL_NAV_TOOL.handle_gate(gate_req)

        if gate_resp.body_kind == "ndjson_stream":
            resp = StreamingResponse(
                _gate_instrumented_stream(gate_resp.body_data, op),
                media_type="application/x-ndjson",
            )
        elif gate_resp.body_kind == "ndjson_bytes":
            _gate_log_ndjson_bytes_complete(op, gate_resp.body_data)
            resp = Response(
                content=gate_resp.body_data, media_type="application/x-ndjson",
            )
        elif gate_resp.body_kind == "file":
            # A download, answered through the one entrance. body_data is
            # {media_type, filename, content}.
            f = gate_resp.body_data
            resp = Response(
                content=f["content"], media_type=f["media_type"],
                headers={"Content-Disposition":
                         f'attachment; filename="{f["filename"]}"'},
            )
        else:  # "json"
            _gate_log_json_complete(op)
            resp = JSONResponse(content=gate_resp.body_data)

        return resp
    except ChatHealthyException:
        # The failure was named where it happened. Wrapping it here would
        # replace that name with this one, and the panel that has to say
        # what was being attempted reads the name.
        raise
    except Exception as exc:
        # REQ-T-009: any /gate exception MUST log the full stack with the
        # originating request shape. The browser sees HTTP 500 and renders
        # its hard-fail page.
        raise ChatHealthyException(
            mode="gate_failed",
            message=f"/gate failed: method={request.method} path={request.url.path} op={op} intent={intent!r}: {type(exc).__name__}: {exc}",
            component="SharedServices",
            exception=exc,
        )


# ─────────────────────────────────────────────────────────────────────
# Auxiliary routes (OAuth + secrets — out of scope of /gate; OAuth needs
# top-level navigation; secrets are admin-only). Everything else is on
# /gate per EPIC-002-F-004-S-001.
# ─────────────────────────────────────────────────────────────────────

@app.post("/auth/issue", operation_id="AuthIssue", response_model=SessionToken,
          openapi_extra=impl("MintableAuthToken", "authentication/mintable_auth_token.py"))
async def auth_issue(request: Request):
    """Establish a session and hand back its token.

    This is the one unauthenticated door in the system, and the whole of
    session establishment: the session exists when this returns. It used
    to mint a token and nothing else, leaving the session to be created
    by a `boot` call that followed it on every page load -- two round
    trips where the second existed only because the first had made a GUID
    with nothing behind it.

    The page passes back the GUID it holds. A GUID naming a session that
    is in Mongo and has not expired is resumed; anything else is ignored
    and a new session begins, so a GUID a caller invents buys nothing.

    The form factor is told to the session here because here is where the
    session is made, and it does not change while the session lives.
    """
    try:
        body = await request.json()
    except Exception:  # noqa: BLE001 - an unparsable body is simply no guid
        body = {}
    offered = str((body or {}).get("session_guid") or "").strip()
    resumed = _live_session_guid(offered) if offered else ""
    if resumed:
        # A session we already have is not authorised again: it is
        # stamped. The GUID is per session and the nonce is per hop, so
        # the token is minted fresh against the session that exists --
        # handing back the stored one would replay a nonce, and building
        # a second session would orphan the first, which is the waste
        # this endpoint was meant to end.
        return MintableAuthToken.manufacture(
            server_env=ENV, guid=resumed).to_wire()

    reported = str((body or {}).get("form_factor") or "").strip().lower()
    deps = AuthnDeps(session_guid="", server_env=ENV,
                           mongo_frontend=authn.get_mongo_frontend())
    user_object = UserObject(
        current_session_token="NULL",
        expires_at=dt.datetime.now(dt.timezone.utc)
        + dt.timedelta(seconds=authn.SESSION_TTL_SECONDS),
    )
    if reported in ("phone", "desktop"):
        user_object.form_factor = reported
    resp = await authn.TOOL.run(
        deps, authn.Request(intent="manufacture_session", user_object=user_object))
    await authn.TOOL.persist(deps, resp.user_object, resp.fresh_mint)
    return resp.user_object.current_session_token.model_dump(mode="json")


def _live_session_guid(guid: str) -> str:
    """The guid, if it names a session that exists and has not expired.

    Returns "" otherwise, and the caller mints. A session this cannot read
    is not resumed: continuing on a GUID whose session is unknown would hand
    the caller a token for state nobody can produce.
    """
    from datetime import datetime, timezone as _tz
    try:
        coll = authn.get_mongo_frontend()[authn.SESSION_DB][authn.SESSION_COLLECTION]
        doc = coll.find_one({"_id": guid}, {"expires_at": 1})
    except Exception as exc:  # noqa: BLE001 - unreadable session, mint instead
        log.info("auth/issue could not read session %s: %s", guid[:8], exc)
        return ""
    if not doc:
        return ""
    expires = doc.get("expires_at")
    if isinstance(expires, str):
        try:
            expires = datetime.fromisoformat(expires.replace("Z", "+00:00"))
        except ValueError:
            return ""
    if isinstance(expires, datetime):
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=_tz.utc)
        if expires <= datetime.now(_tz.utc):
            return ""
    return guid


@app.get("/secrets/{key}", operation_id="SecretsEndpoint",
         openapi_extra=impl("SecretsEndpoint", "secretsManager/secrets_endpoint.py"))
def get_secret(key: str):
    return SecretsEndpoint()(key)


@app.post("/auth/google/start", operation_id="GoogleOAuthStart",
          openapi_extra=impl("GoogleOAuthEndpoint", "authentication/google_oauth_endpoint.py"))
async def google_oauth_start(
    session_guid: str | None = FormBody(default=None),
    flow: str = FormBody(default="login"),
):
    return await GoogleOAuthEndpoint.start(
        server_env=ENV, session_guid=session_guid, flow=flow,
    )


@app.get("/auth/google/callback", operation_id="GoogleOAuthCallback",
         openapi_extra=impl("GoogleOAuthEndpoint", "authentication/google_oauth_endpoint.py"))
async def google_oauth_callback(
    code: str = None, state: str = None, error: str = None,
):
    # session_guid is recovered from the HMAC-signed OAuth state parameter
    # (see GoogleOAuthEndpoint.build_state / verify_state). No cookies used.
    return await GoogleOAuthEndpoint.callback(
        code=code, state=state, server_env=ENV, error=error,
    )


# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8002"))
    log.info("SharedServices starting on port %d", port)
    kwargs = {"host": "0.0.0.0", "port": port}
    ssl_cert = os.getenv("SSL_CERTFILE")
    ssl_key = os.getenv("SSL_KEYFILE")
    if ssl_cert and ssl_key and os.path.exists(ssl_cert) and os.path.exists(ssl_key):
        kwargs["ssl_certfile"] = ssl_cert
        kwargs["ssl_keyfile"] = ssl_key
    uvicorn.run(app, **kwargs)
