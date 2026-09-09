# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""UniversalNavigation tool — the graph orchestrator.

Runs after AuthorizationsAndAuthentications has established the user
(deps.user_object). Dispatches by op to a graph node handler:

  * session_data      — render the session: identity, live parameters,
                        and the utterance/action history
  * record_ux_event   — append a UX-control event to ux_events[]
  * utterance         — capture typed text, classify it, and hand the
                        turn to the page the classification named, which
                        reads the utterance itself (streams events
                        progressively to the FE)
  * peer_urls, peer_health, session, verify_token,
    transfer_to_findcare
                      — where a peer is, whether it is answering, a fresh
                        stamp on a token, and the handing back of page
                        ownership. The HTTP route used to answer these
                        itself, which left the door deciding what a peer is
                        and when a token is valid; they are ordinary ops
                        here and reach the client behind the same session
                        verification as everything else.

Every handler reads its input off deps.user_object (the working memory)
and emits its result via deps.stream(...). The dispatcher returns a
NavResult carrying the final event; the gate route persists the
mutated user_object back to Users.sessions after the run.

Canonical *_tool.py exports: TOOL_NAME, Request, Response, run().

UR is a class: orchestration logic lives as methods on
UniversalNavigationTool so each component has its own encapsulation.
Pure utility functions (session-token assembly, session-data shape,
small read-only helpers) stay module-level since they hold no
orchestration state and are referenced from non-orchestration code
paths.
"""
from __future__ import annotations

import asyncio
import json as json
from chathealthy_lib import ChatHealthyLoggingService
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Literal, Optional, Union

from pydantic import BaseModel, Field

from chathealthy_lib.authentication.agent_deps import (
    AgentDeps,
    AuthnDeps,
    append_action,
)
from chathealthy_lib.authentication.chathealthy_tool import ChatHealthyTool
from authentication import (
    authorizations_and_authentications_tool as authn,
    evalcare_splash_tool,
    provider_detail_tool,
)
from chathealthy_lib.authentication.auth_token import (
    AuthToken,
    SessionRestampRequest,
)
from chathealthy_lib.authentication.nonce import Nonce
from chathealthy_lib.authentication.session_token import SessionToken
from chathealthy_lib.authentication.user_object import UserObject
from chathealthy_lib import ChatHealthyException
from UtteranceManager import utterance_manager
import sys as _ch_sys, pathlib as _ch_pl  # noqa: E402
for _ch_d in _ch_pl.Path(__file__).resolve().parents:
    if (_ch_d / '.git').exists():
        _ch_lib = _ch_d / 'ChatHealthyLib' / 'src'
        if str(_ch_lib) not in _ch_sys.path:
            _ch_sys.path.insert(0, str(_ch_lib))
        break
from chathealthy_lib.exceptions import ChatHealthyException  # noqa: E402

log = ChatHealthyLoggingService()

TOOL_NAME = "universal_navigation"

# ────────────────────────────────────────────────────────────────────
# Wire-level constants — gateway-level concerns that live with the
# orchestrator since /gate is now thin HTTP plumbing only.
# ────────────────────────────────────────────────────────────────────

ENV = os.getenv("ENV_PREFIX", "dev")
SESSION_TTL_SECONDS = 300

# A peer has two addresses and they are not interchangeable: the one a
# browser can reach and the one this process reaches it by. Both are named
# here so neither is derived from the other. Stamped by the deploy, which is
# why the wrapper can be served as static bytes with no substitution in them.
CH_BROWSER_PEER_URL_FINDCARE = os.getenv("CH_BROWSER_PEER_URL_FINDCARE", "https://localhost:7860")
CH_BROWSER_PEER_URL_EVALCARE = os.getenv("CH_BROWSER_PEER_URL_EVALCARE", "https://localhost:8001")
FINDCARE_INTERNAL_URL_FOR_HEALTH = os.getenv("FINDCARE_INTERNAL_URL", "https://ch-findcare:7860")
EVALCARE_INTERNAL_URL_FOR_HEALTH = os.getenv("EVALCARE_INTERNAL_URL", "https://ch-evalcare:7860")

WIRE_INTENT_UTTERANCE = "utterance"
KNOWN_WIRE_INTENTS = frozenset({WIRE_INTENT_UTTERANCE})
# login_register removed from /gate per S-004 rewire: the Login &
# Registration nav button is now a form-target popup posting directly
# to /auth/google/start, which routes through OAuthLoginTool (start
# phase). /gate is for utterance traffic only.

ENV_TO_SHARED_URL = {
    "dev":   "https://dev-hf.chathealthy.ai",
    "qa":    "https://skipsnow-qa-sharedservicesspace.hf.space",
    "prod":  "https://skipsnow-sharedservicesspace.hf.space",
    "local": "https://localhost:8002",
}

MAX_DISPATCH_HOPS = 3


# ────────────────────────────────────────────────────────────────────
# Request / Response contracts
# ────────────────────────────────────────────────────────────────────


def _ch_exc():
    """ChatHealthyException without assuming the library is installed.
    These modules run as bare scripts in the devops chain."""
    import sys as _s, pathlib as _p
    for _d in _p.Path(__file__).resolve().parents:
        if (_d / ".git").exists():
            _l = _d / "ChatHealthyLib" / "src"
            if str(_l) not in _s.path:
                _s.path.insert(0, str(_l))
            break
    from chathealthy_lib.exceptions import ChatHealthyException
    return ChatHealthyException


class Request(BaseModel):
    """Op + opaque payload. The router picks a handler by `op`."""
    op: str = Field(
        description="Which gesture the client made. Every browser call "
                    "arrives here and names one; the router dispatches the "
                    "tool that owns it. Required: a call that names no "
                    "gesture used to default to `boot`, and once boot was "
                    "gone that default dispatched nowhere.")
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="The gesture's own arguments, shaped by the op.")


class Response(BaseModel):
    kind: str
    result: dict[str, Any] = Field(default_factory=dict)


# ────────────────────────────────────────────────────────────────────
# Gate-level Request / Response — what /gate hands the orchestrator
# and what comes back. /gate only does HTTP parsing + response shaping.
# ────────────────────────────────────────────────────────────────────


@dataclass
class GateRequest:
    op: str
    payload: dict[str, Any] = field(default_factory=dict)
    intent: Optional[str] = None
    session_guid: Optional[str] = None
    want_ndjson: bool = False
    # Pulled by /gate from X-Forwarded-For (preferred — Cloudflare and HF
    # proxies populate it with the real client) or scope.client.host. UR
    # stamps it onto user_object.ip_address before any tool dispatch so
    # SafetyLockoutTool reads it from session state, not from the HTTP
    # layer.
    client_ip: Optional[str] = None


# Ops whose answer is a file. They are answered inside the one entrance;
# a download is not a reason to open a second.
FILE_OPS = frozenset({"session_pdf"})


@dataclass
class GateResponse:
    """What the orchestrator returns to /gate.

    body_kind == 'ndjson_stream' → body_data is an async iterator of
    bytes; /gate returns StreamingResponse(body_data).

    body_kind == 'ndjson_bytes' → body_data is a single bytes blob (e.g.,
    the redirect event serialized as one NDJSON line); /gate returns
    Response(content=body_data, media_type='application/x-ndjson').

    body_kind == 'json' → body_data is a dict; /gate returns it as JSON.

    body_kind == 'file' → body_data is {media_type, filename, content};
    /gate returns the bytes as a download. A download is not a reason to
    open a second entrance.

    Session continuity is carried client-side: ClientRouter holds the
    session GUID in JavaScript memory and threads it as body.session_guid
    on every /gate POST. No HTTP cookie is set or read.
    """
    body_kind: Literal["ndjson_stream", "ndjson_bytes", "json", "file"]
    body_data: Any


# ────────────────────────────────────────────────────────────────────
# Pure utility helpers (no orchestration state; referenced from /gate
# and from UR methods). Kept module-level so non-orchestration callers
# (login-register short-circuit, final-event projection) can use them
# without instantiating UR.
# ────────────────────────────────────────────────────────────────────


def time_remaining_seconds(user_object: UserObject) -> int:
    """REQ-T-004: floor((most_recent_restamp + 300s) - now) in seconds.
    Clipped to non-negative integers."""
    nonce_field = user_object.current_session_token.get_nonce()
    latest = nonce_field[18:]
    try:
        secs = datetime.strptime(latest[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        ms = int(latest[14:17])
        restamp_ms = int(secs.timestamp() * 1000) + ms
    except Exception as _exc:
        # Mode 2 (REQ-B-008): nonce parse failure. Per operator (Skip 2026-
        # 06-22): set time remaining back to FULL (300s) AND demote the
        # user to guest if currently registered — corrupt nonce must not
        # silently expire a logged-in user. Mode 2 because demoting a
        # registered user is user-affecting; operator must always see it.
        log.error("nonce parse failed: %s", _exc, exc=ChatHealthyException(
                                                     mode="nonce_parse_failed",
                                                     message=f"nonce parse failed (resetting to full guest session): {_exc}",
                                                     component="UniversalNavigationTool",
                                                     exception=_exc,
                                                 ), if_not_debug_log=True)
        if getattr(user_object, "is_registered", False):
            user_object.is_registered = False
        return 300
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    remaining_ms = (restamp_ms + 300_000) - now_ms
    return max(0, remaining_ms // 1000)


def was_registered(user_object: UserObject) -> bool:
    """REQ-T-010: tiny derived flag for the browser's timeout copy."""
    return bool(getattr(user_object, "is_registered", False))


def session_token_wire(user_object: UserObject) -> dict:
    """Cryptographic-display projection of the session token.

    Surfaces the bare SessionToken — the three display fields the
    panels render (signed token, nonce, GUID) plus the SessionToken
    envelope (origin, signature, created_at, signed, server_env,
    last_used) needed by the existing /session + /verify-token chain.
    """
    st = user_object.current_session_token
    if hasattr(st, "model_dump"):
        return st.model_dump(mode="python", exclude_none=False)
    return dict(st)


# The servers the application is made of, named as components. A target id
# names where something is deployed, not what it is, and an operator reading
# the session wants the component. Each is asked what build it carries; that
# is the whole fact.
COMPONENTS = (
    ("FindCare Server", "FINDCARE_INTERNAL_URL", "https://ch-findcare:7860"),
    ("EvaluateCare Server", "EVALCARE_INTERNAL_URL", "https://ch-evalcare:7860"),
    ("SharedServices Server", "", ""),
)


async def _build_of(env_var: str, default: str) -> str:
    """What one server says it is running, asked directly.

    Blank when it cannot say. A server that does not answer is a fact worth
    seeing, and inventing a number for it would hide exactly the case this
    section exists for.
    """
    # A build cannot change inside a process's lifetime: a new build is a
    # new container and therefore a new process. So the answer is asked
    # for once and held, and a session opened a second time costs no
    # call at all. Asking per session put three requests on the peers
    # every time anyone looked, and Hugging Face answered 429 Too Many
    # Requests -- which read on screen as every server refusing to say
    # what it was running.
    #
    # Only an answer is held. A peer that was mid-restart when it was
    # first asked would otherwise read as unreachable for the life of
    # this process, which is the opposite of what this section is for.
    cached = _BUILD_ANSWERS.get(env_var or "")
    if cached:
        return cached
    # A refusal is remembered too, briefly. Retrying a peer that has just
    # said "too many requests" on every session is what turns a burst into
    # a wall: the probe becomes the load. The refusal is held long enough
    # for the far side to recover and then asked again, so a peer that
    # comes back is picked up without anyone restarting anything.
    import time as _time
    refused_at = _BUILD_REFUSALS.get(env_var or "")
    if refused_at and (_time.monotonic() - refused_at) < _REFUSAL_HELD_SECONDS:
        return "did not answer (waiting before asking again)"
    if not env_var:
        from buildIdentity.build_identity import build_number  # noqa: PLC0415
        # A component that cannot say which build it carries names the
        # reason. Blank is not an answer: a server mid-restart and a server
        # running an image with no build identity in it looked identical,
        # and both read as the feature being broken.
        answer = build_number() or "image carries no build identity"
        _BUILD_ANSWERS[""] = answer
        return answer
    url = os.environ.get(env_var) or default
    import httpx  # noqa: PLC0415
    try:
        async with httpx.AsyncClient(verify=False, timeout=10.0) as client:
            resp = await client.post(url + "/health")
            resp.raise_for_status()
            answer = str(resp.json().get("build") or "")
            if answer:
                _BUILD_ANSWERS[env_var] = answer
            return answer
    except Exception as exc:  # noqa: BLE001 - an unreachable server says so
        import time as _time
        _BUILD_REFUSALS[env_var] = _time.monotonic()
        log.info("build unknown for %s: %s", env_var, exc)
        return f"did not answer ({type(exc).__name__})"


# component -> the build it answered with. Held for the life of the
# process; see _build_of.
_BUILD_ANSWERS: dict[str, str] = {}

# component -> when it last refused. Held for this long before asking again.
_BUILD_REFUSALS: dict[str, float] = {}
_REFUSAL_HELD_SECONDS = 300.0


async def deployment_facts() -> list[dict]:
    """Each component and the build it is running. Asked once per session."""
    builds = await asyncio.gather(
        *[_build_of(var, default) for _name, var, default in COMPONENTS])
    return [{"component": name, "build": build}
            for (name, _v, _d), build in zip(COMPONENTS, builds)]


def session_data(user_object) -> dict[str, Any]:
    """The session, as a read-only projection of the user object.

    Not splash data. The splash is one surface that renders it; the
    content is the session -- who the user is, what is true for them
    now, and what has happened -- and naming it for the page that
    happens to draw it invites a second copy the day something else
    needs the same facts.
    """
    cst = user_object.current_session_token
    oauth_idents = [
        oi.model_dump(mode="json") if hasattr(oi, "model_dump") else oi
        for oi in (getattr(user_object, "OAuthIdentities", []) or [])
    ]
    identity = {
        "user_type": getattr(user_object, "user_type", None) or "Guest",
        "is_registered": bool(getattr(user_object, "is_registered", False)),
        "public_username": getattr(user_object, "public_username", None) or "",
        "OAuthIdentities": oauth_idents,
        "guid": cst.get_auth_token(),
        "origin": cst.origin,
        "server_env": cst.server_env,
        "created_at": str(cst.created_at) if cst.created_at is not None else "",
        "expires_at": str(user_object.expires_at) if user_object.expires_at is not None else "",
    }
    sch = user_object.session_conversation_history.model_dump()
    utterances = sch.get("utterances") if isinstance(sch, dict) else []
    actions = sch.get("actions") if isinstance(sch, dict) else []

    # The live parameter set, in its own right rather than inferred from the
    # actions beside it. A parameter that can be changed at will and cannot
    # be seen is the same trap as a hidden cache: the user gets results
    # narrowed by a geography they no longer remember giving. The page
    # already shows what happened; this is what is true now.
    params = user_object.userParameters
    return {
        "identity": identity,
        "parameters": params.model_dump(mode="json") if params else {},
        "threads": {
            "empty": not utterances and not actions,
            "utterances": utterances or [],
            "actions": actions or [],
        },
    }


def any_pending_disambiguation(document) -> bool:
    """True when any intent entry on the document carries a
    pending_disambiguation marker. UR uses this to suppress closeConnection200
    chaining (REQ-B-004) so the connection remains logically open across the
    user's next turn. No document means nothing is pending."""
    for entry in getattr(document, "intents", None) or []:
        if getattr(entry, "pending_disambiguation", None) is not None:
            return True
    return False


def read_nucc_codes_query(document) -> str:
    """The query the cached nucc_codes were computed from, or "".

    Codes without the question they answer cannot be reused safely, so an
    entry carrying no key never matches and the filter re-runs.
    """
    for name in ("specialtySearch", "findAProvider"):
        entry = next((i for i in document.intents if i.name == name), None)
        if entry is None:
            continue
        for arg in entry.arguments:
            if arg.name == "nucc_codes_query" and arg.value:
                return str(arg.value)
    return ""


def read_nucc_codes_cache(document) -> Optional[list[dict]]:
    """Return the parsed nucc_codes list cached on any specialty/find
    intent entry, or None if no cache hit. Prefers specialtySearch's
    cache (the first place SpecialtyFilter writes to today)."""
    import json as json

    for name in ("specialtySearch", "findAProvider"):
        entry = next((i for i in document.intents if i.name == name), None)
        if entry is None:
            continue
        for arg in entry.arguments:
            if arg.name == "nucc_codes" and arg.value:
                try:
                    parsed = json.loads(arg.value)
                except json.JSONDecodeError as _exc:
                    # Mode 1 (REQ-B-008): intent arg JSON malformed; UR skips
                    # the arg and continues. log.info + default debug-gated.
                    log.info("intent arg JSON decode failed (skipped): %s", _exc, exc=ChatHealthyException(
                                                                                      mode="intent_arg_json_decode_failed",
                                                                                      message=f"intent arg JSON decode failed (skipped): {_exc}",
                                                                                      component="UniversalNavigationTool",
                                                                                      exception=_exc,
                                                                                  ), if_not_debug_log=True)
                    continue
                if isinstance(parsed, list) and parsed:
                    return parsed
    return None


# ────────────────────────────────────────────────────────────────────
# Tool class — single dispatch entry point for the gate. UR's
# orchestration helpers live as methods so each component is
# self-contained and the class is the unit of testing and extension.
# ────────────────────────────────────────────────────────────────────


# The four pages, named where the router dispatches to them. A parameter
# is addressed as a page and an attribute together; there is no default
# page, because a default page is the flat model with a longer spelling.
FACILITY = "facility"
INDIVIDUAL_PROVIDER = "individualProvider"
NUCC = "NUCC"
CLINICAL_TRIAL = "clinicalTrial"


def latest_utterance_and_prior_dialogue(user_object) -> tuple[str, list[dict]]:
    """The utterance a page is to read, and the talk before it.

    A page mines from these two things, so the gateway's whole part in a
    mining turn is producing them: it takes the last thing the person
    said, and hands over everything said before it as context. It reads
    no parameter and forms no opinion about what the words mean.
    """
    dialogue: list[dict[str, str]] = []
    for u in user_object.session_conversation_history.utterances:
        actor = getattr(u, "actor", None) or (u.get("actor") if isinstance(u, dict) else None)
        text = getattr(u, "text", None) or (u.get("text") if isinstance(u, dict) else "")
        if actor not in ("person", "system") or not text:
            continue
        dialogue.append({"actor": actor, "text": str(text).strip()})
    utterance = ""
    for position in range(len(dialogue) - 1, -1, -1):
        if dialogue[position]["actor"] == "person":
            utterance = dialogue[position]["text"]
            dialogue = dialogue[:position]
            break
    return utterance, dialogue


async def post_to_findcare(page_route: str, deps: AgentDeps, mode: str) -> dict:
    """Hand one turn's utterance and prior talk to the page that owns it.

    One implementation because the gateway's side of every mining page is
    the same: post the two things, raise if the page could not be reached,
    and give back what it said. The mode names which page could not be
    reached, so a failure says which search did not run.
    """
    import httpx
    from authentication import provider_search_tool

    utterance, dialogue = latest_utterance_and_prior_dialogue(deps.user_object)
    url = provider_search_tool.findcare_url() + page_route
    try:
        async with httpx.AsyncClient(timeout=None, verify=False) as client:
            r = await client.post(url, json={
                # The token this hop already holds, forwarded so FindCare
                # can verify the SharedServices signature.
                "session_token": deps.session_token.model_dump(mode="json"),
                "utterance": utterance,
                "history": dialogue,
            })
            r.raise_for_status()
            return r.json()
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
            httpx.WriteTimeout, httpx.PoolTimeout, httpx.ReadError,
            httpx.WriteError, httpx.RemoteProtocolError,
            httpx.HTTPStatusError) as exc:
        raise ChatHealthyException(
            mode=mode,
            component="universal_navigation_tool",
            message=f"FindCare {page_route} call failed: "
                    f"{type(exc).__name__}: {exc}",
            exception=exc,
        )


def geography_of(params, page: str):
    """The geography in force on one page, as its model.

    Values are held as plain shapes so a session round-trips through Mongo
    unchanged; the model is rebuilt where it is read.
    """
    from chathealthy_lib.authentication.user_parameters import Geography
    if not params:
        return None
    # A place is four parameters, each declared and each writable on its
    # own, because only one of them -- the state -- is required. Assembled
    # here into the shape a search takes.
    parts = {part: params.get(page, part) or ""
             for part in ("state", "city", "county", "zip")}
    if not any(parts.values()):
        return None
    return Geography(**parts)


def provider_name_of(params):
    from chathealthy_lib.authentication.user_parameters import ProviderName
    got = params.get(INDIVIDUAL_PROVIDER, "providerName") if params else None
    if not got:
        return None
    return got if isinstance(got, ProviderName) else ProviderName(**dict(got))


def codes_in_force(params) -> list[str]:
    """The codes the individual-provider search should run under.

    No selection means the person has not narrowed, so the whole offered
    set applies. This is a rule about one page, which is why it lives here
    rather than on the container that holds every page.
    """
    if params is None:
        return []
    chosen = params.get(INDIVIDUAL_PROVIDER, "selectedSpecialtyCodes") or []
    if chosen:
        return [str(c) for c in chosen]
    offered = params.get(NUCC, "offeredSpecialties") or []
    return [s.get("code") for s in offered
            if isinstance(s, dict) and s.get("code")]


# Which page each dispatched action produces an answer on. Carry-over is
# applied on dispatch to a page and nowhere else.
_ACTION_PAGES = {
    "findAProvider": INDIVIDUAL_PROVIDER,
    "findAFacility": FACILITY,
    "specialtySearch": NUCC,
    "findClinicalTrials": CLINICAL_TRIAL,
}


class UniversalNavigationTool(ChatHealthyTool):
    """Receives the post-AuthN deps + a typed NavRequest (op + payload),
    dispatches to the right graph-node handler. The handler may invoke
    other tools (via their `run_and_log()`); those calls auto-log to
    `deps.user_object.session_conversation_history` so the session render
    finds the per-tool invocation entries with `kind:"tool_invocation"`.

    Every op the application has is here, including the wire-level ones --
    peer addresses, peer health, token stamping and verification, the
    handing back of page ownership. They were answered by the HTTP route
    before this class was reached, so the door held an opinion about what a
    peer is and when a token is valid; it holds none now.
    """
    TOOL_NAME = "universal_navigation"
    Request = Request
    Response = Response

    # Map ops to method names. run() looks up by op and dispatches via
    # getattr(self, name). Adding a new op = new method + new dict entry.
    _OP_HANDLERS = {
        "session_data":         "_handle_session_data",
        "session_pdf":          "_handle_session_pdf",
        "record_ux_event":      "_handle_record_ux_event",
        "utterance":            "_handle_utterance",
        "provider-detail":      "_handle_provider_detail",
        "provider_detail_close": "_handle_provider_detail_close",
        "apply_filter":         "_handle_apply_filter",
        "refine_search":        "_handle_refine_search",
        "provider_page":        "_handle_provider_page",
        "restore_findcare":     "_handle_restore_findcare",
        "evalcare-splash":      "_handle_evalcare_splash",
        # clinical_trials_page op removed: client-side cache pagination
        # eliminated the per-page server round-trip; the React widget
        # slices its cached chunks locally.
        "about_chathealthy":    "_handle_about_chathealthy",
        "provider_selection":   "_handle_provider_selection",
        "facility_selection":   "_handle_facility_selection",
        "facility_page":        "_handle_facility_page",
        "clinical_trial_selection": "_handle_clinical_trial_selection",
        "claim_oauth_result":   "_handle_claim_oauth_result",
        "parameter_change":     "_handle_parameter_change",
        "peer_urls":            "_handle_peer_urls",
        "peer_health":          "_handle_peer_health",
        "session":              "_handle_session",
        "verify_token":         "_handle_verify_token",
        "transfer_to_findcare": "_handle_transfer_to_findcare",
    }

    async def run(self, deps: AgentDeps, request: "Request") -> "Response":
        op = request.op
        method_name = self._OP_HANDLERS.get(op)
        if method_name is None:
            return self.Response(
                kind="unknown_op",
                result={"op": op, "error": f"unknown op {op!r}"},
            )
        return await getattr(self, method_name)(deps, request.payload or {})

    # ── Op handlers ───────────────────────────────────────────────

    # The two form factors the application is built for. Which one a
    # person is on is a fact only the browser holds, so it is reported
    # rather than inferred, and it is one of these two or it is unknown.
    FORM_FACTORS = ("phone", "desktop")

    async def _handle_session_data(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        data = session_data(deps.user_object)
        data.setdefault("identity", {})["form_factor"] = (
            deps.user_object.form_factor or "unknown")
        data["deployment_facts"] = await deployment_facts()
        append_action(
            deps.user_object,
            tool_name="session_data_displayed",
            input_json={},
            output_json={"note": "SharedServices took ownership and rendered the User Object."},
        )
        deps.stream({"kind": "session_data", "data": data})
        return Response(kind="session_data", result=data)

    async def _handle_session_pdf(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """The session as a file the person downloads.

        The same projection the window renders, laid out for paper. It is a
        download rather than the browser's print dialogue because a phone
        has no usable print dialogue -- iOS shows a preview with no route to
        a PDF, and a native shell has none at all.
        """
        from sessionPdf import session_pdf  # noqa: PLC0415
        data = session_data(deps.user_object)
        data["deployment_facts"] = await deployment_facts()
        append_action(
            deps.user_object,
            tool_name="session_pdf_downloaded",
            input_json={},
            output_json={"filename": session_pdf.FILENAME},
        )
        return Response(kind="file", result={
            "media_type": "application/pdf",
            "filename": session_pdf.FILENAME,
            "content": session_pdf.render(data),
        })

    async def _handle_record_ux_event(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        event_type = str(payload.get("event_type") or "").strip()
        if not event_type:
            return Response(kind="record_ux_event", result={"ok": False, "error": "event_type required"})
        append_action(
            deps.user_object,
            tool_name="ux_event",
            input_json={
                "event_type": event_type,
                "value": payload.get("value"),
            },
            output_json={},
        )
        deps.stream({"kind": "ux_event_recorded", "data": {"event_type": event_type}})
        return Response(kind="record_ux_event", result={"ok": True})

    async def _handle_utterance(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """UR dispatch for op == 'utterance'. Routes the user's typed text
        through UtteranceManager (which classifies and writes IntentDocument
        to user_object.intent), then pattern-matches on target_action and
        dispatches the appropriate tool.

        UR validates schema + semantic compliance before dispatch; raises if
        UM gave us a malformed document. The tool dispatched is the only one
        that should write user-facing output for that intent.
        """
        text = str(payload.get("text") or "").strip()
        if not text:
            return Response(kind="utterance", result={"ok": False, "error": "text required"})

        # Persist the utterance onto user_object so downstream tools (UM or
        # LockoutTool) read it from the canonical place
        # (session_conversation_history.utterances[-1]).
        deps.user_object.persist_user_state(text)

        # safetyLockout pre-UM gate. UR hydrates user_object.is_locked_out +
        # user_object.lockout from {env}_Safety.emergency_incidents by IP. If
        # the IP has an active (unexpired, not-unlocked) record, UM is NOT
        # called this turn — LockoutTool runs directly to handle Task A
        # (operator $unlock backdoor) or Task B (still-locked reminder).
        await self._hydrate_lockout_if_any(deps)
        if deps.user_object.is_locked_out:
            from LockoutTool import lockout_tool
            await lockout_tool.TOOL.run_and_log(deps, lockout_tool.Request())
            return Response(kind="utterance", result={"target_action": "safetyLockout"})

        # What the search was about before this turn spoke. UM writes the
        # complaint only when the turn named one, so comparing across the
        # call is how the router tells a new question from a narrowing of
        # the one already in force.
        complaint_before = deps.user_object.userParameters.get(NUCC, "complaint")

        # Whether the system had asked the person something and is now
        # hearing the answer. Read before UM runs, because UM replaces the
        # document and the question it had pending goes with it.
        answering_a_question = any_pending_disambiguation(deps.user_object.intent)

        from UtteranceManager import utterance_manager as utterance_manager_module
        try:
            await utterance_manager_module.TOOL.run_and_log(
                deps, utterance_manager_module.Request(),
            )
        except ChatHealthyException as exc:
            if exc.mode != "llm_unavailable":
                raise
            await self._dispatch_llm_unavailable_dialogue(deps, exc)
            return Response(
                kind="utterance",
                result={"target_action": "closeConnection200",
                        "mode": "llm_unavailable"},
            )

        # A turn that answers a question the system asked is not a new
        # question, whatever the words were. Saying "California" to "did
        # you mean California?" is the same search with its place filled
        # in, so the panel the person is choosing from stands.
        #
        # Without this the decision rested on string equality over prose
        # a model rewrites every turn: the complaint came back as a
        # different phrasing of the same thing, the filter re-ran, and
        # the answer turn handed back a panel of nine where the person
        # had ticked twelve -- four of their choices dropped for saying
        # the state out loud.
        complaint_changed = (
            not answering_a_question
            and deps.user_object.userParameters.get(NUCC, "complaint")
            != complaint_before)

        # "New is new" invariant: a fresh free-text utterance clears any
        # selected_nucc_codes carried over from a prior turn's apply_filter
        # gesture. selected_nucc_codes exists on the IntentDocument ONLY as
        # the immediate result of an apply_filter; it does not survive a
        # subsequent free-text utterance. Without this strip the prior
        # turn's filter selection silently narrows ProviderSearch on the
        # new query (reported by operator 2026-06-11).
        document = deps.user_object.intent
        if document is not None:
            for entry in document.intents:
                entry.arguments = [
                    a for a in entry.arguments if a.name != "selected_nucc_codes"
                ]

        # Same invariant on the parameter side. The selection belongs to the
        # panel it was made on; carrying it onto a new question's panel
        # would show the user ticks they never made for this question.
        #
        # Position goes with it: a new question starts at the top of its own
        # list, and an open detail belongs to the list that is being
        # replaced.
        #
        # Cleared when this turn asks a NEW question. A turn that goes on to
        # a page writes that page's ticks itself, from the panel it just
        # resolved, so what this clearing decides is what a turn ending in
        # no page dispatch leaves behind: a new question leaves no ticks
        # from the old one.
        from UserParameters import user_parameters_tool
        carried = [(INDIVIDUAL_PROVIDER, "position"),
                   (INDIVIDUAL_PROVIDER, "openNpi")]
        # A page's displayed state does not survive a turn about another
        # page. A facility detail left open while the person went back to
        # looking for a care giver stayed on screen beside a care-giver
        # list -- an organization in Long Beach beside psychiatrists in San
        # Francisco -- because only this page's own position and open
        # record were cleared. Every page a turn is not about is cleared of
        # what it was showing.
        carried += [(FACILITY, "position"), (FACILITY, "openNpi"),
                    (CLINICAL_TRIAL, "position"), (CLINICAL_TRIAL, "openNctId")]
        if complaint_changed:
            carried = [(NUCC, "selectedSpecialtyCodes"),
                       (INDIVIDUAL_PROVIDER, "selectedSpecialtyCodes")] + carried
        # Whether another page had a record open before this turn cleared
        # it. Read before the clear, because after it there is nothing left
        # to say the panel is showing something.
        other_page_was_open = bool(
            deps.user_object.userParameters.get(FACILITY, "openNpi")
            or deps.user_object.userParameters.get(CLINICAL_TRIAL, "openNctId"))
        await user_parameters_tool.TOOL.run_and_log(
            deps,
            user_parameters_tool.Request(
                verb="clear", route="gateway", origin="deterministic",
                changes=[user_parameters_tool.Change(page=page, name=name)
                         for page, name in carried],
            ),
        )
        # Clearing the parameter does not unpaint the frame. The panel comes
        # down when the server says so, which is the same event the deliberate
        # close already uses (EPIC-006-F-008-S-002-REQ-B-006).
        if other_page_was_open:
            deps.stream({"kind": "provider_detail_close", "data": {"closed": True}})

        last_target_action: Optional[str] = None
        for _hop in range(MAX_DISPATCH_HOPS):
            document = deps.user_object.intent
            if document is None:
                raise ChatHealthyException(
            mode="runtime_error",
            component="universal_navigation_tool",
            message="UR compliance: UtteranceManager returned without setting "
                    "user_object.intent")

            target_action = document.target_action
            if target_action == last_target_action:
                break

            # REQ-B-004: do not chain to closeConnection200 on a turn that
            # carries a pending disambiguation — the connection is logically
            # still open across the user's next turn.
            if target_action == "closeConnection200" and any_pending_disambiguation(document):
                break

            self._validate_document(document, target_action)
            await self._dispatch_target_action(deps, document, target_action)
            last_target_action = target_action
        else:
            raise ChatHealthyException(
            mode="runtime_error",
            component="universal_navigation_tool",
            message=f"UR dispatch exceeded {MAX_DISPATCH_HOPS} hops; last "
                f"target_action={last_target_action!r}")

        return Response(
            kind="utterance",
            result={"target_action": last_target_action},
        )

    async def _handle_provider_detail(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Click path: a user clicked the detail link on a provider card. This
        is NOT an utterance — no LLM hop. Forward the card-field payload to
        the ProviderDetail tool, which HTTPS-hops to FindCare and streams
        {kind: 'provider-detail', data: ...} back to the FE."""
        req = provider_detail_tool.Request(**payload)
        resp = await provider_detail_tool.TOOL.run_and_log(deps, req)

        # An open detail is a place the user navigated to. Recording it here
        # is what lets a return to FindCare put them back on it instead of
        # at the top of the list.
        record_id = str((payload or {}).get("npi") or "").strip()
        if record_id:
            # The record is recorded on the page it belongs to. Both pages
            # open a detail through this one handler, and it wrote every
            # open record to the care-giver page -- so a facility was held
            # open there, no page ever knew a facility was on screen, and
            # nothing could take it down when the person moved on.
            #
            # Which page is a routing fact and stays here. Which of that
            # page's parameters holds a record on screen is the page's, and
            # is not the same attribute on every page.
            entity_type = str((payload or {}).get("entity_type") or "").strip()
            page = FACILITY if entity_type == "2" else INDIVIDUAL_PROVIDER
            await self._tell_page_detail_opened(deps, page, record_id)
        # No inner "final" emission — _run_pipeline_then_finalize emits
        # the single canonical final event with full payload.
        return Response(kind="provider-detail", result=resp.model_dump(exclude_none=True, mode='json'))

    async def _search_providers(self, deps: AgentDeps, **fields) -> Any:
        """The gestures' route to the provider search.

        Paging, refining and restoring reach the search here; an utterance
        reaches it through the individual-provider page, which mines the
        utterance and searches on what it mined. Both ends put the list
        they produced through _reconcile_open_detail, so the rule about an
        open detail is written once rather than at each site.
        """
        # The preferences the person stated are read here, not at the three
        # call sites. They live on the session, every provider list comes
        # through this function, and a caller that forgot to pass them would
        # silently return a list the person had already narrowed away.
        params = deps.user_object.userParameters
        if params is not None:
            name = provider_name_of(params)
            if name is not None and not name.is_empty():
                fields.setdefault("last_name", name.last or None)
                fields.setdefault("first_name", name.first or None)
                fields.setdefault("middle_name", name.middle or None)
            sex = params.get(INDIVIDUAL_PROVIDER, "providerSex")
            if sex:
                fields.setdefault("provider_sex", sex)
            payer = params.get(INDIVIDUAL_PROVIDER, "insurance")
            if payer:
                fields.setdefault("insurance", payer)
            sole = params.get(INDIVIDUAL_PROVIDER, "soleProprietor")
            if sole is not None:
                fields.setdefault("sole_proprietor", sole)

        from authentication import provider_search_tool
        resp = await provider_search_tool.TOOL.run_and_log(
            deps, provider_search_tool.Request(**fields))
        await self._reconcile_open_detail(deps, resp.providers or [])
        return resp

    async def _reconcile_open_detail(self, deps: AgentDeps,
                                     providers: list) -> None:
        """A detail belongs to a provider in the list being presented.

        Paging forward, narrowing the filter, or restoring to a different
        page all replace the list the detail came from. Leaving the panel
        up then describes somebody the user cannot see, and it is the panel
        that is wrong, not the list.

        Membership is checked against the page actually returned, not
        against the query that produced it, because the page is what the
        user is looking at. The rows are taken rather than the response
        that carried them, so a list produced by the provider page and one
        produced by the search tool are checked by the same rule.
        """
        npi = str(deps.user_object.userParameters.get(
            INDIVIDUAL_PROVIDER, "openNpi") or "").strip()
        if not npi:
            return
        presented = {str(p.get("npi") or "").strip()
                     for p in (providers or [])}
        if npi in presented:
            return
        await self._handle_provider_detail_close(deps, {})

    async def _handle_refine_search(
            self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """The person picked a refinement in the panel.

        The panel reported which dimension and which value; what that means
        is decided here. One parameter is written and the search already in
        force is re-run -- the query has not changed, only how narrow it is,
        so this emits search_running rather than intent_classified, which
        would blank the panel being refined with.

        Choosing the value already in force clears it, so a chip is a
        toggle: pick Female to narrow, pick it again to stop.
        """
        dimension = str((payload or {}).get("dimension") or "").strip()
        value = str((payload or {}).get("value") or "").strip()
        if dimension not in ("provider_sex", "insurance", "sole_proprietor"):
            return Response(kind="refine_search",
                            result={"ok": False, "error": f"unknown dimension {dimension!r}"})

        params = deps.user_object.userParameters
        attribute = {"provider_sex": "providerSex",
                     "insurance": "insurance",
                     "sole_proprietor": "soleProprietor"}[dimension]
        current = params.get(INDIVIDUAL_PROVIDER, attribute)
        if dimension == "sole_proprietor":
            chosen = value.upper() == "Y"
            clearing = current is not None and bool(current) == chosen
        else:
            chosen = value
            clearing = str(current or "") == value

        from UserParameters import user_parameters_tool
        await user_parameters_tool.TOOL.run_and_log(
            deps,
            user_parameters_tool.Request(
                verb="clear" if clearing else "set",
                page=INDIVIDUAL_PROVIDER,
                name=attribute,
                value=None if clearing else chosen,
                route="gateway", origin="deterministic",
            ),
        )

        deps.stream({"kind": "search_running",
                     "data": {"action": "findAProvider", "refining": dimension}})

        params = deps.user_object.userParameters
        geo = geography_of(params, INDIVIDUAL_PROVIDER)
        await self._search_providers(
            deps,
            specialty_codes=codes_in_force(params),
            state=geo.state if geo else None,
            city=geo.city if geo else None,
            county=geo.county if geo else None,
            zip=geo.zip if geo else None,
            limit=25,
        )
        return Response(kind="refine_search",
                        result={"dimension": dimension,
                                "cleared": clearing, "value": value})

    async def _handle_provider_detail_close(
            self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """The detail is no longer on screen. Record that.

        selected_provider_npi means "a detail is open", not "a detail was
        opened once". A parameter that only ever gets set turns a return to
        FindCare into a resurrection: the user closes a panel, leaves,
        comes back, and it is there again.

        Reached by the panel's own close control and by scrolling the
        provider list -- scrolling moves the user off the row the detail
        belongs to, so the panel stops describing what they are looking at.
        The gesture is the client's; what it means is decided here.
        """
        await self._tell_page_detail_closed(deps, INDIVIDUAL_PROVIDER)
        deps.stream({"kind": "provider_detail_close", "data": {"closed": True}})
        return Response(kind="provider_detail_close", result={"closed": True})

    async def _handle_restore_findcare(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Put the user back exactly where they left FindCare.

        Replayed from the parameters, not from a cached screen. Everything
        needed is already there because every step wrote it down: the panel
        and which rows are ticked, the geography, how far down the list they
        had read, and whether a detail was open.

        That is the point of one live parameter set. A cached screen would
        have to be invalidated, would go stale against a data version swap,
        and would be a second copy of facts the session already holds.

        Nothing here classifies or re-runs the specialty pipeline: the
        panel is republished from the parameter, so returning costs no LLM
        call and cannot come back different from how it was left.
        """
        params = deps.user_object.userParameters
        offered = params.get(NUCC, "offeredSpecialties") or []
        if not offered:
            deps.stream({"kind": "restore_findcare", "data": {"restored": False}})
            return Response(kind="restore_findcare", result={"restored": False})

        # The panel, exactly as it was -- same rows, same ticks.
        deps.stream({
            "kind": "specialties",
            "data": {
                "specialties": [dict(s) for s in offered],
                "homeopathic_generalists": [],
                "selected_codes": list(
                    params.get(NUCC, "selectedSpecialtyCodes") or []),
                "restored": True,
            },
        })

        geo = geography_of(params, INDIVIDUAL_PROVIDER)
        codes = codes_in_force(params)

        # Back to the page they were on, in one query. The search is ordered
        # by NPI, so the stored key IS the position: asking for the rows
        # after it returns that page directly. No key means the top, which
        # is where a list that was never paged belongs.
        position = params.get(INDIVIDUAL_PROVIDER, "position") or {}
        cursor = str(position.get("first") or "")
        await self._search_providers(
            deps,
            specialty_codes=codes,
            state=geo.state if geo else None,
            city=geo.city if geo else None,
            county=geo.county if geo else None,
            zip=geo.zip if geo else None,
            cursor=cursor or None,
            direction="forward" if not cursor else "back",
            limit=25,
        )

        # And the detail, if one was open AND its provider is on the page
        # that just painted. The search above already cleared it if not, so
        # re-reading the parameter here is what stops the restore from
        # putting back a panel the rule just took down.
        detail_npi = deps.user_object.userParameters.get(
            INDIVIDUAL_PROVIDER, "openNpi")
        if detail_npi:
            await self._handle_provider_detail(deps, {"npi": detail_npi})

        return Response(kind="restore_findcare", result={
            "restored": True,
            "cursor": cursor,
            "detail_npi": detail_npi,
        })

    async def _handle_evalcare_splash(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Click path: a user clicked the EvaluateCare banner button. This
        is NOT an utterance — no LLM hop. Server-to-server HTTPS-hops to
        EvaluateCare's /splash and streams {kind: 'evalcare-splash', ...}
        back to the FE. Mirror of _handle_provider_detail."""
        req = evalcare_splash_tool.Request(**payload)
        resp = await evalcare_splash_tool.TOOL.run_and_log(deps, req)
        # No inner "final" emission — _run_pipeline_then_finalize emits
        # the single canonical final event with full payload.
        return Response(kind="evalcare-splash", result=resp.model_dump(exclude_none=True))

    async def _handle_apply_filter(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """UR dispatch for op == 'apply_filter' per
        EPIC-002-F-010-S-002-REQ-B-002.

        Apply Filter is a parameter change, not a flow. The user narrowed
        the specialty selection; nothing else about what they asked for
        moved. So this writes one parameter and runs the search again on
        the parameters in force. It does not take the utterance path: that
        path hands the utterance to the individual-provider page, which
        reads it, and this gesture produced no utterance to be read.

        It used to rebuild the IntentDocument from parts, carrying the
        complaint, the geography and the panel back onto a freshly
        constructed pair of intent entries so none of them would be lost.
        None of that is needed once those facts live on the user rather
        than on the document: the document is not rebuilt, so there is
        nothing to carry.

        Geography insufficient still goes to UM's manufacture path, which
        authors the prompt asking for a location.
        """
        # The FE sends selected_codes and the panel those codes came from.
        # It used to send `codes` while this read `nucc_codes`, so the
        # selection never arrived and every Apply Filter searched the full
        # universe.
        nucc_codes = (payload.get("selected_codes")
                      or payload.get("nucc_codes") or [])
        if not isinstance(nucc_codes, list):
            return Response(
                kind="apply_filter",
                result={"ok": False, "error": "selected_codes must be a list"},
            )
        selected_codes = [c for c in nucc_codes if isinstance(c, str)]

        from UserParameters import user_parameters_tool
        await user_parameters_tool.TOOL.run_and_log(
            deps,
            user_parameters_tool.Request(
                verb="set", route="gateway", origin="deterministic",
                # What the person kept, and what the search is running
                # under. They differ between choosing and applying; this is
                # the moment they are made the same.
                changes=[
                    user_parameters_tool.Change(
                        page=NUCC, name="selectedSpecialtyCodes",
                        value=selected_codes),
                    user_parameters_tool.Change(
                        page=INDIVIDUAL_PROVIDER, name="selectedSpecialtyCodes",
                        value=selected_codes),
                ],
            ),
        )

        prior = deps.user_object.intent
        params = deps.user_object.userParameters

        # Read, never dug out of the document. Geography was never Apply
        # Filter's to carry: whoever set it last set it, and this gesture
        # does not change it.
        live_geo = geography_of(params, INDIVIDUAL_PROVIDER)
        geography = (live_geo.model_dump(exclude_none=True) if live_geo else {})

        if self._geography_sufficient(geography) and prior is not None:
            # NOT intent_classified. That kind means "a new query has been
            # classified", and NewQueryLoadingWidget answers it by blanking
            # LeftPanel, RightPanel and MainWindow -- so announcing it here
            # wiped the specialty panel the user was filtering with.
            #
            # Apply Filter is a narrowing of the query already in force, so
            # it says only that a search is running. The panel stays; the
            # results pane repaints when the providers arrive.
            deps.stream({
                "kind": "search_running",
                "data": {
                    "action": "findAProvider",
                    "criteria": params.get(NUCC, "complaint")
                                or "your selected specialties",
                    "selected_specialty_count": len(selected_codes),
                },
            })
            # The search under the parameters in force, and NOT the
            # utterance path's dispatch. That path hands the utterance to
            # the provider page, which mines it and re-resolves the panel
            # -- and Apply Filter produces no utterance, so the words it
            # would re-read are the previous turn's. The person's ticks
            # would be replaced by a freshly resolved set they never chose
            # (the shape of the 2026-06-10 defect). Apply Filter changes
            # which boxes are ticked and nothing else, so the panel on
            # screen stands and only the search runs again.
            await self._search_providers(
                deps,
                specialty_codes=codes_in_force(params),
                state=live_geo.state if live_geo else None,
                city=live_geo.city if live_geo else None,
                county=live_geo.county if live_geo else None,
                zip=live_geo.zip if live_geo else None,
                limit=25,
            )
            return Response(
                kind="apply_filter",
                result={"target_action": "findAProvider"},
            )

        reason: dict[str, Any] = {
            "gesture": "apply_filter",
            "missing_slot": "geography",
            "selected_specialty_count": len(selected_codes),
        }
        prior_complaint = params.get(NUCC, "complaint")
        if prior_complaint:
            reason["prior_complaint"] = prior_complaint
        partial_geo = {k: v for k, v in (geography or {}).items() if v}
        if partial_geo:
            reason["prior_partial_geography"] = partial_geo

        # Nothing is seeded onto the document. The selection is already on
        # the user's parameters, where the next turn reads it from, so
        # copying it into the intent would be a second home for one fact.

        from UtteranceManager import utterance_manager as utterance_manager_module
        try:
            await utterance_manager_module.TOOL.run_and_log(
                deps,
                utterance_manager_module.Request(
                    trigger_type="manufacture",
                    manufacture_utterance_reason=reason,
                ),
            )
        except ChatHealthyException as exc:
            if exc.mode != "llm_unavailable":
                raise
            log.exception(
                "UR caught ChatHealthyException — raised at server=%s "
                "component=%s; caught at server=shared_services "
                "component=UR; mode=%s message=%s context=%s",
                exc.server, exc.component, exc.mode, exc.message,
                exc.context,
                stack_info=True,
                exc=exc, if_not_debug_log=True,
            )
            await self._dispatch_llm_unavailable_dialogue(deps, exc)
            return Response(
                kind="apply_filter",
                result={"target_action": "closeConnection200",
                        "mode": "llm_unavailable"},
            )
        return Response(
            kind="apply_filter",
            result={"target_action": "closeConnection200"},
        )

    async def _handle_provider_page(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Another page of the search already in force, forward or back.

        The server has always paged -- the search takes a keyset position
        and the result carries first_npi and last_npi -- and only forward
        was ever exposed, so a person who paged past what they wanted had
        no way back short of re-running the search.

        It is deterministic: same parameters, one more page. No LLM, no
        reclassification, and no announcement that would blank the frames.
        """
        cursor = str((payload or {}).get("cursor") or "").strip()
        direction = str((payload or {}).get("direction") or "forward").strip()
        if direction not in ("forward", "back"):
            return Response(kind="provider_page",
                            result={"ok": False,
                                    "error": f"unknown direction {direction!r}"})
        if not cursor:
            return Response(kind="provider_page",
                            result={"ok": False, "error": "cursor required"})

        params = deps.user_object.userParameters
        geo = geography_of(params, INDIVIDUAL_PROVIDER)
        codes = codes_in_force(params)
        if not codes:
            return Response(kind="provider_page",
                            result={"ok": False, "error": "no specialties selected"})

        resp = await self._search_providers(
            deps,
            specialty_codes=codes,
            state=geo.state if geo else None,
            city=geo.city if geo else None,
            county=geo.county if geo else None,
            zip=geo.zip if geo else None,
            cursor=cursor,
            direction=direction,
            limit=25,
        )

        # Remember where they now are. The position is the first and last
        # key of the page in view, which is what both directions need: back
        # takes the rows before the first, forward those after the last.
        await self._write_position(deps, "individualProvider",
                                   resp.first_npi, resp.last_npi)
        return Response(kind="provider_page",
                        result={"cursor": cursor, "direction": direction})

    async def _write_position(self, deps: AgentDeps, page: str,
                              first_key, last_key) -> None:
        """Record the page in view as its first and last key."""
        from UserParameters import user_parameters_tool
        await user_parameters_tool.TOOL.run_and_log(
            deps,
            user_parameters_tool.Request(
                verb="set", page=page, name="position",
                value={"first": first_key or "", "last": last_key or ""},
                route="gateway", origin="deterministic",
            ),
        )

    async def _handle_facility_selection(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Thin pass-through to the facility selection tool. The facility
        set is its own, so choosing a facility disturbs no care-giver
        answer (EPIC-006-F-008-S-001-REQ-B-006)."""
        from FacilitySelection import facility_selection_tool
        req = facility_selection_tool.Request(**(payload or {}))
        resp = await facility_selection_tool.TOOL.run_and_log(deps, req)
        return Response(kind="facility_selection",
                        result=resp.model_dump(exclude_none=True))

    async def _handle_facility_page(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Another page of the facility list, forward or back. The same
        keyset paging the care-giver list uses, on the facility page's own
        position."""
        cursor = str((payload or {}).get("cursor") or "").strip()
        direction = str((payload or {}).get("direction") or "forward").strip()
        if direction not in ("forward", "back"):
            return Response(kind="facility_page",
                            result={"ok": False,
                                    "error": f"unknown direction {direction!r}"})
        if not cursor:
            return Response(kind="facility_page",
                            result={"ok": False, "error": "cursor required"})

        raw = await self._tell_page(
            deps, "/facility/page",
            {"cursor": cursor, "direction": direction, "limit": 25})
        self._stream_facilities(deps, raw)
        return Response(kind="facility_page",
                        result={"cursor": cursor, "direction": direction})

    async def _handle_parameter_change(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """A parameter change asked for from the panel that shows it.

        The panel is a surface, not a fourth writer: this dispatches the
        one parameters tool exactly as any other route does, so the same
        validation and the same refusal apply. The gateway may address any
        page, because it dispatches across pages and must clear a stale
        position or an open detail no tool owns.

        The response carries the new state and the panel repaints from it
        in the same turn. It is not refetched: refetching would be a
        second read that could disagree with what was just acknowledged.
        """
        from UserParameters import user_parameters_tool
        verb = str((payload or {}).get("verb") or "").strip()
        # read is one of the three things each route must be able to do
        # (EPIC-006-F-008-S-003-REQ-B-001..003), so the gateway's op
        # carries it alongside the writes.
        if verb not in ("read", "set", "clear", "clear_page", "clear_all"):
            return Response(kind="parameter_change",
                            result={"ok": False,
                                    "error": f"unknown verb {verb!r}"})
        resp = await user_parameters_tool.TOOL.run_and_log(
            deps,
            user_parameters_tool.Request(
                verb=verb,
                page=(payload or {}).get("page"),
                name=(payload or {}).get("name"),
                value=(payload or {}).get("value"),
                route="gateway", origin="deterministic",
            ),
        )
        # Reported after the write is acknowledged, never before. A failed
        # write emits the failure instead, naming the parameters that did
        # not take.
        deps.stream({"kind": "parameters_changed",
                     "data": resp.model_dump(exclude_none=True)})
        return Response(kind="parameter_change",
                        result=resp.model_dump(exclude_none=True))

    async def _handle_about_chathealthy(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Thin pass-through to AboutChatHealthyTool. Tool streams
        `kind:about_chathealthy` with structured data; React widget
        renders HTML and calls ClientRouter.render."""
        from AboutChatHealthy import about_chathealthy_tool
        req = about_chathealthy_tool.Request(**(payload or {}))
        await about_chathealthy_tool.TOOL.run_and_log(deps, req)
        return Response(kind="about_chathealthy", result={"ok": True})

    async def _handle_claim_oauth_result(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Pop the OAuth result the callback stashed on user_object.
        Returns {result: {...}} for the React HeaderWidget to render its
        banner, then clears the field so the next poll returns nothing.
        Pure structured-data path: no HTML anywhere.

        Atomic find_one_and_update: claim and clear in one Mongo round-trip
        so a concurrent /gate save with a snapshot of the user_object from
        before the OAuth-callback persist cannot overwrite the persisted
        result. The user_object's in-memory pending_oauth_result is also
        cleared so the request-end save (write_session_record) doesn't put
        it back.
        """
        from pymongo import ReturnDocument
        from authentication.authorizations_and_authentications_tool import (
            SESSION_DB, SESSION_COLLECTION,
        )
        user_obj = deps.user_object
        guid = user_obj.current_session_token.get_auth_token()
        coll = deps.mongo_frontend[SESSION_DB][SESSION_COLLECTION]
        before = coll.find_one_and_update(
            {"_id": guid, "pending_oauth_result": {"$ne": None}},
            {"$set": {"pending_oauth_result": None}},
            projection={"pending_oauth_result": 1},
            return_document=ReturnDocument.BEFORE,
        )
        result = before.get("pending_oauth_result") if before else None
        user_obj.pending_oauth_result = None
        data = {"result": result}
        deps.stream({"kind": "oauth_result", "data": data})
        return Response(kind="claim_oauth_result", result=data)

    async def _handle_provider_selection(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Thin pass-through to ProviderSelectionTool. Tool mutates
        user_object.selected_providers and streams `kind:selection_changed`
        with the current NPI list; SelectedProvidersWidget repaints its
        strip via ClientRouter.merge into MainWindow's selected_strip
        region."""
        from ProviderSelection import provider_selection_tool
        req = provider_selection_tool.Request(**(payload or {}))
        await provider_selection_tool.TOOL.run_and_log(deps, req)
        return Response(kind="provider_selection", result={"ok": True})

    async def _handle_clinical_trial_selection(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Thin pass-through to ClinicalTrialSelectionTool. Tool mutates
        user_object.selected_clinical_trials and streams
        `kind:trial_selection_changed` with the current NCT-ID list; the
        SelectedClinicalTrialsWidget repaints its strip via ClientRouter.merge
        into MainWindow's selected_trials_strip region."""
        from ClinicalTrialSelection import clinical_trial_selection_tool
        req = clinical_trial_selection_tool.Request(**(payload or {}))
        await clinical_trial_selection_tool.TOOL.run_and_log(deps, req)
        return Response(kind="clinical_trial_selection", result={"ok": True})

    async def _handle_peer_urls(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Where the browser can reach each peer.

        The wrapper is served as static bytes, so the addresses it gives its
        iframe cannot be substituted into it at build time and are asked for
        instead.
        """
        return Response(kind="peer_urls", result={
            "findcare":       CH_BROWSER_PEER_URL_FINDCARE,
            "evaluatecare":   CH_BROWSER_PEER_URL_EVALCARE,
            "sharedservices": "",   # the wrapper already knows its own /gate origin
        })

    async def _handle_peer_health(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """One peer's health, asked for on the caller's behalf.

        A peer's internal address is not reachable from a browser, so the
        question is put from here and the peer's own answer handed back
        unchanged. A peer that does not answer is itself the answer.
        """
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
            from healthcheck.health_endpoint import HealthEndpoint  # noqa: PLC0415
            return Response(kind="peer_health", result=HealthEndpoint()())
        import httpx  # noqa: PLC0415
        try:
            async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
                r = await client.post(target_url + "/health")
                r.raise_for_status()
                return Response(kind="peer_health", result=r.json())
        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError,
                httpx.HTTPStatusError) as exc:
            return Response(kind="peer_health", result={
                "status": "unreachable",
                "service": peer,
                "error": f"{type(exc).__name__}: {exc}",
            })

    async def _handle_session(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """A fresh stamp on the token the caller already holds.

        The GUID is per session and the nonce is per hop, so a caller that is
        still here is restamped rather than given a second session.
        """
        body_obj = SessionRestampRequest.model_validate(payload or {})
        return Response(kind="session", result=AuthToken.handle_session(
            body_obj, origin=authn.ORIGIN, server_env=ENV).model_dump())

    async def _handle_verify_token(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """Whether a token the caller holds still verifies against us.

        A peer that was handed a token asks here rather than carrying our
        signing material, which is what keeps verification one component's
        job.
        """
        body_obj = SessionRestampRequest.model_validate(payload or {})
        return Response(kind="verify_token", result=AuthToken.handle_verify(
            body_obj, origin=authn.ORIGIN, server_env=ENV).model_dump())

    async def _handle_transfer_to_findcare(self, deps: AgentDeps, payload: dict[str, Any]) -> Response:
        """SharedServices gives the page back to FindCare.

        Ownership is named by the server so both sides read one answer rather
        than each deciding who has the page.
        """
        from displayChrome.transfer_to_findcare_endpoint import (  # noqa: PLC0415
            TransferToFindCareEndpoint,
        )
        return Response(kind="transfer_to_findcare",
                        result=TransferToFindCareEndpoint()())

    # ── Orchestration helpers ─────────────────────────────────────

    @staticmethod
    def _geography_sufficient(geo: Optional[dict]) -> bool:
        """Same rule findAProvider applies — zip OR state. city/county
        without state are not enough."""
        if not geo:
            return False
        return bool((geo.get("zip") or "").strip()) or bool((geo.get("state") or "").strip())





    async def _hydrate_lockout_if_any(self, deps: AgentDeps) -> None:
        """UR's pre-UM hydration step for the safetyLockout flow.

        Single find_one against {env}_Safety.emergency_incidents keyed by
        user_object.ip_address. If an active record exists (expires_at >
        now AND unlocked != true), stamps is_locked_out=True and a Lockout
        sub-object straight from the DB record's matching fields. Otherwise
        leaves user_object untouched.

        All three LockoutTool tasks read their inputs from user_object;
        this is the only DB read in the lockout flow.
        """
        ip = (deps.user_object.ip_address or "").strip()
        if not ip:
            return
        if deps.mongo_frontend is None:
            return
        coll = deps.mongo_frontend[f"{ENV}_Safety"]["emergency_incidents"]
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            record = coll.find_one({
                "ip": ip,
                "expires_at": {"$gt": now_iso},
                "unlocked": {"$ne": True},
            })
        except Exception as exc:
            # Mode 2 (REQ-B-008): Mongo lockout-table query failed; UR
            # proceeds as if not locked (operator must know). log.error +
            # if_not_debug_log=True.
            log.error("UR: _hydrate_lockout_if_any find_one failed: %s", exc, exc=ChatHealthyException(
                                                                                   mode="ur_hydrate_lockout_find_failed",
                                                                                   message=f"UR: _hydrate_lockout_if_any find_one failed: {exc}",
                                                                                   component="UniversalNavigationTool",
                                                                                   exception=exc,
                                                                               ), if_not_debug_log=True)
            return
        if record is None:
            return
        from chathealthy_lib.authentication.user_object import Lockout
        expires_str = record.get("expires_at") or ""
        try:
            expires_at = datetime.fromisoformat(expires_str)
        except Exception as _exc:
            # Mode 1 (REQ-B-008): bad data in emergency_incidents row;
            # treated as inactive and UR continues. log.info + default
            # debug-gated.
            log.info(
                "UR: emergency_incidents row for ip=%s has invalid expires_at=%r; "
                "treating as inactive", ip[:16] + "...", expires_str,
                exc=ChatHealthyException(
                 mode="ur_lockout_expires_at_invalid",
                 message=f"UR: emergency_incidents row for ip={ip[:16]}... has invalid expires_at={expires_str!r}; treating as inactive",
                 component="UniversalNavigationTool",
                 exception=_exc,
             ), if_not_debug_log=True,
            )
            return
        deps.user_object.is_locked_out = True
        deps.user_object.lockout = Lockout(
            expires_at=expires_at,
            trigger_utterance=str(record.get("trigger_message") or ""),
            history=list(record.get("history") or []),
        )
        log.debug(
            "UR: hydrated lockout for ip=%s; expires_at=%s",
            ip[:16] + "...", expires_str,
        )

    def _validate_document(self, document, target_action: str) -> None:
        """UR compliance check on the IntentDocument: target_action enumerated
        by Pydantic; must correspond to a name in intents[]; required arguments
        non-empty and parseable; findAProvider geography sufficiency."""
        import json as json

        target_intent_entry = next(
            (i for i in document.intents if i.name == target_action), None,
        )
        if target_intent_entry is None:
            raise ChatHealthyException(
            mode="runtime_error",
            component="universal_navigation_tool",
            message=f"UR compliance: target_action {target_action!r} has no matching "
                f"entry in intents[] (names={[i.name for i in document.intents]})")
        for arg in target_intent_entry.arguments:
            if arg.required and not arg.value:
                raise ChatHealthyException(
            mode="runtime_error",
            component="universal_navigation_tool",
            message=f"UR compliance: required argument {arg.name!r} for target_action "
                    f"{target_action!r} has empty value")
            if arg.type == "boolean" and arg.value not in ("true", "false"):
                raise ChatHealthyException(
            mode="runtime_error",
            component="universal_navigation_tool",
            message=f"UR compliance: boolean argument {arg.name!r} has value "
                    f"{arg.value!r}, must be 'true' or 'false'")
            if arg.type in ("object", "array"):
                try:
                    json.loads(arg.value)
                except json.JSONDecodeError as exc:
                    raise ChatHealthyException(
                        mode="ur_compliance_arg_invalid_json",
                        message=f"UR compliance: {arg.type} argument {arg.name!r} value is not valid JSON: {exc}",
                        component="UniversalNavigationTool",
                        exception=exc,
                    )

        # Nothing here judges whether a tool has enough to work with.
        # Routing is this component's job; sufficiency belongs to the tool,
        # which holds its own declaration, refuses on its own terms and
        # asks the person for what it lacks. This used to refuse a
        # care-giver search that named a city and no state, so the turn
        # died at dispatch instead of arriving at the page that would have
        # asked which state.

    async def _dispatch_llm_unavailable_dialogue(
        self, deps: AgentDeps, exc: ChatHealthyException,
    ) -> None:
        """Rung 2 of the failure ladder (EPIC-008-F-002-S-009-REQ-B-007).

        UM's classifier or apply_filter LLM call exhausted retries against
        an LLM provider. Re-dispatch UM with a manufacture trigger so it
        authors a non-technical user-facing message via the existing
        manufacture pattern. If THIS manufacture call also raises
        ChatHealthyException, it propagates to UR's outermost catch and
        the existing fatal path fires (rung 3).
        """
        from UtteranceManager import utterance_manager as utterance_manager_module
        reason = {
            "gesture": "system_llm_unavailable",
            "raised_at_server": exc.server,
            "raised_at_component": exc.component,
            "caught_at_server": "shared_services",
            "caught_at_component": "UR",
            "provider": exc.context.get("provider"),
            "call_site": exc.context.get("call_site"),
            "attempts": exc.context.get("attempts"),
        }
        await utterance_manager_module.TOOL.run_and_log(
            deps,
            utterance_manager_module.Request(
                trigger_type="manufacture",
                manufacture_utterance_reason=reason,
            ),
        )

    # The ops that are a turn: the person said or gestured something and is
    # owed an answer. Everything else -- session reads, selection
    # bookkeeping -- is not a turn and is allowed to be silent.
    _TURN_OPS = frozenset({
        "utterance", "apply_filter", "provider_page", "facility_page",
    })

    # An event that puts something on a window or says something to the
    # person. intent_classified and search_running are not answers: the
    # first blanks the windows and the second says a search is running.
    _ANSWERING_KINDS = frozenset({
        "prompt", "specialties", "providers", "facilities",
        "clinical_trials_chunk", "provider-detail", "evalcare-splash",
        "about_chathealthy", "show_welcome",
    })

    def _stream_facilities(self, deps: AgentDeps, raw: dict) -> None:
        """Put a set of organizations on the window.

        One copy, because a search and a page turn produce the same event
        and two copies of the shaping is two things to keep in step. Every
        field is taken from the page's answer and none is interpreted.
        """
        facilities = (raw or {}).get("providers") or []
        if not facilities:
            return
        data = {
            "facilities": facilities,
            "has_more": bool(raw.get("has_more", False)),
            "has_previous": bool(raw.get("has_previous", False)),
            "first_npi": raw.get("first_npi"),
            "last_npi": raw.get("last_npi"),
            "count": int(raw.get("count", 0) or 0),
            "total_count": int(raw.get("total_count", 0) or 0),
            "page_start": int(raw.get("page_start", 1) or 1),
            "page_end": int(raw.get("page_end", 0) or 0),
            "search_params": raw.get("search_params"),
            "state": raw.get("state"),
            "summary_message": raw.get("summary_message"),
        }
        deps.stream({
            "kind": "facilities",
            "data": {name: value for name, value in data.items()
                     if value is not None},
        })

    async def _tell_page(self, deps: AgentDeps, path: str,
                         body: dict) -> dict:
        """Say something to the page that owns it, and take its answer.

        The body carries page names and record identities and no parameter
        name: which of its own parameters a page changes on being told this
        is the page's business, and naming one here would be this component
        holding a fact it has no use for.
        """
        import httpx
        from authentication import provider_search_tool
        url = provider_search_tool.findcare_url() + path
        try:
            async with httpx.AsyncClient(timeout=None, verify=False) as client:
                r = await client.post(url, json={
                    "session_token": deps.session_token.model_dump(mode="json"),
                    **body,
                })
                r.raise_for_status()
                return r.json() or {}
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.WriteTimeout, httpx.PoolTimeout, httpx.ReadError,
                httpx.WriteError, httpx.RemoteProtocolError,
                httpx.HTTPStatusError) as exc:
            raise ChatHealthyException(
                mode="page_unreachable",
                component="universal_navigation_tool",
                message=f"FindCare {path} call failed: "
                        f"{type(exc).__name__}: {exc}",
                exception=exc,
            )

    async def _tell_page_detail_opened(self, deps: AgentDeps, page: str,
                                       record_id: str) -> None:
        """This page is showing this record."""
        await self._tell_page(deps, "/page/detail-open",
                              {"page": page, "record_id": record_id})

    async def _tell_page_detail_closed(self, deps: AgentDeps,
                                       page: str) -> None:
        """This page has stopped showing a record."""
        await self._tell_page(deps, "/page/detail-close", {"page": page})

    async def _ask_what_the_page_needs(self, deps: AgentDeps,
                                       raw: dict) -> None:
        """Say what the page still needs, in the page's own words.

        A page that cannot run answers with a question rather than an empty
        window, and the question is authored where the requirement is known
        -- the page holds the declaration saying which of its attributes it
        cannot run without. The gateway carries it and reads none of it, so
        an attribute made required in the declaration is asked for without
        anything here changing.

        Streamed as a prompt, which is an answering kind, so the turn has
        answered and the manufactured question does not also fire.
        """
        question = (raw or {}).get("refinement_question")
        if not question:
            return
        from chathealthy_lib.authentication.agent_deps import (
            append_system_utterance)
        deps.stream({"kind": "prompt", "data": {"text": question}})
        append_system_utterance(deps.user_object, question)

    # Which page a turn with nothing to show should be asked about. A
    # gesture names its page; an utterance that produced nothing was about
    # finding a care giver, which is the page every unnamed turn lands on.
    _PAGE_OF_OP = {
        "provider_page": (INDIVIDUAL_PROVIDER, "ProviderSearch"),
        "facility_page": (FACILITY, "FacilitySearch"),
        "apply_filter": (INDIVIDUAL_PROVIDER, "ProviderSearch"),
        "utterance": (INDIVIDUAL_PROVIDER, "ProviderSearch"),
    }

    @staticmethod
    def _answers_the_turn(event: dict) -> bool:
        """Whether this event is an answer, or only an annotation of the ask.

        A prompt that says how a word was read -- "San Francisco (corrected
        from 'san fransisco')" -- tells the person nothing about their
        request. Counted as an answer it ends the turn, and a search that
        could not run because no state was named leaves the person holding
        a spelling note and no way forward.

        So a prompt carrying corrections answers only if it also asks.
        Everything else answers as it always did.
        """
        if event.get("kind") != "prompt":
            return True
        data = event.get("data") or {}
        if not data.get("corrections"):
            return True
        return "?" in str(data.get("text") or "")

    async def _ask_the_page_what_it_needs(self, deps: AgentDeps,
                                          op: str) -> bool:
        """Ask the page why it had nothing, and say so to the person.

        True when the page asked something. False when it had nothing to
        ask, which leaves the manufactured question as it was.
        """
        placed = self._PAGE_OF_OP.get(op)
        if not placed:
            return False
        utterance, dialogue = latest_utterance_and_prior_dialogue(
            deps.user_object)
        page, tool = placed
        raw = await self._tell_page(
            deps, "/page/what-is-needed",
            {"page": page, "tool": tool,
             "utterance": utterance, "history": dialogue})
        question = (raw or {}).get("refinement_question")
        if not question:
            return False
        from chathealthy_lib.authentication.agent_deps import (
            append_system_utterance)
        deps.stream({"kind": "prompt", "data": {"text": question}})
        append_system_utterance(deps.user_object, question)
        return True

    async def _ensure_the_turn_answered(
        self, deps: AgentDeps, op: str, kinds_seen: set[str],
    ) -> None:
        """A turn ends having shown something or having asked something.

        Ending silent is neither, and it is what the person sees as a dead
        screen with no error on it. When a turn reaches here having put
        nothing on any window and said nothing, the answer is the question
        -- authored by UM's manufacture path (EPIC-002-F-010-S-003) from
        the facts naming why there was nothing, so the person can say the
        thing that would let the search run.

        This sits at the one place every event passes through rather than
        on each branch that can come up empty, because the branch that
        comes up empty is exactly the branch nobody thought of.
        """
        if op not in self._TURN_OPS:
            return
        if kinds_seen & self._ANSWERING_KINDS:
            return

        params = deps.user_object.userParameters
        reason: dict[str, Any] = {
            "gesture": op,
            "outcome": "the turn produced nothing for any window",
        }
        complaint = params.get(NUCC, "complaint")
        if complaint:
            reason["prior_complaint"] = complaint
        geography = geography_of(params, INDIVIDUAL_PROVIDER)
        stated_geography = {
            k: v for k, v in
            (geography.model_dump(exclude_none=True) if geography else {}).items()
            if v
        }
        if stated_geography:
            reason["prior_partial_geography"] = stated_geography
        else:
            reason["missing_slot"] = "geography"
        in_force = self._specialties_in_force(deps)
        if in_force:
            reason["specialties_in_force"] = len(in_force)

        log.warning(
            "UR: op=%s ended with nothing on any window; asking instead. %s",
            op, reason)

        # The page is asked first, because the page is what knows: it holds
        # the declaration naming what it cannot run without, and it reads
        # its own parameters to see which are missing. The question names
        # the thing by the word the declaration uses, so making an
        # attribute required is enough to have it asked for.
        answered = await self._ask_the_page_what_it_needs(deps, op)
        if answered:
            return
        from UtteranceManager import utterance_manager as utterance_manager_module
        await utterance_manager_module.TOOL.run_and_log(
            deps,
            utterance_manager_module.Request(
                trigger_type="manufacture",
                manufacture_utterance_reason=reason,
            ),
        )

    # Which page an action dispatches to. Carry-over is applied per
    # DESTINATION, which is what keeps it per destination: the same value
    # carries to one page and not to another because the dispatch is to
    # one page.
    _CARRY_OVER_DESTINATION_OF: dict[str, str] = {}

    async def _apply_carry_over(self, deps: AgentDeps,
                                destination: Optional[str]) -> None:
        """Fill the destination page's empty attributes from stated triples.

        Carry-over is a set of stated triples -- parameter, source page,
        destination page -- and nothing else produces it. The absence of a
        triple is the refusal: there is no rule that a parameter of the
        same name carries between pages.

        It fills, it does not overwrite. A person who has already said
        something about the destination page keeps what they said. Where it
        does apply it is an ordinary write through the one write path, so
        it validates like any other and is recorded like any other, naming
        the page it came from.
        """
        if not destination:
            return
        from chathealthy_lib.runtime_data_collections import carry_over_triples
        from UserParameters import user_parameters_tool

        params = deps.user_object.userParameters
        changes = []
        sources: list[str] = []
        for triple in carry_over_triples(destination):
            attribute = triple.get("parameter")
            source = triple.get("from")
            if not attribute or not source:
                continue
            if params.has(destination, attribute):
                continue
            if not params.has(source, attribute):
                continue
            changes.append(user_parameters_tool.Change(
                page=destination, name=attribute,
                value=params.get(source, attribute)))
            sources.append(source)
        if not changes:
            return
        # One write per source page, so the entry can name the page the
        # value came from -- which is what the panel shows.
        for change, source in zip(changes, sources):
            await user_parameters_tool.TOOL.run_and_log(
                deps,
                user_parameters_tool.Request(
                    verb="set", route="gateway", origin="deterministic",
                    carried_from=source, changes=[change],
                ),
            )

    async def _dispatch_target_action(self, deps: AgentDeps, document,
                                      target_action: str) -> None:
        """Dispatch the tool that owns this target_action. Tools may mutate
        user_object.intent before returning; the caller loops and re-dispatches.
        """

        # Carry-over is applied at dispatch, per destination page, and
        # nowhere else.
        await self._apply_carry_over(deps, _ACTION_PAGES.get(target_action))

        if target_action == "safetyLockout":
            # UM judged the latest utterance signals immediate medical
            # attention. LockoutTool's Task C inserts the
            # {env}_Safety.emergency_incidents record, stamps user_object
            # state, streams the deterministic "why" + Skip phone number,
            # and morphs target_action to closeConnection200.
            from LockoutTool import lockout_tool
            await lockout_tool.TOOL.run_and_log(deps, lockout_tool.Request())

        elif target_action == "closeConnection200":
            # The turn is over and no page was asked for anything. Nothing
            # is resolved and nothing is repainted: the panel in force
            # stands.
            from CloseConnection200Tool import close_connection_200_tool
            await close_connection_200_tool.TOOL.run_and_log(
                deps, close_connection_200_tool.Request(),
            )

        elif target_action == "specialtySearch":
            # The NUCC page mines its own complaint from the talk and
            # writes it, the panel it offers and the codes that panel is
            # ticked with to its own page. The gateway carries the turn
            # across the wire and paints what comes back; it reads no
            # specialty parameter and writes none.
            raw = await post_to_findcare(
                "/specialty/find", deps, "specialty_search_unavailable")
            specialties = raw.get("specialties") or []
            if specialties:
                # A panel is painted when there are rows to paint. A run
                # that matched nothing says nothing about the panel, so
                # what the person is looking at stays and the turn -- having
                # shown nothing new -- ends by asking rather than by going
                # quiet.
                deps.stream({
                    "kind": "specialties",
                    "data": {
                        "specialties": specialties,
                        "homeopathic_generalists": [],
                        "selected_codes": raw.get("selected_codes") or [],
                        "complaint": raw.get("complaint") or "",
                        "all_codes": raw.get("all_codes") or [],
                        "prescriber_codes": raw.get("prescriber_codes") or [],
                        "homeopathic_codes": raw.get("homeopathic_codes") or [],
                        "default_selected_codes":
                            raw.get("default_selected_codes") or [],
                    },
                })
            # No inner "final" emission — _run_pipeline_then_finalize
            # emits the single canonical final event with full payload.

        elif target_action == "findClinicalTrials":
            # EPIC-006-F-005 — the clinical-trial page mines its own
            # parameters from the talk and writes them to its own page.
            # The dispatcher carries the utterance across and streams the
            # trials back; the criteria are announced by the page, on the
            # same stream, because this side has not read the utterance.
            from authentication import clinical_trials_dispatcher
            utterance, dialogue = latest_utterance_and_prior_dialogue(
                deps.user_object)
            await clinical_trials_dispatcher.TOOL.run_and_log(
                deps,
                clinical_trials_dispatcher.Request(
                    utterance=utterance, history=dialogue),
            )

        elif target_action == "findAFacility":
            # The facility page mines its own parameters from the talk and
            # writes them to its own page. The gateway carries the turn
            # across the wire and paints what comes back; it reads no
            # facility parameter and writes none, so the domain knowledge
            # of what a facility search is made of stays in FindCare.
            import httpx
            from authentication import provider_search_tool

            dialogue: list[dict[str, str]] = []
            for u in deps.user_object.session_conversation_history.utterances:
                actor = getattr(u, "actor", None) or (u.get("actor") if isinstance(u, dict) else None)
                text = getattr(u, "text", None) or (u.get("text") if isinstance(u, dict) else "")
                if actor not in ("person", "system") or not text:
                    continue
                dialogue.append({"actor": actor, "text": str(text).strip()})
            utterance = ""
            for position in range(len(dialogue) - 1, -1, -1):
                if dialogue[position]["actor"] == "person":
                    utterance = dialogue[position]["text"]
                    dialogue = dialogue[:position]
                    break

            url = provider_search_tool.findcare_url() + "/facility/find"
            try:
                async with httpx.AsyncClient(timeout=None, verify=False) as client:
                    r = await client.post(url, json={
                        # The token this hop already holds, forwarded so
                        # FindCare can verify the SharedServices signature.
                        "session_token": deps.session_token.model_dump(mode="json"),
                        "utterance": utterance,
                        "history": dialogue,
                    })
                    r.raise_for_status()
                    raw = r.json()
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                    httpx.WriteTimeout, httpx.PoolTimeout, httpx.ReadError,
                    httpx.WriteError, httpx.RemoteProtocolError,
                    httpx.HTTPStatusError) as exc:
                raise ChatHealthyException(
                    mode="facility_search_unavailable",
                    component="universal_navigation_tool",
                    message=f"FindCare /facility/find call failed: "
                            f"{type(exc).__name__}: {exc}",
                    exception=exc,
                )

            mined = raw.get("mined") or {}
            deps.stream({
                "kind": "intent_classified",
                "data": {
                    "action": "findAFacility",
                    "criteria": (mined.get("facility_name")
                                 or mined.get("facility_type")
                                 or "facilities"),
                },
            })
            self._stream_facilities(deps, raw)
            await self._write_position(deps, FACILITY,
                                       raw.get("first_npi"), raw.get("last_npi"))
            await self._ask_what_the_page_needs(deps, raw)

        elif target_action == "findAProvider":
            # The individual-provider page mines its own parameters from
            # the talk and writes them to its own page. The gateway
            # carries the turn across the wire and paints what comes back;
            # it reads no provider parameter and writes none, so the domain
            # knowledge of what a care-giver search is made of stays in
            # FindCare.
            raw = await post_to_findcare(
                "/provider/find", deps, "provider_search_unavailable")
            specialties = raw.get("offered_specialties") or []
            if specialties:
                deps.stream({
                    "kind": "specialties",
                    "data": {
                        "specialties": specialties,
                        "homeopathic_generalists": [],
                        "selected_codes": raw.get("selected_specialty_codes") or [],
                        "complaint": raw.get("complaint") or "",
                        # The sets the panel ticks as one gesture, and what
                        # a fresh panel arrives ticked with. Carried, not
                        # read: which codes are in a set is the page's.
                        "all_codes": raw.get("all_codes") or [],
                        "prescriber_codes": raw.get("prescriber_codes") or [],
                        "homeopathic_codes": raw.get("homeopathic_codes") or [],
                        "default_selected_codes":
                            raw.get("default_selected_codes") or [],
                    },
                })
            providers = raw.get("providers") or []
            if providers:
                data = {
                    "providers": providers,
                    "has_more": bool(raw.get("has_more", False)),
            "has_previous": bool(raw.get("has_previous", False)),
                    "first_npi": raw.get("first_npi"),
                    "last_npi": raw.get("last_npi"),
                    "count": int(raw.get("count", 0) or 0),
                    "total_count": int(raw.get("total_count", 0) or 0),
                    "page_start": int(raw.get("page_start", 1) or 1),
                    "page_end": int(raw.get("page_end", 0) or 0),
                    "search_params": raw.get("search_params"),
                    "specialization_options": raw.get("specialization_options"),
                    "state": raw.get("state"),
                    "refinements": raw.get("refinements"),
                    "summary_message": raw.get("summary_message"),
                }
                deps.stream({
                    "kind": "providers",
                    "data": {name: value for name, value in data.items()
                             if value is not None},
                })
            await self._reconcile_open_detail(deps, providers)
            await self._ask_what_the_page_needs(deps, raw)
            # No inner "final" emission — outer pipeline emits the
            # canonical final event with full payload.

        else:
            # An action naming no tool here is a turn nobody can serve, and
            # the person is owed a question rather than an exception. The
            # end-of-turn path asks, because nothing was shown and nothing
            # was said.
            log.warning("UR: no tool for target_action %r; asking instead",
                        target_action)

    async def _run_or_cache_specialty_filter(
            self, deps: AgentDeps, complaint: str,
            complaint_changed: bool = True) -> list[dict]:
        """No dispatch reaches this. The specialty resolution moved to the
        pages that need it -- the NUCC page and the individual-provider
        page, each resolving its own complaint on its own server -- so the
        only callers left are the tests over this file.

        Look for a cached nucc_codes argument on the IntentDocument's
        specialtySearch and findAProvider entries. On a cache hit, parse
        the cached value and stream it back to the FE as a
        {kind:"specialties"} event without re-running SpecialtyFilter.
        On a cache miss, run SpecialtyFilter, then write nucc_codes back
        onto both intent entries (whichever exist) so subsequent turns hit
        the cache.

        Returns the list of {code, name, score, ...} dicts.
        """
        import json as json
        from chathealthy_lib.authentication.intent_document import Argument

        document = deps.user_object.intent
        if document is None:
            return []

        # The cache is keyed by the query the codes were computed from.
        # Neither of the two previous shapes was: the first keyed by the
        # IntentDocument, so a dental query's codes came back for "find me
        # a bone doctor" (2026-06-21); the second removed caching, so
        # Apply Filter regenerated the panel the user was mid-way through
        # choosing from (2026-06-10). One bug was traded for the other
        # because the key was never the thing that decides.
        #
        # A new utterance is a different query and re-runs. Apply Filter
        # produces no utterance, so the query is unchanged and the panel
        # the user is looking at is the panel they keep.

        # Restore prior behavior: SpecialtyFilter sees the VERBATIM latest
        # person utterance, not UM's narrow `complaint` extraction. The
        # filter's prompt scores user-named roles ("nurse practitioner",
        # "acupuncturist", etc.) at 1.0 with adjacents <=0.7 — but only if
        # the role name actually reaches it. UM's complaint extraction
        # strips role names, so the filter must source the raw utterance.
        raw_utterance = ""
        for u in reversed(deps.user_object.session_conversation_history.utterances):
            actor = getattr(u, "actor", None) or (u.get("actor") if isinstance(u, dict) else None)
            text = getattr(u, "text", None) or (u.get("text") if isinstance(u, dict) else "")
            if actor == "person" and text:
                raw_utterance = str(text).strip()
                break
        query = raw_utterance or complaint
        cached = read_nucc_codes_cache(document)

        # A turn that names no new complaint is not a new question -- it
        # narrows the one in force. "now find me the male docs" is about the
        # same bone doctors, so it keeps the panel it was given: the cache is
        # honoured whatever this turn's words were, SpecialtyFilter does not
        # run, and the panel is not repainted. Re-resolving cost a second LLM
        # pass and returned a different set of codes each time, so the
        # narrowing landed on a list the person was not looking at -- 11 of
        # the Medicare providers were male on screen and the answer came back
        # 9.
        # A complaint that did not change asks the specialty filter nothing,
        # so it is not handed off to. The panel in force stands.
        #
        # This used to be keyed on the verbatim utterance, which handed off
        # on every turn whose words differed -- answering "yes" to "did you
        # mean California?" was scored as a description of care, and a
        # psychiatry panel of 45 came back as 57 codes with chiropractors in
        # it and Psychologist gone.
        if not complaint_changed:
            in_force = cached or self._specialties_in_force(deps)
            if in_force:
                await self._set_specialties_parameter(deps, in_force)
                return in_force

        from SpecialtyFilter import specialty_filter_tool
        fs = await specialty_filter_tool.TOOL.run_and_log(
            deps, specialty_filter_tool.Request(query=complaint),
        )
        if fs.error:
            # A service that failed and a search that matched nothing took
            # the same branch, so a 503 from /nucc/classify reached the rest of
            # the turn as an empty result. The turn then had nothing to
            # show, and the end-of-turn check asked the person for what
            # they had already given -- a complaint and a city that were
            # both on the document. The person is told a service is down
            # rather than questioned about their own sentence.
            await self._dispatch_llm_unavailable_dialogue(
                deps,
                ChatHealthyException(
                    mode="llm_unavailable",
                    message=f"specialty filter unavailable: {fs.error}",
                    component="SpecialtyFilterTool",
                    context={"provider": "classify", "call_site": "specialty_filter"},
                ))
            return []
        if not fs.specialties:
            return []

        specialties = [s.model_dump(exclude_none=True) for s in fs.specialties]
        encoded = json.dumps(specialties)

        # Write nucc_codes back onto every applicable intent entry so the
        # next turn (after the user resolves a pending disambiguation) sees
        # the cached value. Rebuild each affected entry through Pydantic so
        # validation runs on the new argument list.
        new_intents = []
        for entry in document.intents:
            if entry.name in ("specialtySearch", "findAProvider"):
                kept_args = [a for a in entry.arguments
                             if a.name not in ("nucc_codes", "nucc_codes_query")]
                kept_args.append(Argument(
                    name="nucc_codes", value=encoded, type="array", required=False,
                ))
                # The key travels with the value. Without it the next turn
                # cannot tell whether these codes belong to its question.
                kept_args.append(Argument(
                    name="nucc_codes_query", value=query, type="string",
                    required=False,
                ))
                new_intents.append(type(entry).model_validate({
                    **entry.model_dump(),
                    "arguments": [a.model_dump() for a in kept_args],
                }))
            else:
                new_intents.append(entry)
        from chathealthy_lib.authentication.intent_document import IntentDocument as _IntentDocument
        deps.user_object.intent = _IntentDocument(
            target_action=document.target_action,
            intents=new_intents,
            user_message=document.user_message,
        )

        await self._set_specialties_parameter(deps, specialties)

        # The specialty pipeline's normalize step emits two readings of the
        # utterance: the search term it embeds with, and the complaint. The
        # classifier also produces a complaint, but it is normalizing for a
        # specialty search -- "orthodontist" -- which names who you see and
        # not what you have. This one says "tooth problem", so it wins.
        if fs.complaint:
            from UserParameters import user_parameters_tool
            await user_parameters_tool.TOOL.run_and_log(
                deps,
                user_parameters_tool.Request(
                    verb="set", page=NUCC, name="complaint",
                    value=fs.complaint,
                    route="gateway", origin="non_deterministic",
                ),
            )

        return specialties

    @staticmethod
    def _specialties_in_force(deps) -> list:
        """The panel the person is looking at, from live state."""
        params = getattr(deps.user_object, "userParameters", None)
        offered = (params.get(NUCC, "offeredSpecialties") or []) if params else []
        return [s.model_dump(exclude_none=True) if hasattr(s, "model_dump") else dict(s)
                for s in offered]

    async def _set_specialties_parameter(
        self, deps: AgentDeps, specialties: list[dict],
    ) -> None:
        """Publish the offered panel, and the selection it is drawn with.

        The panel paints prescribers checked and everything else clear, so
        the search has to be the checked set or the screen is lying: the
        user saw psychiatrists ticked and chiropractors not, and got
        chiropractors, because an empty selection meant "search everything
        offered" rather than "search what is checked".

        Seeding the selection here makes the two the same set by
        construction. Unchecking a row and applying then narrows from a
        real starting point rather than from a set nobody chose.
        """
        from UserParameters import user_parameters_tool
        await user_parameters_tool.TOOL.run_and_log(
            deps,
            user_parameters_tool.Request(
                verb="set", page=NUCC, name="offeredSpecialties",
                value=specialties,
                route="gateway", origin="non_deterministic",
            ),
        )
        # ONLY when the user has not chosen yet. Seeding unconditionally
        # overwrote the choice they had just made: Apply Filter writes the
        # ticked codes, then dispatches findAProvider, which comes back
        # through here and replaced them with the prescriber default. The
        # user picked Orthodontics and got the default set, because the
        # default was written after their choice.
        #
        # A choice is deterministic and outranks a default. The default
        # exists for the turn where there is nothing to outrank it.
        if not deps.user_object.userParameters.has(
                NUCC, "selectedSpecialtyCodes"):
            prescribers = [s.get("code") for s in specialties
                           if s.get("can_prescribe") and s.get("code")]
            await user_parameters_tool.TOOL.run_and_log(
                deps,
                user_parameters_tool.Request(
                    verb="set", route="gateway", origin="deterministic",
                    changes=[
                        user_parameters_tool.Change(
                            page=NUCC, name="selectedSpecialtyCodes",
                            value=prescribers),
                        user_parameters_tool.Change(
                            page=INDIVIDUAL_PROVIDER,
                            name="selectedSpecialtyCodes",
                            value=prescribers),
                    ],
                ),
            )

    # ── /gate orchestration entry point ───────────────────────────

    async def handle_gate(self, gate_req: GateRequest) -> GateResponse:
        """The whole /gate request lifecycle minus the HTTP envelope.

        `/gate` is now thin HTTP plumbing: it parses the body + cookie,
        constructs a `GateRequest`, calls this method, and turns the
        returned `GateResponse` into a FastAPI Response. Everything
        else — wire-intent validation, session load, AuthN dispatch,
        OAuth-start URL manufacture, cookie value assembly, AgentDeps
        construction, op dispatch, stream feed, AUTHN persist, final
        event construction — lives here.
        """
        # 1. Wire-intent validation (gateway concern).
        if gate_req.intent is not None and gate_req.intent not in KNOWN_WIRE_INTENTS:
            raise ChatHealthyException(
            mode="value_error",
            component="universal_navigation_tool",
            message=f"/gate: unknown intent {gate_req.intent!r}; expected one of "
                f"{sorted(KNOWN_WIRE_INTENTS)} or absent")

        # 2. Mongo handle + AuthnDeps.
        mongo_frontend = authn.get_mongo_frontend()
        authn_deps = AuthnDeps(
            session_guid=gate_req.session_guid,
            server_env=ENV,
            mongo_frontend=mongo_frontend,
        )

        # 3. Session load + auth_intent decision.
        loaded_user_object: Optional[UserObject] = None
        if gate_req.session_guid:
            sessions_coll = mongo_frontend[authn.SESSION_DB][authn.SESSION_COLLECTION]
            session_doc = sessions_coll.find_one({"_id": gate_req.session_guid})
            if session_doc:
                try:
                    candidate = UserObject.model_validate(
                        {k: v for k, v in session_doc.items() if k != "_id"}
                    )
                    expires_at = candidate.expires_at
                    if expires_at.tzinfo is None:
                        expires_at = expires_at.replace(tzinfo=timezone.utc)
                    # A session continues only while its nonce is inside
                    # the window. The nonce's latest stamp is the last hop
                    # this session made, so a lapsed one means nobody has
                    # been here for longer than a session is allowed to be
                    # quiet. The gateway decides this and nothing else does.
                    stored_token = candidate.current_session_token
                    nonce_live = isinstance(stored_token, SessionToken) and (
                        not Nonce.is_expired(stored_token.get_nonce(),
                                             SESSION_TTL_SECONDS))
                    if expires_at > datetime.now(timezone.utc) and nonce_live:
                        loaded_user_object = candidate
                except Exception as exc:
                    # Mode 2 (REQ-B-008): persisted session doc can't be
                    # deserialized into a UserObject — user loses their
                    # state and gets fresh-minted as guest on this turn.
                    # User-affecting → log.error + always-log.
                    log.error("UR: persisted session could not be read; the "
                              "user is fresh-minted as a guest this turn: %s",
                              exc,
                              exc=ChatHealthyException(
                                  mode="ur_session_unreadable",
                                  message=f"UR: persisted session could not be "
                                          f"read, user fresh-minted: {exc}",
                                  component="UniversalNavigationTool",
                                  exception=exc if isinstance(exc, Exception) else None,
                              ))

        if loaded_user_object is not None:
            auth_intent = "manage_session"
            inbound_user_object = loaded_user_object
            sch_dump = loaded_user_object.session_conversation_history.model_dump()
            intent_dump = (
                loaded_user_object.intent.model_dump()
                if loaded_user_object.intent is not None else None
            )
        else:
            auth_intent = "manufacture_session"
            inbound_user_object = UserObject(
                current_session_token="NULL",
                expires_at=datetime.now(timezone.utc)
                + timedelta(seconds=SESSION_TTL_SECONDS),
            )

        # 4. Call AUTHN_TOOL.run.
        authn_resp = await authn.TOOL.run(
            authn_deps,
            authn.Request(intent=auth_intent, user_object=inbound_user_object),
        )
        user_object = authn_resp.user_object
        fresh_mint = authn_resp.fresh_mint
        guid = user_object.current_session_token.get_auth_token()

        # Bind the user_object's GUID to the logging context for this
        # async task. Every log emitted from here on (handle_gate +
        # downstream tools + middleware request-end log) carries
        # session_guid + user_action=true automatically. The developer
        # never harvests the GUID.
        from chathealthy_lib.logging_service import bind_user_object_to_log
        bind_user_object_to_log(user_object)

        # Stamp client IP onto user_object at the HTTP boundary so tools
        # (SafetyLockoutTool in particular) never touch the Request. UR's
        # subsequent _hydrate_lockout_if_any consumes this in
        # _handle_utterance.
        if gate_req.client_ip:
            user_object.ip_address = gate_req.client_ip

        # 5. (login_register short-circuit removed — that flow now goes
        #    direct to /auth/google/start as a form-target popup post
        #    which routes through OAuthLoginTool start phase. /gate's
        #    only client-facing intent is utterance traffic.)

        # 6. Build event queue + stream sink + AgentDeps + nav.Request.
        event_queue: asyncio.Queue = asyncio.Queue()
        _SENTINEL = object()

        kinds_seen: set[str] = set()

        def stream_sink(event: dict) -> None:
            kind = event.get("kind")
            if isinstance(kind, str) and self._answers_the_turn(event):
                kinds_seen.add(kind)
            event_queue.put_nowait(event)

        # Read if a call carries it. Session establishment is
        # /auth/issue, which is where the form factor is reported and
        # where the session that holds it is made.
        reported = str((gate_req.payload or {}).get("form_factor") or "").strip().lower()
        if reported in self.FORM_FACTORS and not user_object.form_factor:
            user_object.form_factor = reported

        agent_deps = AgentDeps(
            user_object=user_object,
            session_token=user_object.current_session_token,
            mongo_frontend=mongo_frontend,
            server_env=ENV,
            stream=stream_sink,
        )
        nav_req = self.Request(op=gate_req.op, payload=gate_req.payload)

        # An op whose answer is a file emits no events, so it does not go
        # through the streaming pipeline. It still arrives here -- one
        # entrance, one session validation, one place that hydrates the
        # user -- and returns bytes instead of a stream.
        if gate_req.op in FILE_OPS:
            resp = await self.run(agent_deps, nav_req)
            await authn.TOOL.persist(authn_deps, user_object, fresh_mint)
            return GateResponse(body_kind="file", body_data=resp.result)

        # 7. Pipeline coroutine: run handler dispatch, persist, push the
        #    final_event, push the sentinel. Wrapped in try/finally so the
        #    sentinel is always pushed even if the orchestration crashes
        #    (otherwise the stream consumer would hang).
        async def _run_pipeline_then_finalize() -> None:
            nav_exc_local: Optional[BaseException] = None
            res_local: Optional[Response] = None
            try:
                try:
                    res_local = await self.run(agent_deps, nav_req)
                except Exception as exc:
                    nav_exc_local = exc
                    # Mode 2 (REQ-B-008): UR's broad Exception catch surfaces
                    # the failure to the user via the final ok:false stream
                    # event (NOT a 503). Per the taxonomy, only the catch-
                    # all @app.exception_handler does Mode 3 fatal_error.
                    #
                    # It is recorded here because this is the catch site, and
                    # a catch that says nothing is a failure nobody can see:
                    # a tool raised, the turn answered final ok:false, the
                    # screen did not change and the log held nothing at all.
                    # Selecting a provider failed this way and was invisible
                    # until an operator clicked and nothing happened.
                    #
                    # This catch needs follow-on work: discriminate on
                    # exc.mode for known ChatHealthyException modes (Mode 1
                    # or Mode 2 per mode); for non-ChatHealthyException,
                    # re-raise so the safety net handles it as Mode 3.
                    log.error("UR: op=%s failed: %s", gate_req.op, exc,
                              exc=ChatHealthyException(
                                  mode="ur_operation_failed",
                                  message=f"UR: op={gate_req.op!r} failed: {exc}",
                                  component="UniversalNavigationTool",
                                  exception=exc if isinstance(exc, Exception) else None,
                              ))

                if nav_exc_local is None:
                    try:
                        await self._ensure_the_turn_answered(
                            agent_deps, gate_req.op, kinds_seen)
                    except Exception as exc:
                        # Failing to ask must not also cost the turn its
                        # final event -- that would leave the stream with
                        # no terminator and the client waiting forever.
                        log.error(
                            "UR: op=%s ended with nothing shown and the "
                            "question could not be authored: %s",
                            gate_req.op, exc,
                            exc=ChatHealthyException(
                                mode="ur_turn_answered_nothing",
                                message=f"UR: op={gate_req.op!r} ended with "
                                        f"nothing on any window and the "
                                        f"manufactured question failed: {exc}",
                                component="UniversalNavigationTool",
                                exception=exc if isinstance(exc, Exception) else None,
                            ))

                session_token_proj = session_token_wire(user_object)
                if nav_exc_local is not None:
                    final_event_local = {
                        "kind": "final", "ok": False,
                        "error": f"{type(nav_exc_local).__name__}: {nav_exc_local}",
                        "guid": guid,
                        "time_remaining_seconds": time_remaining_seconds(user_object),
                        "was_registered": was_registered(user_object),
                        "session_token": session_token_proj,
                    }
                else:
                    final_event_local = {
                        "kind": "final", "ok": True,
                        "guid": guid,
                        "result": res_local.result if res_local else {},
                        "result_kind": res_local.kind if res_local else "unknown",
                        "time_remaining_seconds": time_remaining_seconds(user_object),
                        "was_registered": was_registered(user_object),
                        "session_token": session_token_proj,
                    }

                try:
                    sch_dump = user_object.session_conversation_history.model_dump()
                    intent_dump = (
                        user_object.intent.model_dump()
                        if user_object.intent is not None else None
                    )
                    await authn.TOOL.persist(authn_deps, user_object, fresh_mint)
                except Exception as exc:
                    # Mode 2 (REQ-B-008): session persist to Mongo failed.
                    # Stream continues so the user sees this turn's result,
                    # but next turn will rehydrate stale state. Operator
                    # must know. log.exception (ERROR+traceback) + always-log.
                    log.error("UR: session persist failed; this turn is shown "
                              "but the next rehydrates stale state: %s", exc,
                              exc=ChatHealthyException(
                                  mode="ur_session_persist_failed",
                                  message=f"UR: session persist failed, next "
                                          f"turn rehydrates stale state: {exc}",
                                  component="UniversalNavigationTool",
                                  exception=exc if isinstance(exc, Exception) else None,
                              ))

                event_queue.put_nowait(final_event_local)
            finally:
                event_queue.put_nowait(_SENTINEL)

        # 8. NDJSON streaming: schedule the pipeline as a background task;
        #    return an async iterator that the FastAPI StreamingResponse
        #    will consume chunk-by-chunk.
        if gate_req.want_ndjson:
            asyncio.create_task(_run_pipeline_then_finalize())

            async def _event_stream() -> AsyncIterator[bytes]:
                while True:
                    item = await event_queue.get()
                    if item is _SENTINEL:
                        return
                    yield (json.dumps(item, default=str) + "\n").encode("utf-8")

            return GateResponse(
                body_kind="ndjson_stream",
                body_data=_event_stream(),
            )

        # 9. Non-NDJSON: run inline, drain queue for the final_event.
        await _run_pipeline_then_finalize()
        final_event: Optional[dict] = None
        while True:
            item = await event_queue.get()
            if item is _SENTINEL:
                break
            if isinstance(item, dict) and item.get("kind") == "final":
                final_event = item
        return GateResponse(
            body_kind="json",
            body_data=final_event or {},
        )


TOOL = UniversalNavigationTool()
