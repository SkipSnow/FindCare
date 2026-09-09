# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# main.py — ChatHealthy.ai FindCare backend. Host adapter only.
#
# ARCH-001: All business logic in domain/ services. All config in PromptSystemMaker.
# This file: FastAPI setup, service wiring, chat loop. Nothing else.

# Establish which component this process is and what the library will let it
# load, before any other library capability is imported. The finder installed
# here refuses a forbidden module at import, so a late import inside a
# function is caught the same as one at the top of a file -- which only holds
# if nothing has been imported ahead of this call.
from chathealthy_lib.permissions import initialize as _ch_permissions_init
_ch_permissions_init()

import asyncio
import json
from chathealthy_lib import ChatHealthyLoggingService
import os
import sys
import traceback
from typing import Optional

from anthropic import Anthropic
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests as requests_lib

# FindCare/ on sys.path so business-model tools (SpecialtyFilter,
# ProviderManagement) are importable. Must happen BEFORE the imports
# below that pull from those packages. Dockerfile COPYs FindCare/ into
# /app/FindCare so the same relative walk resolves in the container.

log = ChatHealthyLoggingService()


sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "FindCare"))
# sharedServices/Code/ on sys.path for the same reason: a widget or a
# service lives with the feature that owns it, and several of those
# features are shared ones.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "sharedServices", "Code"))

# ARCH-001 — domain services
from externalInterface.tool_router import ToolRouter
from application.facades.evaluate_care_facade import EvaluateCareFacade
from ProviderManagement.provider_search_service import FindCareService
from SpecialtyFilter.filter import (
    SpecialtyFilter, SECTION_INDIVIDUAL, SECTION_ORGANIZATION,
)
from ClinicalTrials.clinical_trials_service import ClinicalTrialsService
from ProviderDetail.provider_detail_service import ProviderDetailService
from domain.shared.safety.safety_service import SafetyService
from AboutChatHealthy.about_service import AboutService
from ProviderManagement.provider_search_models import ProviderSearchInput, SpecialtyInput
from ProviderManagement.facility_utterance import mine_facility_parameters
from ProviderManagement.individual_provider_utterance import (
    mine_individual_provider_parameters,
)
from ProviderManagement.nucc_utterance import mine_nucc_parameters
from ProviderManagement.clinical_trial_utterance import (
    mine_clinical_trial_parameters,
)
from ClinicalTrials.clinical_trials_models import ClinicalTrialsInput
from application.facades.provider_lookup_models import ProviderLookupInput
from infrastructure.embeddings.embedding_client import EmbeddingClient
from logs.debug_logger import DebugLogger

load_dotenv(override=True)

# Shared utilities
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "Shared"))
from chathealthy_lib.mongo_utilities import ChatHealthyMongoUtilities
from chathealthy_lib import http_request_facts as request_facts
from chathealthy_lib.exceptions import ChatHealthyException
from prompt_system_maker import PromptSystemMaker

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ENV_PREFIX    = os.getenv("ENV_PREFIX", "dev")
DEBUG         = os.getenv("DEBUG", "false").lower() == "true"
HUMAN_TESTING_RAW = os.getenv("HUMAN_TESTING", "false")
HUMAN_TESTING = HUMAN_TESTING_RAW.lower() not in ("false", "0", "")
APP_VERSION   = os.getenv("APP_VERSION", "unknown")

EMERGENCY_RESPONSE = (
    "<b>Call 911 or go to the nearest emergency room immediately. Do not wait.</b>\n\n"
    "<b>This chat has been suspended.</b>"
)

# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
db_manager = None

def get_db():
    global db_manager
    try:
        if db_manager is None:
            db_manager = ChatHealthyMongoUtilities()
        return db_manager.getConnection("frontendUser", "ChatHealthyFrontEnd")
    except Exception as e:
        # Mode 1 (REQ-B-008): recoverable — caller's next call will retry
        # via the same lazy-init path. log.info + default if_not_debug_log
        # so this only emits in debug mode.
        log.info("MongoDB unavailable (will retry next call): %s", e, exc=ChatHealthyException(
                                                                          mode="mongo_unavailable",
                                                                          message=f"MongoDB unavailable (will retry next call): {e}",
                                                                          component="FindCareBackend",
                                                                          exception=e,
                                                                      ))
        db_manager = None
        return None

# ---------------------------------------------------------------------------
# Utilities — push notification + DB write
# ---------------------------------------------------------------------------
SPARKMAIL_API_KEY = os.getenv("SPARKMAIL_API_KEY", "")
SPARKMAIL_FROM    = os.getenv("NOTIFICATION_FROM_EMAIL", "")
SPARKMAIL_TO      = os.getenv("NOTIFICATION_TO_EMAIL", "")

def push(message):
    """Send an operator-notification email via SparkPost.

    Returns:
        {"sent": True}                 — delivered
        {"sent": False, "skipped": ...} — env var missing, intentionally skipped
        {"sent": False, "error": ...}  — SparkPost call raised; caller must
                                         see this and decide what to do
                                         (no silent swallow per the no-fallback
                                         rule).
    """
    if not SPARKMAIL_API_KEY:
        return {"sent": False, "skipped": "SPARKMAIL_API_KEY not configured"}
    try:
        from sparkpost import SparkPost
        SparkPost(SPARKMAIL_API_KEY).transmissions.send(
            recipients=[SPARKMAIL_TO], from_email=SPARKMAIL_FROM,
            subject="ChatHealthy — Activity", text=message,
        )
        return {"sent": True}
    except Exception as exc:
        # Mode 1 (REQ-B-008): SparkPost push notification is best-effort;
        # caller proceeds with the operation regardless. log.info + default
        # debug-gated.
        log.info("SparkPost send failed: %s", exc, exc=ChatHealthyException(
                                                       mode="sparkpost_send_failed",
                                                       message=f"SparkPost send failed: {exc}",
                                                       component="FindCareBackend",
                                                       exception=exc,
                                                   ))
        return {"sent": False, "error": f"{type(exc).__name__}: {exc}"}

def commitSignificantActivity(payload=None, **kwargs):
    """Commit a significant-activity record to MongoDB.

    Failure semantics (NO silent fallbacks):
      - DB unavailable         → {"recorded": "skipped", "reason": "db_unavailable"}
                                 (NOT "ok" — a skipped commit is not a successful one)
      - Bad payload / DB error → {"recorded": "error", "error": "..."} — caller
                                 MUST inspect this; upstream code has no excuse
                                 for treating this as success.
    """
    client = get_db()
    if client is None:
        return {"recorded": "skipped", "reason": "db_unavailable"}
    try:
        payload = payload or kwargs
        if isinstance(payload, str):
            payload = json.loads(payload)
        db_name = f"{ENV_PREFIX}_{payload['database']}"
        coll = client[db_name][payload["collection"]]
        record = dict(payload["record"])
        record["record_number"] = coll.count_documents({}) + 1
        record["datetime"] = dt.datetime.now().isoformat()
        coll.insert_one(record)
        return {"recorded": "ok"}
    except Exception as exc:
        # Mode 2 (REQ-B-008): audit-trail write failed; we return an
        # error dict to the caller (no 503) but operator MUST know — the
        # audit trail is the regulatory artifact, not optional.
        log.error("commitSignificantActivity failed: %s", exc, exc=ChatHealthyException(
                                                                mode="commit_significant_activity_failed",
                                                                message=f"commitSignificantActivity failed: {exc}",
                                                                component="FindCareBackend",
                                                                exception=exc,
                                                            ), if_not_debug_log=True)
        return {"recorded": "error", "error": f"{type(exc).__name__}: {exc}"}

def format_chat_history(messages, truncate: bool = True):
    max_len = 500 if truncate else None
    formatted = []
    for m in messages:
        content = m.get("content") or ""
        if isinstance(content, list):
            parts = [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
            content = " ".join(parts)
        content = str(content)
        if max_len and len(content) > max_len:
            content = content[:max_len] + "..."
        formatted.append({"role": m.get("role", ""), "content": content})
    return formatted

# ---------------------------------------------------------------------------
# PromptSystemMaker — loads all config from brain artifacts
# ---------------------------------------------------------------------------
# Brain dir: try local repo structure first, fall back to HuggingFace flat layout
brain_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..", "brain")
if not os.path.isdir(brain_dir):
    brain_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brain")
prompt_maker = PromptSystemMaker(brain_dir=brain_dir, env_prefix=ENV_PREFIX)
EMERGENCY_KEYWORDS = prompt_maker.load_emergency_keywords()
anthropic_tools = prompt_maker.load_tool_definitions()
WELCOME_MESSAGE = PromptSystemMaker.build_welcome_message()
# Build/version/framework: live from MongoDB per EPIC-008-F-004-S-001

ME_DIR = os.getenv("ME_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "me")
if not os.path.isdir(ME_DIR):
    ME_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "ChatHealthyWhoAmIChat", "me")
ME = prompt_maker.load_me_context(ME_DIR)

# UAT report
# UAT report: local repo path or HF flat layout
def system_prompt(follow_up_check: bool = False) -> str:
    return prompt_maker.build_system_prompt(emergency_response=EMERGENCY_RESPONSE, follow_up_check=follow_up_check)

# ---------------------------------------------------------------------------
# Service initialization — ARCH-001
# ---------------------------------------------------------------------------
embedding_client = EmbeddingClient()

specialty_service = SpecialtyFilter(
    get_db_fn=get_db, env_prefix=ENV_PREFIX,
    get_vector_fn=embedding_client.get_specialty_vector)
find_care = FindCareService(
    get_db_fn=get_db, env_prefix=ENV_PREFIX, specialty_service=specialty_service)

clinical_trials_service = ClinicalTrialsService()
provider_detail_service = ProviderDetailService()
evaluate_care_facade = EvaluateCareFacade(
    clinical_trials=clinical_trials_service, provider_detail=provider_detail_service, find_care_facade=find_care)

safety_service = SafetyService(get_db_fn=get_db, env_prefix=ENV_PREFIX, emergency_keywords=EMERGENCY_KEYWORDS)
about_service = AboutService(me_context=ME, trim_fn=PromptSystemMaker.trim)

debug_logger = DebugLogger(get_db_fn=get_db, env_prefix=ENV_PREFIX)

# ToolRouter — F-05 fix
tool_router = ToolRouter()
tool_router.register_with_models([
    ("find_providers",          find_care.search_providers,            ProviderSearchInput),
    ("find_specialty_codes",    find_care.identify_specialty,          SpecialtyInput),
    ("search_clinical_trials",  evaluate_care_facade.search_clinical_trials,  ClinicalTrialsInput),
    ("lookup_provider_external", evaluate_care_facade.get_provider_details,   ProviderLookupInput),
    ("get_skip_snow_context",   about_service.get_skip_snow_context),
    ("get_chathealthy_context", about_service.get_chathealthy_context),
    ("commitSignificantActivity", commitSignificantActivity),
])
log.info("ToolRouter initialized: %s", tool_router.registered_tools)

def handle_tool_calls(tool_use_blocks, messages):
    return tool_router.handle_tool_calls(tool_use_blocks, messages, format_chat_history)

# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
import time as time_mod

app = FastAPI(title="ChatHealthy FindCare API")


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
    return JSONResponse(status_code=status, content={"detail": exc.message})

from chathealthy_lib.runtime_data_collections import (  # noqa: E402
    declared_attributes, optional_parameters, required_parameters)
from ProviderManagement.utterance_mining import ask_for_missing  # noqa: E402
from chathealthy_lib.runtime_data_collections import (
    providers_coll,
    specialty_meta_coll,
    bind_from_manifest as bind_data_collections,
    router as data_collections_router,
)

import datetime as dt
from fastapi.responses import JSONResponse as JSONResponse


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
        content={"service": "FindCare", "source": "unhandled",
                 "time": dt.datetime.now(dt.timezone.utc).isoformat()},
    )

# v2.2 Part B 7.4 — startup Mongo probe. Construct the canonical utility
# then issue an explicit ping; failure raises and the container crashes,
# HF (or local docker) restarts, and the operator sees the restart loop
# and reads the logs. Steady-state degraded mode in _get_db() remains for
# transient runtime blips; only the startup probe is mandatory-loud.
_startup_db_probe = ChatHealthyMongoUtilities()
_startup_db_probe.getConnection("frontendUser", "ChatHealthyFrontEnd").admin.command("ping")
log.info("FindCare backend Mongo startup probe: ping OK")

# EPIC-010-F-101-S-005 (Data version management): bind runtime data
# collections from ChatHealthyConfig.DBVersions on startup, and mount the
# /admin/swap + /debug/active_collections endpoints.
bind_data_collections()
app.include_router(data_collections_router)


# ── EPIC-002-F-001-S-012: startup security-primitive verification ──
# FindCare's security primitives are nonce restamp (signs with findcare.key)
# and verify (reads peer certs). The probe loads both findcare.key and
# findcare.crt to confirm CERTS_DIR is bootstrapped. Exit codes per sysexits.h:
#   78 (EX_CONFIG)    — missing key or cert file
#   77 (EX_NOPERM)    — permission denied on cert/key
#   70 (EX_SOFTWARE)  — unexpected internal error
def decode_cert_pem(env_var: str, b64_value: str) -> bytes:
    """Decode one PEM env var. Raises on malformed base64. No logging here —
    the caller decides what to do with the failure."""
    import base64
    try:
        return base64.b64decode(b64_value.strip())
    except Exception as exc:
        raise ChatHealthyException(
            mode="startup_invalid_base64",
            message=f"STARTUP: {env_var} is present but not valid base64: {exc}",
            component="FindCareBackend",
            exception=exc,
        )


def try_chmod_0600(path: str) -> None:
    """Best-effort restrict file mode. Logs and continues on failure (Windows
    or non-POSIX filesystems return non-fatal errors). Never raises."""
    try:
        os.chmod(path, 0o600)
    except Exception as exc:
        # Mode 1 (REQ-B-008): best-effort startup chmod; system continues
        # without the restriction. log.info + default debug-gated.
        log.info(
            "STARTUP: chmod 0600 on %s failed (continuing): %s", path, exc,
            exc=ChatHealthyException(
                mode="startup_chmod_failed",
                message=f"STARTUP: chmod 0600 on {path} failed (continuing): {exc}",
                component="FindCareBackend",
                exception=exc,
            ),
        )


def write_certs_to_runtime_dir(mapping: dict[str, str], runtime_dir: str) -> list[str]:
    """For each present env var, decode and write to runtime_dir. Returns the
    list of filenames written. Logs the final summary on success."""
    wrote = []
    for env_var, filename in mapping.items():
        b64 = os.environ.get(env_var)
        if not b64:
            continue
        pem_bytes = decode_cert_pem(env_var, b64)
        os.makedirs(runtime_dir, exist_ok=True)
        path = os.path.join(runtime_dir, filename)
        with open(path, "wb") as f:
            f.write(pem_bytes)
        try_chmod_0600(path)
        wrote.append(filename)
    if wrote:
        log.info("startup bootstrap: wrote %s to %s (CERTS_DIR=%s)",
                 ",".join(wrote), runtime_dir, runtime_dir)
    return wrote


def bootstrap_certs_from_env():
    """Write PKI material from env vars to a runtime dir and point CERTS_DIR at it.

    HF Spaces don't support bind-mounted cert directories. The deploy pipeline
    stores the signing key and public cert as HF Space secrets (base64-encoded
    PEM). On startup we decode them to /tmp/ch_certs and set CERTS_DIR.

    If none of the env vars are present (e.g. local dev, local Docker with a
    bind-mounted /certs), the function is a no-op — the caller's CERTS_DIR
    resolution remains in effect. Malformed PEM content raises, which the
    startup check turns into an exit-78 abend per EPIC-002-F-001-S-012.
    """
    runtime_dir = "/tmp/ch_certs"
    mapping = {
        "FINDCARE_SIGNING_KEY_PEM":  "findcare.key",
        "FINDCARE_CERT_PEM":         "findcare.crt",
        # SEC-HTTPS-001-REQ-021: FindCare verifies tokens minted by peers
        # (page-owning service mints; FindCare verifies for mutual auth).
        "SHARED_CERT_PEM":           "shared.crt",
        "EVALCARE_CERT_PEM":         "evalcare.crt",
        "CA_CERT_PEM":               "ca.crt",
    }
    wrote = write_certs_to_runtime_dir(mapping, runtime_dir)
    if wrote:
        os.environ["CERTS_DIR"] = runtime_dir

def startup_security_verification():
    """EPIC-002-F-001-S-012: exercise the security primitives this
    service uses. FindCare does NOT manufacture auth tokens — that's
    SharedServices's /auth/issue. FindCare's primitives are nonce restamp
    (signs with findcare.key) and verify (reads the page-owner's cert).
    Probe loads both to confirm CERTS_DIR is bootstrapped and the
    cryptography primitives can parse them."""
    bootstrap_certs_from_env()
    try:
        from chathealthy_lib.authentication.session_token import cert_basename
        from cryptography.hazmat.primitives import serialization
        from cryptography.x509 import load_pem_x509_certificate
    except ImportError as _imp:
        # Mode 2 (REQ-B-008): startup-time fatal — handled locally by
        # logging + sys.exit(78). The process abends cleanly with a named
        # exit code so the operator can diagnose; the user never sees the
        # service since it never bound a port. Not Mode 3 because the
        # exception IS caught and handled with explicit abend semantics.
        raise ChatHealthyException(
            mode="startup_abend_config",
            component="FindCareBackend",
            message=("STARTUP ABEND exit=78 primitive=crypto reason=import_failed: %s" % (_imp,)),
            exit_code=78,
            exception=_imp)
    certs_dir = os.environ.get("CERTS_DIR", "/certs")
    key_path = os.path.join(certs_dir, f"{cert_basename('FindCare')}.key")
    cert_path = os.path.join(certs_dir, f"{cert_basename('FindCare')}.crt")
    try:
        with open(key_path, "rb") as _f:
            serialization.load_pem_private_key(_f.read(), password=None)
        with open(cert_path, "rb") as _f:
            load_pem_x509_certificate(_f.read())
    except FileNotFoundError as _fnf:
        # Mode 2 (REQ-B-008): startup-time fatal — handled locally with
        # named exit code 78 (EX_CONFIG, missing cert/key file).
        raise ChatHealthyException(
            mode="startup_abend_config",
            component="FindCareBackend",
            message=("STARTUP ABEND exit=78 primitive=session_token reason=missing_cert_or_key: %s" % (_fnf,)),
            exit_code=78,
            exception=_fnf)
    except PermissionError as _perm:
        # Mode 2 (REQ-B-008): startup-time fatal — handled locally with
        # named exit code 77 (EX_NOPERM, permission denied on cert/key).
        raise ChatHealthyException(
            mode="startup_abend_permission",
            component="FindCareBackend",
            message=("STARTUP ABEND exit=77 primitive=session_token reason=permission: %s" % (_perm,)),
            exit_code=77,
            exception=_perm)
    except Exception as _exc:
        # Mode 2 (REQ-B-008): startup-time fatal — handled locally with
        # named exit code 70 (EX_SOFTWARE, key/cert unreadable for other
        # reasons). Process abends cleanly; user never sees the service.
        raise ChatHealthyException(
            mode="startup_abend_software",
            component="FindCareBackend",
            message=("STARTUP ABEND exit=70 primitive=session_token reason=key_or_cert_unreadable: %s" % (_exc,)),
            exit_code=70,
            exception=_exc)
    log.info("startup security check PASSED — findcare.key + findcare.crt OK at %s", certs_dir)

startup_security_verification()

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time_mod.time()
    response = await call_next(request)
    elapsed = round((time_mod.time() - start) * 1000)
    log.info("REQUEST %s %s → %d (%dms) from %s",
              request.method, request.url.path, response.status_code, elapsed,
              request.headers.get("x-forwarded-for", request.client.host if request.client else "unknown"))
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://chathealthy.ai", "https://www.chathealthy.ai", "https://dev.chathealthy.ai"],
    allow_origin_regex=r"https://localhost(:\d+)?$|https://[a-zA-Z0-9-]+\.chathealthy\.ai$",
    allow_credentials=False, allow_methods=["*"], allow_headers=["*"],
)

class ChatRequest(BaseModel):
    message: str
    history: list[dict] = []

class PaginationMeta(BaseModel):
    has_more: bool = False
    first_npi: Optional[str] = None
    last_npi: Optional[str] = None
    count: int = 0
    total_count: int = 0
    page_start: int = 1
    page_end: int = 0
    search_params: Optional[dict] = None
    specialization_options: Optional[list[dict]] = None
    summary_message: Optional[str] = None

class TrialsMeta(BaseModel):
    trial_count: int = 0
    condition: str = ""
    location: str = ""
    summary_message: Optional[str] = None

class ChatResponse(BaseModel):
    response: Optional[str] = None
    emergency: bool = False
    error: Optional[str] = None
    error_type: Optional[str] = None
    tokens_in: Optional[int] = None
    tokens_out: Optional[int] = None
    pagination: Optional[PaginationMeta] = None
    trials: Optional[TrialsMeta] = None
    # EPIC-002-F-003-S-004: when the chat detects a register/sign-in
    # intent it sets this field instead of running the normal pipeline.
    # The chat iframe forwards it to the wrapper as
    # postMessage(type: "gui:initiate-oauth-google").
    oauth_init: Optional[str] = None

SHARED_SERVICES_ORIGIN = "SharedServices"


def require_gateway_signature(session_token: Optional[dict],
                              posted: Optional[dict] = None,
                              headers: Optional[dict] = None) -> None:
    """Refuse a request that did not arrive through the approved gateway,
    and state what it carries for the code that serves it.

    EPIC-006-F-001-S-003-REQ-B-004 has two halves. Arriving through the
    gateway is met by the client having no other address to call; refusing
    a request that arrived by another route can only be met here, inside
    FindCare, because the Space is a public HTTPS host and cannot require
    a client certificate.

    The mechanism is the one the application already carries: SharedServices
    signs the session token with its own key and FindCare receives
    SharedServices' certificate as SHARED_CERT_PEM at deploy time. FindCare
    verifies that signature and does not re-validate the session -- /gate
    has already done that, and a second validation with a different answer
    would be worse than none.
    """
    if not session_token:
        raise ChatHealthyException(
            mode="http_error",
            component="FindCareBackend",
            message="request carries no session token; every FindCare route "
                    "requires one bearing a SharedServices signature",
            status_code=401,
        )
    token = SessionToken.model_validate(session_token)
    if not token.verify(expected_origin=SHARED_SERVICES_ORIGIN):
        raise ChatHealthyException(
            mode="http_error",
            component="FindCareBackend",
            message="session token does not bear a valid SharedServices "
                    "signature",
            status_code=401,
        )
    # This is the door, and every route passes through it, so this is where
    # the request's facts are stated. Code below reads them from
    # http_request_facts instead of taking them as arguments.
    request_facts.state_the_facts(
        token=token, posted=posted or {}, headers=headers or {})


class SearchRequest(BaseModel):
    """Direct provider search. Used for pagination.

    entity_type carries no default: it is a property of the page the request
    was dispatched to, so a request that does not name it is refused rather
    than falling through to one page's answer.
    """
    entity_type: str
    specialty_query: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    zip: Optional[str] = None
    npi: Optional[str] = None
    # A set of records addressed by identity. The general shape; a single
    # npi is a case of it. A caller holding several identities asks once
    # rather than opening the collection itself (C-26).
    npis: Optional[list[str]] = None
    nucc_codes: Optional[list[str]] = None
    # The keyset position and which way the page is taken from it.
    cursor: Optional[str] = None
    direction: str = "forward"
    limit: int = 25
    # The gateway's signature, verified before anything else happens.
    session_token: Optional[dict] = None
    # How a facility is named: outright, or by the person who administers
    # it. Undeclared here they were dropped at this boundary, so a search
    # that named an administrator returned every organization instead.
    facility_name: Optional[str] = None
    administrator_last_name: Optional[str] = None
    administrator_first_name: Optional[str] = None
    administrator_middle_name: Optional[str] = None
    # Named outright rather than searched for by what they do.
    last_name: Optional[str] = None
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    # Preferences the person stated. Applied to an already-narrow result.
    provider_sex: Optional[str] = None
    sole_proprietor: Optional[bool] = None
    insurance: Optional[str] = None

@app.post("/search")
async def search(body: SearchRequest):
    """Direct provider search — for pagination. No LLM involved.

    Local catch with mode discrimination per EPIC-008-F-002-S-009-REQ-B-008.
    Catcher classifies caught ChatHealthyException by mode and acts per the
    three-mode taxonomy."""
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    params = body.model_dump(exclude_none=True)
    params.pop("session_token", None)
    try:
        return find_care.search_providers(**params)
    except ChatHealthyException as exc:
        if exc.mode == "mongo_query_timeout":
            # Mode 2 (REQ-B-008): resource temporarily unavailable (Mongo
            # aggregate exceeded its timeout budget). Surface a graceful
            # user-facing 200 response carrying an error string; NOT 503;
            # no fatal_error tag.
            log.error("search Mode 2: mongo_query_timeout on %s.%s",
                      exc.context.get("db"), exc.context.get("coll"),
                      exc=exc, if_not_debug_log=True)
            return {
                "providers": [],
                "total_count": 0,
                "error": "Provider search is taking longer than usual. "
                         "Please try the same search again in a moment.",
                "error_mode": exc.mode,
            }
        # Unknown ChatHealthyException mode at this site → re-raise so the
        # Mode 3 safety net handles it. Adding a known mode here with its
        # Mode 1 / Mode 2 / Mode 3 classification is the way to bring it
        # under local control.
        raise


class FacilityFindRequest(BaseModel):
    """The facility page's own request: an utterance and the talk before it.

    The page mines what it needs from these two things; nothing upstream
    assembles its parameters for it.
    """
    session_token: dict
    utterance: str
    history: list = []


FACILITY_PAGE = "facility"
SESSION_DB = "Users"
SESSION_COLLECTION = "sessions"


def _facility_kinds(facility_type: str) -> list[dict]:
    """Extracted so the resolution raises without also logging (Rule-005
    statement 3). find_specialties is its own single catch point and
    answers with an error string rather than an exception; an unresolved
    kind of place is not a search across every organization.

    The rows leave in the shape the panel holds them in, the same one
    /nucc/classify hands back, so a kind of place and a specialty are one shape
    wherever they are read."""
    resolved = specialty_service.find_specialties(
        facility_type, None, SECTION_ORGANIZATION)
    if "error" in resolved:
        raise ChatHealthyException(
            mode="facility_type_unresolved",
            component="FindCareBackend",
            message=f"facility type {facility_type!r} did not resolve: "
                    f"{resolved['error']}",
        )
    return [{"code": row["Code"], "name": row["Display Name"],
             "can_prescribe": row.get("can_prescribe", False),
             "homeopathic": row.get("homeopathic", False),
             "rank": row.get("rank", 0)}
            for row in resolved.get("specialties", [])]


def _facility_page_entries(mined, offered: list[dict],
                           codes: list[str]) -> dict:
    """The mined values as parameter entries, keyed by attribute.

    The route is the tool because this tool mined them, and the
    determination is the model because a model inferred them.
    """
    from chathealthy_lib.authentication.user_parameters import ParameterEntry

    def entry(value):
        return ParameterEntry(value=value, route="tool",
                              determination="model").model_dump(
                                  exclude_none=True)

    entries: dict = {}
    # A place is four facts, not one. The state is the fact a search
    # needs -- healthcare is regulated per state -- and the rest narrow
    # it. Declared separately so the declaration can say which is
    # required, and so a page that cannot run can name the missing one.
    for part, value in mined.geography.model_dump().items():
        if part in ("state", "city", "zip", "county") and value:
            entries[part] = entry(value)
    if mined.facility_type:
        entries["facilityType"] = entry(mined.facility_type)
    if mined.facility_name:
        entries["facilityName"] = entry(mined.facility_name)
    if codes:
        entries["offeredFacilityTypes"] = entry(offered)
        entries["selectedTaxonomyCodes"] = entry(codes)
    return entries


def _write_facility_parameters(session_token: dict, entries: dict) -> None:
    """The page that mined a parameter writes it, and writes it on its own
    page: no other page of the session is addressed here, ever."""
    if not entries:
        return
    db = get_db()
    if db is None:
        raise ChatHealthyException(
            mode="mongo_network_failure",
            component="FindCareBackend",
            message="facility parameters mined but the session is "
                    "unreachable to write them to",
        )
    guid = SessionToken.model_validate(session_token).session_guid()
    result = db[SESSION_DB][SESSION_COLLECTION].update_one(
        {"_id": guid},
        {"$set": {f"userParameters.pages.{FACILITY_PAGE}.{name}": value
                  for name, value in entries.items()}},
    )
    if result.matched_count == 0:
        raise ChatHealthyException(
            mode="session_not_found",
            component="FindCareBackend",
            message=f"no session {guid!r} to write the mined facility "
                    f"parameters to",
        )


@app.post("/facility/find")
async def facility_find(body: FacilityFindRequest):
    """The facility page mines its own parameters and searches on them.

    Local catch with mode discrimination per EPIC-008-F-002-S-009-REQ-B-008,
    the same shape /search carries."""
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    try:
        # Both the mining and the resolution make blocking model calls;
        # this handler is async and already inside an event loop.
        mined = await asyncio.to_thread(
            mine_facility_parameters, body.utterance, body.history)
        offered: list[dict] = []
        if mined.facility_type:
            offered = await asyncio.to_thread(
                _facility_kinds, mined.facility_type)
        codes = [row["code"] for row in offered]
        # Written before the search, so what the answer was produced under
        # is on the session whatever the search then does.
        await asyncio.to_thread(
            _write_facility_parameters, body.session_token,
            _facility_page_entries(mined, offered, codes))
        # What the search runs on is what is in force, not what this turn
        # happened to say. The mined values were written above, so the
        # session is the merge of everything said so far -- reading it back
        # is what lets an answer add to a request instead of replacing it.
        in_force = await asyncio.to_thread(_parameters_in_force, FACILITY_PAGE)
        if not codes:
            codes = list(in_force.get("selectedTaxonomyCodes") or [])
        in_force["selectedTaxonomyCodes"] = codes
        result: dict = {"providers": [], "total_count": 0}
        known = await asyncio.to_thread(_geography_known, in_force)
        unmet = _unmet_requirements(FACILITY_SEARCH_TOOL, known)
        if not unmet:
            result = find_care.search_providers(
                entity_type="2",
                nucc_codes=codes,
                state=str(in_force.get("state") or ""),
                city=str(in_force.get("city") or ""),
                county=str(in_force.get("county") or ""),
                zip=str(in_force.get("zip") or ""),
                facility_name=str(in_force.get("facilityName") or ""),
            )
        # What the search ran on, for a caller that has to say on screen
        # what was searched for. Nothing downstream reads it to persist:
        # the parameters are already written above.
        result["mined"] = mined.model_dump()
        # What the page still needs before it can answer. The caller asks
        # the person about exactly these, by name, so a requirement added
        # to the declaration is asked for without new code.
        result["unmet_requirements"] = unmet
        if unmet:
            result["refinement_question"] = _question_for(
                FACILITY_SEARCH_TOOL, unmet, known,
                body.utterance, body.history)
        return result
    except ChatHealthyException as exc:
        if exc.mode == "mongo_query_timeout":
            # Mode 2 (REQ-B-008): resource temporarily unavailable (Mongo
            # aggregate exceeded its timeout budget). Graceful user-facing
            # 200 carrying an error string; NOT 503; no fatal_error tag.
            log.error("facility_find Mode 2: mongo_query_timeout on %s.%s",
                      exc.context.get("db"), exc.context.get("coll"),
                      exc=exc, if_not_debug_log=True)
            return {
                "providers": [],
                "total_count": 0,
                "error": "Facility search is taking longer than usual. "
                         "Please try the same search again in a moment.",
                "error_mode": exc.mode,
            }
        # Unknown ChatHealthyException mode at this site → re-raise so the
        # Mode 3 safety net handles it.
        raise


# Which tool each of this service's surfaces is, as the configuration
# names them. A page is a list of tools and holds their values; what a
# tool requires is the tool's, because a page carries several and a
# search runs before any record is opened.
PROVIDER_SEARCH_TOOL = "ProviderSearch"
FACILITY_SEARCH_TOOL = "FacilitySearch"
SPECIALTY_FILTER_TOOL = "SpecialtyFilter"
CLINICAL_TRIALS_TOOL = "ClinicalTrials"

INDIVIDUAL_PROVIDER_PAGE = "individualProvider"
NUCC_PAGE = "NUCC"
CLINICAL_TRIAL_PAGE = "clinicalTrial"

SEX_CODES = ("F", "M", "X", "U")


def _parameter_entry(value) -> dict:
    """A mined value as a parameter entry, ready to be stored.

    The route is the tool because this tool mined it, and the
    determination is the model because a model inferred it.
    """
    from chathealthy_lib.authentication.user_parameters import ParameterEntry

    return ParameterEntry(value=value, route="tool",
                          determination="model").model_dump(exclude_none=True)


def _unmet_requirements(tool: str, in_force: dict) -> list[str]:
    """Which of this page's declared requirements are not in force.

    Empty means the page can run. Otherwise these are the attributes to
    ask the person about, by name -- so a parameter made required in the
    declaration is asked for without a line of code being written for it.

    The declaration says which attributes a page cannot run without, and
    this asks it rather than deciding. Making a parameter required or
    optional is an edit to deployment_architecture.json and a deploy: no
    page has a rule of its own, and none can enforce a requirement the
    record does not state. A page that declares nothing required runs on
    whatever it has.

    A value that is present but empty is not in force -- an empty string
    and a missing string are the same absence to the person who did not
    say it.
    """
    missing = []
    for name in required_parameters(tool):
        value = in_force.get(name)
        if value is None or value == "" or value == [] or value == {}:
            missing.append(name)
    return missing


def _state_of_zip(zip_code: str) -> str:
    """The state a ZIP is in, read from the addresses we already hold.

    A ZIP names exactly one state, so a person who gave one has told us
    which board licenses the care giver -- which is the whole reason the
    state is required. Asking them to say it again is asking for a fact
    they already supplied.

    Derived, never queried on. The state is what makes the requirement
    met; the ZIP is what the person asked for, and it is narrower. Adding
    the state to the query would search a whole state on the strength of
    an answer about one ZIP.
    """
    wanted = (zip_code or "").strip()
    if not wanted:
        return ""
    db = get_db()
    if db is None:
        return ""
    row = db["PublicHealthData"]["Provider"].find_one(
        {"practice_addresses.zip": wanted},
        {"practice_addresses.zip": 1, "practice_addresses.state": 1})
    for address in ((row or {}).get("practice_addresses") or []):
        if str(address.get("zip") or "").strip() == wanted:
            return str(address.get("state") or "").strip().upper()
    return ""


def _geography_known(in_force: dict) -> dict:
    """What is known about the place, as against what was asked for.

    The requirement is that the state be KNOWN. A ZIP supplies it without
    the person repeating themselves, so the derived state is added here --
    and nowhere else, because the query is built from what was asked for.
    """
    known = dict(in_force)
    if not str(known.get("state") or "").strip():
        derived = _state_of_zip(str(known.get("zip") or ""))
        if derived:
            known["state"] = derived
    return known


def _parameters_in_force(page: str) -> dict:
    """Every parameter this page holds, as plain values.

    One read rather than one per attribute: what a search runs on is the
    whole of what the person has said across the turns, and fetching it
    field by field invites a caller to fetch only the fields it remembered.
    """
    db = get_db()
    if db is None:
        raise ChatHealthyException(
            mode="mongo_network_failure",
            component="FindCareBackend",
            message=f"the session is unreachable, so {page} cannot read what "
                    f"is in force")
    doc = db[SESSION_DB][SESSION_COLLECTION].find_one(
        {"_id": request_facts.facts().session_guid()},
        {f"userParameters.pages.{page}": 1})
    held = ((doc or {}).get("userParameters", {})
            .get("pages", {}).get(page, {})) or {}
    return {name: (entry.get("value") if isinstance(entry, dict) else entry)
            for name, entry in held.items()}


def _read_page_parameter(page: str, name: str):
    """One of this page's own parameters, off the session.

    The page reads what it owns rather than being handed it. A caller that
    passes a page's parameter in is a caller that had to know the parameter,
    and knowing it is what this page exists to do.
    """
    db = get_db()
    if db is None:
        raise ChatHealthyException(
            mode="mongo_network_failure",
            component="FindCareBackend",
            message=f"the session is unreachable, so {page} cannot read what "
                    f"it needs to answer")
    address = f"userParameters.pages.{page}.{name}"
    doc = db[SESSION_DB][SESSION_COLLECTION].find_one(
        {"_id": request_facts.facts().session_guid()}, {address: 1})
    held = ((doc or {}).get("userParameters", {})
            .get("pages", {}).get(page, {}).get(name))
    return (held or {}).get("value") if isinstance(held, dict) else held


def _gesture_entry(value) -> dict:
    """A value a gesture set, ready to be stored.

    The route is the tool because this service made the write, and the
    determination is a rule rather than a model: nobody inferred which
    record the person opened -- they opened it.
    """
    from chathealthy_lib.authentication.user_parameters import ParameterEntry

    return ParameterEntry(value=value, route="tool",
                          determination="rule").model_dump(exclude_none=True)


# Which attribute names the record a page is currently showing. Every page
# that can open a detail has one, and it is not the same attribute on each:
# a care giver and an organization are both identified by an NPI, a trial by
# its registry id. Nothing outside this service needs to know that -- a
# caller says which page and which record, and this decides where it lands.
OPEN_RECORD_ATTRIBUTE = {
    INDIVIDUAL_PROVIDER_PAGE: "openNpi",
    FACILITY_PAGE: "openNpi",
    CLINICAL_TRIAL_PAGE: "openNctId",
}


def _open_record_attribute(page: str) -> str:
    name = OPEN_RECORD_ATTRIBUTE.get(page)
    if not name:
        raise ChatHealthyException(
            mode="value_error",
            component="FindCareBackend",
            message=f"{page!r} shows no record, so nothing can be opened or "
                    f"closed on it")
    return name


def _clear_page_parameter(page: str, name: str) -> None:
    """Take one of this page's own parameters off the session."""
    db = get_db()
    if db is None:
        raise ChatHealthyException(
            mode="mongo_network_failure",
            component="FindCareBackend",
            message=f"the session is unreachable, so {page} cannot record "
                    f"what it stopped showing")
    db[SESSION_DB][SESSION_COLLECTION].update_one(
        {"_id": request_facts.facts().session_guid()},
        {"$unset": {f"userParameters.pages.{page}.{name}": ""}})


class FacilityPageRequest(BaseModel):
    """Another page of the facility list, forward or back.

    The caller sends where in the list to continue from and nothing else.
    What the list is a list OF -- the place, the kind, the name, the
    administrator -- is this page's, already in force from the search that
    produced the list being paged.
    """
    session_token: dict
    cursor: str
    direction: str = "forward"
    limit: int = 25


@app.post("/facility/page")
async def facility_page(body: FacilityPageRequest):
    """Page the facility list on the parameters already in force.

    Keyset paging, on this page's own recorded position: back takes the
    rows before the first key on screen, forward those after the last. The
    position is written here because it is this page's parameter and this
    is the page that moved.
    """
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    if body.direction not in ("forward", "back"):
        raise ChatHealthyException(
            mode="value_error",
            component="FindCareBackend",
            message=f"unknown direction {body.direction!r}")
    if not (body.cursor or "").strip():
        raise ChatHealthyException(
            mode="value_error",
            component="FindCareBackend",
            message="a page of a list continues from somewhere, and no "
                    "cursor was given")

    def _in_force() -> dict:
        geo = {part: _read_page_parameter(FACILITY_PAGE, part) or ""
               for part in ("state", "city", "zip", "county")}
        administrator = _read_page_parameter(
            FACILITY_PAGE, "administratorName") or {}
        return {
            "state": geo.get("state") or "",
            "city": geo.get("city") or "",
            "county": geo.get("county") or "",
            "zip": geo.get("zip") or "",
            "facility_name": _read_page_parameter(
                FACILITY_PAGE, "facilityName") or "",
            "administrator_last_name": administrator.get("last") or "",
            "administrator_first_name": administrator.get("first") or "",
            "administrator_middle_name": administrator.get("middle") or "",
            "nucc_codes": list(_read_page_parameter(
                FACILITY_PAGE, "selectedTaxonomyCodes") or []),
        }

    in_force = await asyncio.to_thread(_in_force)
    result = await asyncio.to_thread(
        lambda: find_care.search_providers(
            entity_type="2", limit=body.limit,
            cursor=body.cursor, direction=body.direction, **in_force))
    await asyncio.to_thread(
        _write_page_parameters, FACILITY_PAGE,
        {"position": _gesture_entry(
            {"first": str(result.get("first_npi") or ""),
             "last": str(result.get("last_npi") or "")})})
    return result


class WhatIsNeededRequest(BaseModel):
    """A turn that produced nothing, asking what was lacking.

    Both are named because they answer different halves. The page is the
    scope the values live in -- a geography on the care-giver page is not
    the geography on the facility page. The tool is what has requirements:
    a page carries several, and a search runs before any record is opened.
    """
    session_token: dict
    page: str
    tool: str
    utterance: str = ""
    history: list = []


@app.post("/page/what-is-needed")
async def page_what_is_needed(body: WhatIsNeededRequest):
    """What this page still needs, and the question that asks for it.

    A turn that showed nothing and said nothing leaves the person with a
    dead screen. The page is the only thing that knows why: it holds the
    declaration saying which of its attributes it cannot run without, and
    the session saying which are in force.

    Read from the session rather than from the turn, because the turn may
    have written nothing -- what matters is what the page has, however it
    came to have it.
    """
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))

    def _in_force() -> dict:
        return {name: _read_page_parameter(body.page, name)
                for name in declared_attributes(body.page)}

    in_force = await asyncio.to_thread(_in_force)
    unmet = _unmet_requirements(body.tool, in_force)
    if not unmet:
        return {"unmet_requirements": [], "refinement_question": ""}
    return {
        "unmet_requirements": unmet,
        "refinement_question": _question_for(
            body.tool, unmet, in_force, body.utterance, body.history),
    }


class DetailOpenRequest(BaseModel):
    """A record the person navigated to, on a named page."""
    session_token: dict
    page: str
    record_id: str


class DetailCloseRequest(BaseModel):
    """A page that has stopped showing a record."""
    session_token: dict
    page: str


@app.post("/page/detail-open")
async def page_detail_open(body: DetailOpenRequest):
    """Record that this page is showing this record.

    An open detail is a place the person navigated to, and recording it is
    what lets a return put them back on it rather than at the top of the
    list. The caller names the page and the record; which attribute holds
    it is this service's to know.
    """
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    record = (body.record_id or "").strip()
    if not record:
        return {"opened": False}
    await asyncio.to_thread(
        _write_page_parameters, body.page,
        {_open_record_attribute(body.page): _gesture_entry(record)})
    return {"opened": True}


@app.post("/page/detail-close")
async def page_detail_close(body: DetailCloseRequest):
    """Record that this page has stopped showing a record.

    Not clearing it is what resurrects a panel on the next return, so the
    close is a write and not merely the absence of one.
    """
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    await asyncio.to_thread(_clear_page_parameter, body.page,
                            _open_record_attribute(body.page))
    return {"closed": True}


class ProviderExclusionsRequest(BaseModel):
    """Which of these care givers the filter in force sets aside.

    The caller sends identities and nothing else. Which specialties are in
    force, and what it means for one to admit a care giver, are this page's
    to know -- a caller that sent the codes would be a caller that had to
    hold them.
    """
    session_token: dict
    npis: list[str] = []


@app.post("/provider/exclusions")
async def provider_exclusions(body: ProviderExclusionsRequest):
    """Mark, per identity, whether the specialties in force admit them.

    The row is marked and kept, never dropped: a filter that silently
    discards a person's own choice is the failure this prevents
    (EPIC-006-F-001-S-002-REQ-B-019). Only this page holds a care giver's
    full taxonomy list, which is why the comparison is made here.
    """
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    wanted = [npi for npi in (body.npis or []) if npi]
    if not wanted:
        return {"excluded": {}}
    chosen = set(await asyncio.to_thread(
        _read_page_parameter, INDIVIDUAL_PROVIDER_PAGE,
        "selectedSpecialtyCodes") or [])
    if not chosen:
        return {"excluded": {npi: False for npi in wanted}}
    found = await asyncio.to_thread(
        lambda: find_care.search_providers(
            entity_type="1", npis=wanted, limit=len(wanted)))
    held = {npi: set() for npi in wanted}
    for row in (found or {}).get("providers") or []:
        npi = row.get("npi")
        if npi in held:
            held[npi] = {code for code in (row.get("taxonomy_codes") or [])
                         if code}
    return {"excluded": {npi: not (held[npi] & chosen) for npi in wanted}}


def _question_for(tool: str, missing: list[str], in_force: dict,
                  utterance: str, history) -> str:
    """What to ask the person when this tool cannot run yet.

    The tool authors it rather than the gateway, because the tool is what
    holds the requirement. What it requires, what it merely accepts, and
    what the person has already said all reach the model as facts from the
    configuration -- so making a parameter Required is enough to have it
    asked for, with nothing written for it.
    """
    return ask_for_missing(
        tool, missing, optional_parameters(tool), in_force,
        utterance, history,
        component="FindCareApp", call_site=f"{tool}_refinement_request")


def _write_page_parameters(page: str, entries: dict) -> None:
    """The page that mined a parameter writes it, and writes it on its own
    page: no other page of the session is addressed here, ever.

    Which session is read from the facts this request arrived with, so no
    caller between the route and this line has to carry a token to reach
    it.
    """
    if not entries:
        return
    db = get_db()
    if db is None:
        raise ChatHealthyException(
            mode="mongo_network_failure",
            component="FindCareBackend",
            message=f"{page} parameters mined but the session is unreachable "
                    f"to write them to",
        )
    guid = request_facts.facts().session_guid()
    result = db[SESSION_DB][SESSION_COLLECTION].update_one(
        {"_id": guid},
        {"$set": {f"userParameters.pages.{page}.{name}": value
                  for name, value in entries.items()}},
    )
    if result.matched_count == 0:
        raise ChatHealthyException(
            mode="session_not_found",
            component="FindCareBackend",
            message=f"no session {guid!r} to write the mined {page} "
                    f"parameters to",
        )


def _resolve_specialties(complaint: str) -> dict:
    """The kinds of care giver that treat a complaint.

    Extracted so the resolution raises without also logging (Rule-005
    statement 3). find_specialties is its own single catch point and
    answers with an error string rather than an exception; an unresolved
    complaint is not a search across every specialty.

    The rows leave in the shape the panel holds them in, the same one
    /nucc/classify hands back. The complaint comes back too, because the
    pipeline reads the words clinically -- 'shrink' returns as
    'psychological problem' -- and that reading is what the page records.
    """
    resolved = specialty_service.find_specialties(
        complaint, None, SECTION_INDIVIDUAL)
    if "error" in resolved:
        raise ChatHealthyException(
            mode="complaint_unresolved",
            component="FindCareBackend",
            message=f"complaint {complaint!r} did not resolve: "
                    f"{resolved['error']}",
        )
    return {
        "specialties": [{"code": row["Code"], "name": row["Display Name"],
                         "can_prescribe": row.get("can_prescribe", False),
                         "homeopathic": row.get("homeopathic", False),
                         "rank": row.get("rank", 0)}
                        for row in resolved.get("specialties", [])],
        "complaint": str(resolved.get("complaint") or "").strip() or complaint,
    }


def _ticked(offered: list[dict]) -> list[str]:
    """Which offered rows the panel paints ticked, and therefore which the
    search must run under: the prescribers. The panel paints prescribers
    checked and everything else clear, so a search over the whole offered
    set would show one thing and do another."""
    return [row["code"] for row in offered if row.get("can_prescribe")]


def _specialty_groups(offered: list[dict]) -> dict:
    """The sets the panel offers as one gesture, named by this service.

    Which kinds of care giver may prescribe, and which are homeopathic, are
    clinical facts about the records this service holds. The panel offers a
    control that ticks each set at once, and it can do that knowing only
    which codes are in the set -- so it is given the sets rather than the
    classification, and never has to read a clinical field to group by it.

    The default is here for the same reason: what a fresh panel arrives
    ticked with is a rule about the search that follows it, and it is
    already stated once by _ticked.
    """
    return {
        "all_codes": [row["code"] for row in offered if row.get("code")],
        "prescriber_codes": [row["code"] for row in offered
                             if row.get("can_prescribe")],
        "homeopathic_codes": [row["code"] for row in offered
                              if row.get("homeopathic")],
        "default_selected_codes": _ticked(offered),
    }


def _searched_codes(offered: list[dict], ticked: list[str]) -> list[str]:
    """Nothing ticked means nothing was narrowed, so the whole offered set
    applies."""
    return ticked or [row["code"] for row in offered]


def _sex_code(mined_sex: str) -> str:
    """NPPES records sex as F, M, X (neither male nor female) or U
    (undisclosed). A model answering outside that set has not mined a sex,
    and storing the answer would put a value on the session that no reader
    of it can act on."""
    code = str(mined_sex or "").strip().upper()
    if not code:
        return ""
    if code not in SEX_CODES:
        raise ChatHealthyException(
            mode="value_error",
            component="FindCareBackend",
            message=f"{code!r} is not a sex code; NPPES uses "
                    f"{', '.join(SEX_CODES)}",
        )
    return code


class ProviderFindRequest(BaseModel):
    """The individual-provider page's own request: an utterance and the
    talk before it.

    The page mines what it needs from these two things; nothing upstream
    assembles its parameters for it.
    """
    session_token: dict
    utterance: str
    history: list = []


def _provider_page_entries(mined, complaint: str,
                           ticked: list[str]) -> dict:
    """The mined values as parameter entries, keyed by attribute."""
    entries: dict = {}
    for part, value in mined.geography.model_dump().items():
        if part in ("state", "city", "zip", "county") and value:
            entries[part] = _parameter_entry(value)
    if complaint:
        entries["complaint"] = _parameter_entry(complaint)
    name = mined.provider_name
    # Uppercased on the way in, because the records are uppercase and a
    # case-insensitive match cannot use the name index.
    parts = {"last": name.last.strip().upper(),
             "first": name.first.strip().upper(),
             "middle": name.middle.strip().upper()}
    if any(parts.values()):
        entries["providerName"] = _parameter_entry(parts)
    sex = _sex_code(mined.provider_sex)
    if sex:
        entries["providerSex"] = _parameter_entry(sex)
    if mined.sole_proprietor is not None:
        entries["soleProprietor"] = _parameter_entry(bool(mined.sole_proprietor))
    if mined.insurance:
        entries["insurance"] = _parameter_entry(mined.insurance)
    if ticked:
        entries["selectedSpecialtyCodes"] = _parameter_entry(ticked)
    return entries


@app.post("/provider/find")
async def provider_find(body: ProviderFindRequest):
    """The individual-provider page mines its own parameters and searches
    on them.

    Local catch with mode discrimination per EPIC-008-F-002-S-009-REQ-B-008,
    the same shape /search carries."""
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    try:
        # Both the mining and the resolution make blocking model calls;
        # this handler is async and already inside an event loop.
        mined = await asyncio.to_thread(
            mine_individual_provider_parameters, body.utterance, body.history)
        offered: list[dict] = []
        complaint = mined.complaint
        if mined.complaint:
            resolved = await asyncio.to_thread(
                _resolve_specialties, mined.complaint)
            offered = resolved["specialties"]
            complaint = resolved["complaint"]
        ticked = _ticked(offered)
        codes = _searched_codes(offered, ticked)
        # Written before the search, so what the answer was produced under
        # is on the session whatever the search then does.
        await asyncio.to_thread(
            _write_page_parameters, INDIVIDUAL_PROVIDER_PAGE,
            _provider_page_entries(mined, complaint, ticked))
        # What the search runs on is what is IN FORCE, not what this turn
        # happened to say. A person answering "no, NY" has supplied one
        # part of a place and left the rest standing; searching on the
        # mined values alone threw away the city they gave a turn earlier
        # and returned the whole of New York.
        #
        # The mined values were written above, so the session is already
        # the merge of what was said before and what was said now. Reading
        # it back is what makes an answer add to a request rather than
        # replace it.
        in_force = await asyncio.to_thread(_parameters_in_force,
                                           INDIVIDUAL_PROVIDER_PAGE)
        # The codes were resolved from the complaint this turn when there
        # was one; otherwise the ones already in force still apply.
        if not codes:
            codes = list(in_force.get("selectedSpecialtyCodes") or [])
        in_force["selectedSpecialtyCodes"] = codes
        name = in_force.get("providerName") or {}
        result: dict = {"providers": [], "total_count": 0}
        # Judged on what is known -- a ZIP tells us the state. Searched on
        # what was asked for, which is the ZIP: it is narrower, and the
        # person did not ask for the state.
        known = await asyncio.to_thread(_geography_known, in_force)
        unmet = _unmet_requirements(PROVIDER_SEARCH_TOOL, known)
        if not unmet:
            result = find_care.search_providers(
                entity_type="1",
                nucc_codes=codes,
                state=str(in_force.get("state") or ""),
                city=str(in_force.get("city") or ""),
                county=str(in_force.get("county") or ""),
                zip=str(in_force.get("zip") or ""),
                last_name=str(name.get("last") or "").strip().upper(),
                first_name=str(name.get("first") or "").strip().upper(),
                middle_name=str(name.get("middle") or "").strip().upper(),
                provider_sex=_sex_code(str(in_force.get("providerSex") or "")),
                sole_proprietor=in_force.get("soleProprietor"),
                insurance=str(in_force.get("insurance") or ""),
            )
        # What the search ran on, for a caller that has to say on screen
        # what was searched for and paint the panel it was narrowed by.
        # Nothing downstream reads it to persist: the parameters are
        # already written above.
        result["mined"] = mined.model_dump()
        result["unmet_requirements"] = unmet
        result["complaint"] = complaint
        result["offered_specialties"] = offered
        result["selected_specialty_codes"] = ticked
        result.update(_specialty_groups(offered))
        if unmet:
            result["refinement_question"] = _question_for(
                PROVIDER_SEARCH_TOOL, unmet, known,
                body.utterance, body.history)
        return result
    except ChatHealthyException as exc:
        if exc.mode == "mongo_query_timeout":
            # Mode 2 (REQ-B-008): resource temporarily unavailable (Mongo
            # aggregate exceeded its timeout budget). Graceful user-facing
            # 200 carrying an error string; NOT 503; no fatal_error tag.
            log.error("provider_find Mode 2: mongo_query_timeout on %s.%s",
                      exc.context.get("db"), exc.context.get("coll"),
                      exc=exc, if_not_debug_log=True)
            return {
                "providers": [],
                "total_count": 0,
                "error": "Provider search is taking longer than usual. "
                         "Please try the same search again in a moment.",
                "error_mode": exc.mode,
            }
        # Unknown ChatHealthyException mode at this site → re-raise so the
        # Mode 3 safety net handles it.
        raise


class SpecialtyFindRequest(BaseModel):
    """The NUCC page's own request: an utterance and the talk before it."""
    session_token: dict
    utterance: str
    history: list = []


@app.post("/specialty/find")
async def specialty_find(body: SpecialtyFindRequest):
    """The NUCC page mines its own complaint and offers the kinds of care
    giver that treat it.

    It finds nobody. This is the page the person is on while their
    geography is not yet usable, so what it produces is the panel and the
    codes that panel is ticked with, and nothing else.
    """
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    # Both the mining and the resolution make blocking model calls; this
    # handler is async and already inside an event loop.
    mined = await asyncio.to_thread(
        mine_nucc_parameters, body.utterance, body.history)
    offered: list[dict] = []
    complaint = mined.complaint
    if mined.complaint:
        resolved = await asyncio.to_thread(_resolve_specialties, mined.complaint)
        offered = resolved["specialties"]
        complaint = resolved["complaint"]
    ticked = _ticked(offered)
    entries: dict = {}
    if complaint:
        entries["complaint"] = _parameter_entry(complaint)
    if offered:
        entries["offeredSpecialties"] = _parameter_entry(offered)
    if ticked:
        entries["selectedSpecialtyCodes"] = _parameter_entry(ticked)
    await asyncio.to_thread(_write_page_parameters, NUCC_PAGE, entries)
    # A turn that named no complaint has not withdrawn the one already in
    # force: the panel it painted is still what the person is choosing
    # from, and repainting it empty would take their choices away.
    in_force = await asyncio.to_thread(_parameters_in_force, NUCC_PAGE)
    if not offered:
        offered = list(in_force.get("offeredSpecialties") or [])
        ticked = list(in_force.get("selectedSpecialtyCodes") or [])
        complaint = complaint or str(in_force.get("complaint") or "")
    return {
        "specialties": offered,
        "selected_codes": ticked,
        "complaint": complaint,
        "mined": mined.model_dump(),
        **_specialty_groups(offered),
    }


class ClassifyRequest(BaseModel):
    """GOV-011: AI translates the user's question into structured search parameters.
    One AI call. System answers with DB query after."""
    message: str
    # Which partition of the catalogue this resolution reads: the
    # individual section for a care giver, the organization section for a
    # facility. The same funnel runs either way; this is the one thing its
    # queries differ by.
    section: str = "Individual"
    # The gateway's signature, verified before anything else happens.
    session_token: Optional[dict] = None

def _require_db_for_classify():
    """Guard extracted so /classify does not both raise and log in one body.

    Rule-005 statement 3: the thrower does not log, the catcher does. The
    exception still propagates into classify's existing except block, so
    behaviour is unchanged.
    """
    db = get_db()
    if db is None:
        raise ChatHealthyException(
            mode="mongo_network_failure",
            component="FindCareBackend",
            message="Mongo unavailable",
        )
    return db


@app.post("/nucc/classify")
async def nucc_classify(body: ClassifyRequest, request: Request):
    """EPIC-006-F-003-S-001: specialty matching.

    normalize -> embed -> $vectorSearch -> LLM filter. Semantic search
    carries recall; the LLM call carries precision. CAND_FLOOR is the
    handoff between them and was tuned by the operator over a week.

    This pipeline was replaced on 2026-05-10 (bc102984) by a single call
    that walked the whole NUCC corpus. Nothing asked for that, the commit
    that did it describes a sub-iframe and a label flip, and the tuning
    went with it. SpecialtyFilter was never removed -- it stayed
    instantiated and unreachable -- so this is a restoration, not a
    rewrite, and the stages below are untouched.
    """
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or (
        request.client.host if request.client else "unknown")

    import uuid as _uuid
    from datetime import datetime as dt, timezone as _tz

    # find_specialties is synchronous and makes blocking model calls;
    # /classify is async and already inside an event loop.
    result = await asyncio.to_thread(
        specialty_service.find_specialties, body.message, None, body.section)

    if "error" in result:
        # REQ-B-002/B-003: sanitized outward, full detail kept server-side
        # under a request id. filter.py reports "<stage>: <type>: <detail>",
        # so the stage survives and the leak does not.
        req_id = _uuid.uuid4().hex[:8]
        ts = dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        raw = result["error"]
        stage = (raw.split(":", 1)[0].strip() if ":" in raw else "unknown")
        log.error("classify req_id=%s ip=%s stage=%s detail=%r message=%r",
                  req_id, ip, stage, raw, body.message,
                  exc=ChatHealthyException(
                      mode="classify_failed",
                      message=f"classify req_id={req_id} ip={ip} stage={stage} detail={raw}",
                      component="FindCareBackend",
                  ), if_not_debug_log=True)
        return {"specialties": [], "error": sanitized_classify_error(stage, ts, req_id)}

    specialties = [
        {"code": s["Code"], "name": s["Display Name"],
         "can_prescribe": s.get("can_prescribe", False),
         "homeopathic": s.get("homeopathic", False),
         "rank": s.get("rank", 0)}
        for s in result.get("specialties", [])
    ]
    return {
        "specialties": specialties,
        "homeopathic_generalists": [],
        "complaint": result.get("complaint", ""),
        "model": "normalize + embed + vectorSearch + LLM filter",
    }


def sanitized_classify_error(stage: str, ts: str, req_id: str) -> str:
    return (f"FindCare /nucc/classify temporarily unavailable "
            f"(stage: {stage}) at {ts}. Ref: {req_id}")


@app.post("/welcome")
def welcome():
    return {"message": WELCOME_MESSAGE}


# Clinical trials cross-service entry point. SharedServices posts the
# utterance and the talk before it here; this page reads them, so both
# what a trial search is made of and the searching itself stay inside
# FindCare. Streams the criteria and then the tool's chunk events as
# NDJSON; SS forwards each line into the user's /gate stream.
class TrialFindRequest(BaseModel):
    """The clinical-trial page's own request: an utterance and the talk
    before it.

    The page mines what it needs from these two things; nothing upstream
    assembles its parameters for it.
    """
    session_token: dict
    utterance: str
    history: list = []


def _trial_page_entries(mined) -> dict:
    """The mined values as parameter entries, keyed by attribute."""
    entries: dict = {}
    if mined.condition:
        entries["condition"] = _parameter_entry(mined.condition)
    if mined.age_years is not None:
        entries["ageYears"] = _parameter_entry(int(mined.age_years))
    if mined.sex:
        entries["sex"] = _parameter_entry(mined.sex)
    if mined.united_states_only is not None:
        entries["unitedStatesOnly"] = _parameter_entry(
            bool(mined.united_states_only))
    return entries


@app.post("/trial/find")
async def trial_find(body: TrialFindRequest):
    """The clinical-trial page mines its own parameters and searches on
    them, streaming the trials as they arrive.

    The criteria are announced on the same stream rather than by the
    caller: the caller posted an utterance and has not read it, so it has
    nothing to announce until this page says what the utterance meant.
    """
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    import json as _json
    from fastapi.responses import StreamingResponse
    try:
        from ClinicalTrials import clinical_trials_tool
    except ImportError:
        from FindCare.ClinicalTrials import clinical_trials_tool

    # The mining makes a blocking model call; this handler is async and
    # already inside an event loop.
    mined = await asyncio.to_thread(
        mine_clinical_trial_parameters, body.utterance, body.history)
    await asyncio.to_thread(
        _write_page_parameters, CLINICAL_TRIAL_PAGE, _trial_page_entries(mined))

    queue: asyncio.Queue = asyncio.Queue()
    sentinel = object()

    class _StreamCollector:
        def stream(self, event):
            queue.put_nowait(event)

    deps = _StreamCollector()
    # What the search runs on is what is in force. The mined values were
    # written above, so the session is the merge of everything said so
    # far -- a person who names a condition on one turn and their age on
    # the next has given both, and searching on the latest turn alone
    # would throw the condition away.
    in_force = await asyncio.to_thread(_parameters_in_force,
                                       CLINICAL_TRIAL_PAGE)
    age = in_force.get("ageYears")
    scope = "us" if in_force.get("unitedStatesOnly") else "international"
    req = clinical_trials_tool.Request(
        condition=str(in_force.get("condition") or ""),
        age_years=int(age) if age is not None else None,
        sex=str(in_force.get("sex") or "") or None,
        geographic_scope=scope,
    )
    # What the person asked for, said back to them in words. Composed here
    # because it is a sentence about this page's criteria: what "us" means
    # to a reader, and which criteria are worth repeating, are decisions
    # about the search rather than about the panel that shows it.
    said_back = []
    if in_force.get("condition"):
        said_back.append(f"condition: {in_force['condition']}")
    if age is not None:
        said_back.append(f"subject age: {age}")
    if in_force.get("sex"):
        said_back.append(f"subject sex: {in_force['sex']}")
    said_back.append("scope: US" if in_force.get("unitedStatesOnly")
                     else "scope: international")
    announced = {
        "kind": "intent_classified",
        "data": {
            "action": "findClinicalTrials",
            "condition": in_force.get("condition"),
            "age_years": age,
            "sex": str(in_force.get("sex") or "") or None,
            "geographic_scope": scope,
            "criteria_summary": ", ".join(said_back),
        },
    }

    async def runner():
        try:
            await clinical_trials_tool.TOOL.run(deps, req)
        finally:
            queue.put_nowait(sentinel)

    asyncio.create_task(runner())

    async def gen():
        yield _json.dumps(announced).encode() + b"\n"
        while True:
            item = await queue.get()
            if item is sentinel:
                break
            yield _json.dumps(item).encode() + b"\n"

    return StreamingResponse(gen(), media_type="application/x-ndjson")


# Provider Detail click-path endpoint (EPIC-006-F-002). Pure deterministic
# tool; no LLM. Input fields mirror the on-screen provider card.
from ProviderDetail.provider_detail_models import (
    ProviderDetailInput, ProviderDetailOutput,
)

@app.post("/provider-detail")
def provider_detail(
    body: ProviderDetailInput,
    background_tasks: BackgroundTasks,
) -> ProviderDetailOutput:
    require_gateway_signature(body.session_token,
                              posted=body.model_dump(exclude_none=True))
    return provider_detail_service.lookup(
        entity_type=body.entity_type,
        provider_name=body.name or "",
        npi=body.npi,
        state=body.state or "",
        provider_coll=providers_coll(),
        specialty_meta_coll=specialty_meta_coll,
        schedule_background_task=background_tasks.add_task,
    )



REQUIRED_INDEXES = [
    ("SpecialtyMetaData", specialty_meta_coll, ["specialty_vector_index"]),
]

def check_indexes() -> dict:
    """DR-016/DR-018: verify all required vector search indexes exist.

    Failure semantics (NO silent fallbacks):
      - DB unreachable           → status: "db_unavailable" (caller must
                                   degrade /health, not call this "ok")
      - Index list call raises   → status: "fail" with errors[] explaining
                                   which collections couldn't be checked.
                                   NOT silently appending "/ERROR" to
                                   missing[] (which conflated unreadable
                                   with absent).
      - Indexes legitimately
        missing                  → status: "fail" with missing[] populated.
    """
    missing = []
    errors = []
    for coll_label, coll_fn, index_names in REQUIRED_INDEXES:
        try:
            existing = [idx.get("name") for idx in coll_fn().list_search_indexes()]
        except Exception as exc:
            # Mode 2 (REQ-B-008): the index check failed for this
            # collection; the error is surfaced into the errors[] list
            # and /health reports status="fail". Operator MUST know —
            # missing vector indexes mean search is broken.
            log.error("index check on %s failed: %s", coll_label, exc, exc=ChatHealthyException(
                                                                        mode="index_check_failed",
                                                                        message=f"index check on {coll_label} failed: {exc}",
                                                                        component="FindCareBackend",
                                                                        exception=exc,
                                                                    ), if_not_debug_log=True)
            errors.append({"collection": coll_label, "error": f"{type(exc).__name__}: {exc}"})
            continue
        for idx in index_names:
            if idx not in existing:
                missing.append(f"{coll_label}/{idx}")
    status = "ok" if not missing and not errors else "fail"
    return {"status": status, "missing": missing, "errors": errors}

# graph-exempt: health check — no business logic; per BUG-ARCH-GRAPH-EXEMPT-001
BUILD_INFO_PATH = "/app/build_info.json"


def read_build_info():
    """Baked-at-build-time build/version/framework. Returns None if the
    file is absent (older image); caller falls back to frontEndAdmin.BuildVersions."""
    from pathlib import Path
    p = Path(BUILD_INFO_PATH)
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as _exc:
        # Mode 1 (REQ-B-008): caller falls back to placeholder build info;
        # operation continues. log.info + default debug-gated.
        log.info("build_info read failed (ignored, caller falls back): %s", _exc, exc=ChatHealthyException(
                                                                                      mode="build_info_read_failed",
                                                                                      message=f"build_info read failed (ignored, caller falls back): {_exc}",
                                                                                      component="FindCareBackend",
                                                                                      exception=_exc,
                                                                                  ))
        return None


@app.post("/health")
def health():
    """Health-state report. Returns 200 always — the body's `status` field
    carries the result. /health is a state report, not a fatal-trigger.

    Source priority for build/version/framework:
      1. /app/build_info.json — baked at image build time, truthful about
         what's actually running.
      2. frontEndAdmin.BuildVersions latest doc — legacy fallback for older images.
    """
    env_label = ENV_PREFIX if os.getenv("SPACE_ID") else "local"
    idx_check = check_indexes()
    _build = None
    _version_str = None
    _git_number = None
    _commit = None
    _built_at = None
    _version_error = None
    _source = None
    db = get_db()
    mongo_doc = {}
    if db is not None:
        try:
            mongo_doc = db["frontEndAdmin"]["BuildVersions"].find_one(sort=[("from", -1)]) or {}
        except Exception as _exc:
            # Mode 2 (REQ-B-008): Mongo read for /health version info failed;
            # endpoint still returns a body but version fields are empty.
            # Operator MUST know about Mongo unreachability.
            log.error("/health: MongoDB read for build/version/framework failed: %s", _exc, exc=ChatHealthyException(
                                                                                               mode="health_mongo_read_failed",
                                                                                               message=f"/health: MongoDB read for build/version/framework failed: {_exc}",
                                                                                               component="FindCareBackend",
                                                                                               exception=_exc,
                                                                                           ), if_not_debug_log=True)
            _version_error = f"{type(_exc).__name__}: {_exc}"

    baked = read_build_info()
    if baked is not None:
        _build = baked.get("build")
        _commit = baked.get("commit")
        _built_at = baked.get("built_at")
        _version_str = baked.get("version") or mongo_doc.get("version")
        _git_number = baked.get("commit") or mongo_doc.get("git_number")
        _source = "build_info.json"
    else:
        _build = mongo_doc.get("build")
        _version_str = mongo_doc.get("version")
        _git_number = mongo_doc.get("git_number")
        _source = "frontEndAdmin.BuildVersions"

    db_status = "connected" if db is not None and _version_error is None else (
        "unavailable" if db is None else "unreachable")
    status = "ok" if (idx_check["status"] == "ok" and db_status == "connected") else "degraded"
    result = {"status": status,
              "service": "find_care",
              "db": db_status,
              "env": env_label,
              "build": _build,
              "commit": _commit,
              "built_at": _built_at,
              "version": _version_str,
              "git_number": _git_number,
              "source": _source}
    if idx_check.get("missing"):
        result["missing_indexes"] = idx_check["missing"]
        log.error("HEALTH CHECK: missing indexes — %s", idx_check["missing"])
    if _version_error:
        result["version_error"] = _version_error
    # v2.2 Part B 7.6 — return 503 instead of 200 when Mongo is
    # unreachable. The Website fetch wrapper paints chFatalError on 503,
    # turning /health into the visible operator surface that the
    # rotation-as-operational-response model depends on.
    if db_status != "connected":
        log.error("/health returning 503 — db not connected; result=%s",
                  result, extra={"fatal_error": True})
        return JSONResponse(status_code=503, content=result)
    return result

from chathealthy_lib.authentication import (
    AuthToken, SessionRestampRequest, SessionToken, VerifyTokenResponse,
)

ORIGIN = "FindCare"


# ---------------------------------------------------------------------------
# No browser-addressable surface
# ---------------------------------------------------------------------------
# The React application is served by the website, not from here. This Space
# therefore serves nothing a browser loads directly, which is what lets
# every route on it require a SharedServices signature -- a bundle route
# that required one could not be loaded by the iframe that needs it.

if __name__ == "__main__":
    import uvicorn
    kwargs = {"host": "0.0.0.0", "port": int(os.getenv("PORT", "7860"))}
    ssl_cert = os.getenv("SSL_CERTFILE")
    ssl_key = os.getenv("SSL_KEYFILE")
    if ssl_cert and ssl_key and os.path.exists(ssl_cert) and os.path.exists(ssl_key):
        kwargs["ssl_certfile"] = ssl_cert
        kwargs["ssl_keyfile"] = ssl_key
    uvicorn.run(app, **kwargs)
