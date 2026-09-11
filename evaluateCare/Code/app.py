# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# EvaluateCare Service — FastAPI app on port 8001.
# Each FastAPI route constructs the dedicated endpoint class and runs it.
# Endpoint classes live in api/, healthcheck/, externalInterface/.

# Establish which component this process is and what the library will let it
# load, before any other library capability is imported. The finder installed
# here refuses a forbidden module at import, so a late import inside a
# function is caught the same as one at the top of a file -- which only holds
# if nothing has been imported ahead of this call.
from chathealthy_lib.permissions import initialize as _ch_permissions_init
_ch_permissions_init()

import os
import sys
import time
import base64
import tempfile
from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Code/ on sys.path so api/, healthcheck/, externalInterface/, security/
# all import as top-level packages.

log = ChatHealthyLoggingService()


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# This service acts as frontendUser, including when it writes its own
# logs. The Mongo log handler refuses to build without an identity, and
# nothing else in this process sets one.
from chathealthy_lib.logging_service import set_mongo_log_identity
set_mongo_log_identity("frontendUser")

app = FastAPI(title="ChatHealthy.ai EvaluateCare", version="0.1.4")

# Every runtime is rebindable, not only the one that happens to read
# versioned collections today. /admin/swap is how a data version is
# activated, and a service that does not expose it cannot be told which
# collection generation to serve -- so a version activation would silently
# cover part of the estate and report success. Mounting the router costs
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
    return JSONResponse(status_code=status, content={"detail": exc.message})

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
        content={"service": "EvaluateCare", "source": "unhandled",
                 "time": dt.datetime.now(dt.timezone.utc).isoformat()},
    )


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    elapsed = round((time.time() - start) * 1000)
    log.info(
        "%s %s → %d (%dms) from %s",
        request.method, request.url.path, response.status_code, elapsed,
        request.headers.get("x-forwarded-for", request.client.host if request.client else "unknown"),
    )
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://localhost", "https://localhost:443", "https://localhost:3000",
                   "https://localhost:8080", "https://localhost:8081",
                   "https://chathealthy.ai", "https://dev.chathealthy.ai"],
    allow_origin_regex=r"https://localhost(:\d+)?$|https://[a-zA-Z0-9-]+\.chathealthy\.ai$|https://skipsnow-[a-zA-Z0-9-]+\.hf\.space$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Routes — each constructs its endpoint class and runs it ──

from healthcheck.health_endpoint import HealthEndpoint
from displayChrome.splash_endpoint import SplashEndpoint
from displayChrome.transfer_to_findcare_endpoint import TransferToFindCareEndpoint
from externalInterface.evaluate_providers_endpoint import (
    EvaluateProvidersEndpoint,
    EvaluateProvidersRequest,
    EvaluateProvidersResponse,
)
from chathealthy_lib.authentication import (
    AuthToken, SessionRestampRequest, SessionToken, VerifyTokenResponse,
)

ORIGIN = "EvaluateCare"
ENV = os.getenv("ENV_PREFIX", "dev")


def impl(cls_name, file_subpath):
    return {
        "x-implementing-class": cls_name,
        "x-implementing-file": f"evaluateCare/Code/{file_subpath}",
    }


@app.post("/health", operation_id="HealthEndpoint",
          openapi_extra=impl("HealthEndpoint", "healthcheck/health_endpoint.py"))
def health():
    # v2.2 Part B 7.6/7.7 — return 503 (not 200) when Mongo is
    # unreachable so the Website fetch wrapper triggers chFatalError.
    payload = HealthEndpoint()()
    if payload.get("db") != "connected":
        log.error("/health returning 503 — db not connected; payload=%s",
                  payload, extra={"fatal_error": True})
        return JSONResponse(status_code=503, content=payload)
    return payload


@app.post("/splash", operation_id="SplashEndpoint",
          openapi_extra=impl("SplashEndpoint", "displayChrome/splash_endpoint.py"))
def splash():
    return SplashEndpoint()()


@app.post("/evaluate/providers", operation_id="EvaluateProvidersEndpoint", response_model=EvaluateProvidersResponse,
          openapi_extra=impl("EvaluateProvidersEndpoint", "externalInterface/evaluate_providers_endpoint.py"))
def evaluate_providers(body: EvaluateProvidersRequest):
    return EvaluateProvidersEndpoint()(body)


# ── Run ─────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8001"))
    log.info("EvaluateCare starting on port %d", port)
    kwargs = {"host": "0.0.0.0", "port": port}
    ssl_cert = os.getenv("SSL_CERTFILE")
    ssl_key = os.getenv("SSL_KEYFILE")
    if ssl_cert and ssl_key and os.path.exists(ssl_cert) and os.path.exists(ssl_key):
        kwargs["ssl_certfile"] = ssl_cert
        kwargs["ssl_keyfile"] = ssl_key
    uvicorn.run(app, **kwargs)
