# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""ClinicalTrials dispatcher.

SharedServices does NOT host the clinical-trials tool, and does not read
the utterance that reaches it. This dispatcher posts the person's latest
utterance and the talk before it to the FindCare backend's /trial/find
endpoint over HTTP, then streams the FindCare response's NDJSON events
back into the user's /gate stream so the React widget receives them
transparently. What the utterance means -- the condition, the age, the
sex, whether to stay inside the United States -- is the clinical-trial
page's to decide, and it decides it on the far side of this call.

Canonical *_tool.py exports: TOOL_NAME, Request, Response, run().
"""
from __future__ import annotations

import json as _json
import os
from typing import Any, Optional

import httpx
from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
from pydantic import BaseModel, Field

from chathealthy_lib.authentication.agent_deps import AgentDeps
from chathealthy_lib.authentication.chathealthy_tool import ChatHealthyTool

log = ChatHealthyLoggingService()


FINDCARE_INTERNAL_URL_ENV = "FINDCARE_INTERNAL_URL"
FINDCARE_INTERNAL_URL_DEFAULT = "https://ch-findcare:7860"


def findcare_url() -> str:
    return os.environ.get(FINDCARE_INTERNAL_URL_ENV) or FINDCARE_INTERNAL_URL_DEFAULT


class Request(BaseModel):
    utterance: str = Field(
        description="What the person last said. The clinical-trial page "
                    "reads it; nothing on this side does.")
    history: list[dict] = Field(
        default_factory=list,
        description="The talk before that utterance, oldest first, so a "
                    "condition or an age named on an earlier turn is still "
                    "there to be read.")


class Response(BaseModel):
    error: Optional[str] = None


class ClinicalTrialsDispatcher(ChatHealthyTool):
    TOOL_NAME = "clinical_trials_dispatcher"
    Request = Request
    Response = Response

    async def run(self, deps: AgentDeps, request: "Request") -> "Response":
        url = findcare_url() + "/trial/find"
        body = request.model_dump()
        # The token this hop already holds, forwarded so FindCare can
        # verify the SharedServices signature on it.
        body["session_token"] = deps.session_token.model_dump(mode="json")
        try:
            async with httpx.AsyncClient(timeout=None, verify=False) as client:
                async with client.stream("POST", url, json=body) as resp:
                    resp.raise_for_status()
                    async for raw_line in resp.aiter_lines():
                        if not raw_line.strip():
                            continue
                        try:
                            evt = _json.loads(raw_line)
                        except _json.JSONDecodeError:
                            continue
                        deps.stream(evt)
            return Response()
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.WriteTimeout, httpx.PoolTimeout, httpx.ReadError,
                httpx.WriteError, httpx.RemoteProtocolError,
                httpx.HTTPStatusError) as exc:
            log.error(
                "FindCare /trial/find call failed: %s: %s",
                type(exc).__name__, exc,
                exc=ChatHealthyException(
                    mode="clinical_trials_unavailable",
                    message=f"FindCare /trial/find call failed: {type(exc).__name__}: {exc}",
                    component="ClinicalTrialsDispatcher",
                    exception=exc,
                ), if_not_debug_log=True,
            )
            err = "Clinical trials lookup is unavailable right now. Please try again in a moment."
            # Emit a single final empty chunk so the widget stops the
            # spinner and shows the error.
            deps.stream({
                "kind": "clinical_trials_chunk",
                "data": {
                    "trials": [],
                    "chunk_index": 0,
                    "is_final": True,
                    "total_eligible": 0,
                    "is_partial": False,
                    "error": err,
                },
            })
            return Response(error=err)


TOOL = ClinicalTrialsDispatcher()
