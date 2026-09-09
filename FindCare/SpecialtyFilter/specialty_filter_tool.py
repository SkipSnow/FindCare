# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""SpecialtyFilter tool — turns the user's latest utterance into NUCC
specialty codes + location, then writes the result onto user_object.

The Agent contract is user_object-based: deps.user_object is the input
substrate; this tool reads `utterances[-1].text` as the user's query
and writes the picked codes into `user_object.find_care`. Iteration 1
internally HTTP-calls the existing FindCare `/classify` endpoint so the
specialty pipeline (normalize + embed + $vectorSearch + LLM filter, filter.py
engine) is reused verbatim and behavior is preserved exactly. Iteration
2 pulls that engine in-process and removes the HTTP hop.

Canonical *_tool.py exports: TOOL_NAME, Request, Response, run().
"""
from __future__ import annotations

from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
import os
from typing import Optional

import httpx
from pydantic import BaseModel, Field

from chathealthy_lib.authentication.agent_deps import AgentDeps
from chathealthy_lib.authentication.chathealthy_tool import ChatHealthyTool

log = ChatHealthyLoggingService()


FINDCARE_INTERNAL_URL_ENV = "FINDCARE_INTERNAL_URL"
FINDCARE_INTERNAL_URL_DEFAULT = "https://ch-findcare:7860"



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
    """The natural-language complaint UM extracted from the user's utterance.
    Required and non-empty; SpecialtyFilter does not source from any other
    field on user_object."""
    model_config = {"extra": "ignore"}
    section: str = Field(
        default="Individual",
        description="Which partition of the catalogue this resolution "
                    "reads: Individual for a care giver, Non-Individual "
                    "for a facility. The same funnel runs either way; this "
                    "is the one thing its queries differ by.")
    query: str = Field(
        description="The kind of care wanted, in clinical terms rather than "
                    "the words the person used -- \"psychological problem\", "
                    "not \"shrink\". Returns the provider types that treat "
                    "it. Carries no location and finds no providers.")


class SpecialtyRow(BaseModel):
    code: str
    name: str
    can_prescribe: Optional[bool] = None
    homeopathic: Optional[bool] = None
    rank: Optional[int] = None
    homeopathic_general: Optional[bool] = None


class Response(BaseModel):
    # What the user's words MEAN, in clinical terms: 'shrink' arrives here
    # as 'psychological problem'. The utterance stays in the conversation;
    # this is the translated fact every other tool reads.
    complaint: str = ""
    specialties: list[SpecialtyRow] = Field(default_factory=list)
    homeopathic_generalists: list[SpecialtyRow] = Field(default_factory=list)
    model: Optional[str] = None
    error: Optional[str] = None


def findcare_url() -> str:
    return os.environ.get(FINDCARE_INTERNAL_URL_ENV) or FINDCARE_INTERNAL_URL_DEFAULT


# How many times a refused call is tried again, and how long it waits.
# A host that answers 429 is saying "later", not "no", and one refusal
# used to end the turn: no specialties, therefore no codes, therefore no
# search and no providers. A person saw an empty filter beside an empty
# list. Waiting a moment is the difference between a slow answer and no
# answer at all.
#
# Jittered so that several turns refused at once do not return together
# and reproduce the burst that caused the refusal.
_RETRY_ON = (429, 500, 502, 503, 504)
_ATTEMPTS = 5
_FIRST_WAIT_SECONDS = 1.0


async def _post_retrying(url: str, body: dict) -> dict:
    """POST, and try again while the far side is saying "later"."""
    import asyncio
    import random
    wait = _FIRST_WAIT_SECONDS
    last: Exception = ChatHealthyException(
        mode="runtime_error", component="specialty_filter_tool",
        message="no attempt was made")
    for attempt in range(1, _ATTEMPTS + 1):
        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                r = await client.post(url, json=body)
                if r.status_code in _RETRY_ON and attempt < _ATTEMPTS:
                    log.info("classify answered %s; attempt %d of %d, "
                             "waiting %.1fs", r.status_code, attempt,
                             _ATTEMPTS, wait)
                    await asyncio.sleep(wait + random.uniform(0, wait / 2))
                    wait *= 2
                    continue
                r.raise_for_status()
                return r.json()
        except httpx.HTTPStatusError:
            raise
        except Exception as exc:            # a connection that never landed
            last = exc
            if attempt >= _ATTEMPTS:
                break
            log.info("classify unreachable (%s); attempt %d of %d, "
                     "waiting %.1fs", type(exc).__name__, attempt,
                     _ATTEMPTS, wait)
            await asyncio.sleep(wait + random.uniform(0, wait / 2))
            wait *= 2
    raise last


class SpecialtyFilterTool(ChatHealthyTool):
    @staticmethod
    def _broadcast(deps, section: str, event: dict) -> None:
        """The offered-kinds panel belongs to the care-giver page.

        A facility resolution runs the same funnel but paints no such
        panel: emitting one would repaint the specialty panel the person
        is looking at with facility kinds they never asked to choose
        among.
        """
        if section == "Individual":
            deps.stream(event)

    """Vernacular text → NUCC specialty codes. Consumes the UM-extracted
    complaint phrase via Request.query; HTTP-calls the existing /classify
    engine; emits a stream event so the FE renders the filter as soon as
    picks arrive."""
    TOOL_NAME = "specialty_filter"
    Request = Request
    Response = Response

    async def run(self, deps: AgentDeps, request: "Request") -> "Response":
        text = (request.query or "").strip()

        if not text:
            raise ChatHealthyException(
            mode="value_error",
            component="specialty_filter_tool",
            message="SpecialtyFilter requires a non-empty Request.query; UR "
                "must pass the UM-extracted complaint phrase.")

        url = findcare_url() + "/nucc/classify"
        try:
            # The token this hop already holds, forwarded so FindCare can
            # verify the SharedServices signature on it. This is the fourth
            # cross-service hop, and it needs the token for the same reason
            # the provider search, the provider detail and the
            # clinical-trials dispatcher do.
            raw = await _post_retrying(url, {
                "message": text,
                "section": request.section,
                "session_token": deps.session_token.model_dump(mode="json"),
            })
        except Exception as exc:
            # Mode 2 (REQ-B-008): LLM /classify temporarily unavailable; the
            # tool returns a graceful Response.error to the user inline. NOT
            # a 503; do NOT tag fatal_error=True.
            #
            # Recorded because the graceful answer is indistinguishable on
            # screen from a search that found nothing: the panel paints its
            # chrome around no rows, no codes are produced, and no provider
            # search follows. A person sees an empty filter and an empty
            # list, and nothing anywhere says why.
            log.error("specialty classify failed for section=%s: %s: %s",
                      request.section, type(exc).__name__, exc,
                      exc=ChatHealthyException(
                          mode="specialty_classify_unavailable",
                          message=f"specialty classify failed: {exc}",
                          component="SpecialtyFilterTool",
                          exception=exc if isinstance(exc, Exception) else None,
                      ))
            # Nothing is broadcast. "I have nothing to say about this
            # surface" and "this surface is now empty" are different
            # statements and must not share a payload: emitting the
            # error as a specialties event painted an empty panel over a
            # good one, and the turn then ended with no list, no panel
            # and nothing said. Saying nothing here leaves the panel as
            # it was and leaves the turn with no answer, which is what
            # UR's end-of-turn check reads to ask the person instead.
            return self.Response(
                error=f"classify_unavailable: {type(exc).__name__}")

        specialties = [SpecialtyRow(**s) for s in (raw.get("specialties") or [])]
        homeo = [SpecialtyRow(**s) for s in (raw.get("homeopathic_generalists") or [])]
        resp = self.Response(
            complaint=str(raw.get("complaint") or "").strip(),
        specialties=specialties,
            homeopathic_generalists=homeo,
            model=raw.get("model"),
            error=raw.get("error"),
        )
        # Same rule as the failure path above: a panel is painted when
        # there are rows to paint. A run that matched nothing says
        # nothing about the panel, so what the person is looking at
        # stays, and the turn -- having shown nothing new -- ends by
        # asking rather than by going quiet.
        if specialties or homeo:
            self._broadcast(
                deps, request.section,
                {"kind": "specialties", "data": resp.model_dump(exclude_none=True)})
        return resp


TOOL = SpecialtyFilterTool()
