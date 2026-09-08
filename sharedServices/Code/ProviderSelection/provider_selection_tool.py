# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""ProviderSelection tool — owns user_object.selected_providers.

State (the list of NPIs the user has curated for EvaluateCare handoff)
lives on user_object so it survives page refresh. The tool exposes three
verbs: select, deselect, list. Each broadcasts kind:'selection_changed'
with the current list so the SelectedProvidersWidget repaints its strip.
"""
from __future__ import annotations

import os

from typing import Literal, Optional

import httpx
from pydantic import BaseModel, Field

from chathealthy_lib.exceptions import ChatHealthyException
from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.authentication.agent_deps import AgentDeps
from chathealthy_lib.authentication.chathealthy_tool import ChatHealthyTool

log = ChatHealthyLoggingService()




# FindCare owns the care-giver records and the rules about them; this
# component owns the session and the person's chosen set. A component
# holding identities asks the owner a question about them rather than
# fetching the records and judging them itself (C-26).
FINDCARE_INTERNAL_URL_ENV = "CH_INTERNAL_PEER_URL_FINDCARE"
FINDCARE_INTERNAL_URL_DEFAULT = "https://localhost:7860"


def _findcare_url() -> str:
    return (os.environ.get(FINDCARE_INTERNAL_URL_ENV)
            or FINDCARE_INTERNAL_URL_DEFAULT)


async def _excluded_by_filter(deps, npis: list) -> dict:
    """Which of these the filter in force sets aside, asked of its owner.

    This component sends identities and nothing else. Which specialties are
    in force is a parameter of the care-giver page, and whether one admits a
    care giver is that page's rule about its own records -- so both stay
    there and neither is named here.

    A failure costs the exclusion marks, not the selection: the person keeps
    what they chose and the strip is drawn without the flag rather than the
    turn dying.
    """
    wanted = [n for n in (npis or []) if n]
    if not wanted:
        return {}
    body = {
        "npis": wanted,
        "session_token": deps.session_token.model_dump(mode="json"),
    }
    try:
        async with httpx.AsyncClient(timeout=None, verify=False) as client:
            r = await client.post(_findcare_url() + "/provider/exclusions",
                                  json=body)
            r.raise_for_status()
            return (r.json() or {}).get("excluded") or {}
    except Exception as exc:
        log.error("FindCare exclusion marks failed for %d record(s): %s",
                  len(wanted), exc,
                  exc=ChatHealthyException(
                      mode="findcare_exclusion_marks_failed",
                      message=f"FindCare exclusion marks failed: {exc}",
                      component="SharedServices",
                      exception=exc if isinstance(exc, Exception) else None,
                  ))
        return {}


MAX_SELECTED = 5


class Request(BaseModel):
    verb: Literal["select", "deselect", "list"] = Field(
        description="Add a provider to the evaluation set, remove one, or "
                    "return the set.")
    npi: Optional[str] = Field(
        default=None,
        description="Which provider. Required for select and deselect.")


class Response(BaseModel):
    selected: list[str] = Field(default_factory=list)
    max_selected: int = MAX_SELECTED
    full: bool = False
    # Per selected NPI: whether the specialty filter in force excludes that
    # provider (EPIC-006-F-001-S-002-REQ-B-019). The row is kept and marked,
    # never dropped -- a filter that silently discards a person's own choice
    # is the failure this exists to prevent.
    excluded_by_filter: dict[str, bool] = Field(default_factory=dict)
    error: Optional[str] = None


class ProviderSelectionTool(ChatHealthyTool):
    TOOL_NAME = "provider_selection"
    Request = Request
    Response = Response

    async def run(self, deps: AgentDeps, request: "Request") -> "Response":
        user_obj = deps.user_object
        current = list(user_obj.selected_providers or [])
        error: Optional[str] = None

        if request.verb == "select":
            npi = (request.npi or "").strip()
            if not npi:
                error = "npi required for select"
            elif npi in current:
                pass
            elif len(current) >= MAX_SELECTED:
                error = f"selection full (max {MAX_SELECTED})"
            else:
                current.append(npi)

        elif request.verb == "deselect":
            npi = (request.npi or "").strip()
            if not npi:
                error = "npi required for deselect"
            else:
                current = [n for n in current if n != npi]

        user_obj.selected_providers = current

        resp = Response(
            selected=current,
            max_selected=MAX_SELECTED,
            full=len(current) >= MAX_SELECTED,
            excluded_by_filter=await _excluded_by_filter(deps, current),
            error=error,
        )
        deps.stream({"kind": "selection_changed", "data": resp.model_dump(exclude_none=True)})
        return resp


TOOL = ProviderSelectionTool()
