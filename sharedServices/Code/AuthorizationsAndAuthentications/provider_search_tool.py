# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""ProviderSearch tool — queries the provider DB given the
specialty codes + location resolved by SpecialtyFilter, returns the
provider list payload the FE's center panel renders.

The Agent contract is user_object-based: the prior tool (SpecialtyFilter)
wrote the codes + state to its own response object, which Universal-
Navigation hands here as the Request body. Internally, iteration 1 calls
the existing FindCare `/search` endpoint over HTTP for byte-identical
behavior. Iteration 2 inlines the search.

Canonical *_tool.py exports: TOOL_NAME, Request, Response, run().
"""
from __future__ import annotations

from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
import os
from typing import Any, Optional

import httpx
from pydantic import BaseModel, Field

from chathealthy_lib.authentication.agent_deps import AgentDeps
from chathealthy_lib.authentication.chathealthy_tool import ChatHealthyTool

log = ChatHealthyLoggingService()


FINDCARE_INTERNAL_URL_ENV = "FINDCARE_INTERNAL_URL"
FINDCARE_INTERNAL_URL_DEFAULT = "https://ch-findcare:7860"

# The page this tool serves, and the entity type that page returns. The
# entity type is a property of the page rather than a parameter of it
# (EPIC-006-F-008-S-006-REQ-B-005): every record the individual-provider
# page can return is an individual, so a caller can neither pass it nor
# omit it.
PAGE = "individualProvider"
PAGE_ENTITY_TYPE = "1"


class Request(BaseModel):
    specialty_codes: list[str] = Field(
        default_factory=list,
        description="NUCC taxonomy codes. A provider matches if ANY taxonomy "
                    "it holds carries one of these, not only its primary one, "
                    "so a result can display a specialty that is not in this "
                    "list. Empty searches every code the specialty step "
                    "offered.")
    state: Optional[str] = Field(
        default=None,
        description="Two-letter USPS code, uppercase. Sufficient on its own.")
    city: Optional[str] = Field(
        default=None,
        description="City name. NOT sufficient alone -- city names repeat "
                    "across states, so a state is required with it.")
    county: Optional[str] = Field(
        default=None,
        description="County name. NOT sufficient alone; requires a state.")
    zip: Optional[str] = Field(
        default=None,
        description="Five-digit ZIP. Sufficient on its own.")
    limit: int = Field(
        default=25, description="How many providers this page returns.")
    cursor: Optional[str] = Field(
        default=None,
        description="Keyset position: the NPI this page is taken relative to. "
                    "Absent means the first page. This is how position in a "
                    "long result is carried, since results are ordered by NPI "
                    "rather than numbered.")
    direction: str = Field(
        default="forward",
        description="forward for the providers after the cursor, back for "
                    "those before it. Back inverts the comparison and the "
                    "sort and reverses the page, so it costs what forward "
                    "costs and needs no history of the cursors walked.")
    last_name: Optional[str] = Field(
        default=None,
        description="Surname, when a provider is named outright rather than "
                    "searched for by what they do. Matched exactly.")
    first_name: Optional[str] = Field(
        default=None,
        description="Given name or its initial. Matches either way round: "
                    "JAMES also finds a record holding J, and J also finds "
                    "JAMES.")
    middle_name: Optional[str] = Field(
        default=None,
        description="Middle name or initial, matched the same way. This is "
                    "what separates people who share a first and last name; "
                    "there are 162 Richard Smiths.")
    provider_sex: Optional[str] = Field(
        default=None,
        description="A stated preference about the provider. F Female, "
                    "M Male, X neither male nor female, U undisclosed. X is "
                    "an affirmation the provider made and U is a refusal to "
                    "stipulate -- never merge them, and never present either "
                    "as meaning transgender, which this data does not "
                    "record. Stating a preference excludes everyone who "
                    "does not match it.")
    sole_proprietor: Optional[bool] = Field(
        default=None,
        description="True for a provider practising on their own account.")
    insurance: Optional[str] = Field(
        default=None,
        description="Narrows to providers who list this payer among their "
                    "identifiers in the NPI database. Our insurance "
                    "information is weak: registering a payer identifier is "
                    "voluntary and sparse, so most providers list none at "
                    "all. Say that plainly before offering to use it. It is "
                    "weakest in psychiatry, where many providers take no "
                    "insurance by choice -- about a third list any payer "
                    "against half in paediatrics -- so a psychiatrist "
                    "listing none may genuinely take none. It follows that a "
                    "provider's ABSENCE from these results says nothing "
                    "about whether they take that insurance. It is also NOT "
                    "network membership: nothing here establishes whether a "
                    "particular plan covers a particular provider, and that "
                    "must never be stated or implied. Where a person needs a "
                    "real answer about coverage or network, tell them "
                    "EvaluateCare goes further into insurance than a "
                    "provider search can.")


class Response(BaseModel):
    providers: list[dict] = Field(default_factory=list)
    has_more: bool = False
    first_npi: Optional[str] = None
    last_npi: Optional[str] = None
    count: int = 0
    total_count: int = 0
    page_start: int = 1
    page_end: int = 0
    search_params: Optional[dict] = None
    specialization_options: Optional[list[dict]] = None
    state: Optional[str] = None
    # Built by the search from the structured result and never written by a
    # model. It travels on the same envelope as the rows so the summary and
    # the rows paint in one repaint (EPIC-006-F-001-S-005-REQ-B-009).
    summary_message: Optional[str] = None
    # What the person may still narrow by, and what each choice would cost
    # them, counted over THIS result. The panel shows these so a preference
    # is made with its price visible rather than discovered afterwards.
    refinements: dict = Field(default_factory=dict)
    error: Optional[str] = None


def findcare_url() -> str:
    return os.environ.get(FINDCARE_INTERNAL_URL_ENV) or FINDCARE_INTERNAL_URL_DEFAULT


class ProviderSearchTool(ChatHealthyTool):
    """Pure-DB tool (no LLM Agent inside). Given specialty codes + state
    from deps.user_object.find_care (or via Request), HTTP-calls FindCare
    /search and returns the providers list. Same ChatHealthyTool interface
    as LLM-driven tools."""
    TOOL_NAME = "provider_search"
    Request = Request
    Response = Response

    async def run(self, deps: AgentDeps, request: "Request") -> "Response":
        if not request.specialty_codes:
            # Mode 2 (REQ-B-008): precondition failure surfaced inline as
            # Response.error. NOT 503; no fatal_error tag.
            log.error("provider_search precondition failed: no specialty codes",
                      exc=ChatHealthyException(
                          mode="provider_search_no_specialty_codes",
                          message="provider_search precondition failed: no specialty codes",
                          component="ProviderSearchTool",
                      ), if_not_debug_log=True)
            resp = self.Response(error="No specialty codes; cannot search providers.")
            deps.stream({"kind": "providers", "data": resp.model_dump(exclude_none=True)})
            return resp

        body: dict[str, Any] = {
            # The token this hop already holds, signed by whichever server
            # stamped it. Nothing upstream changes shape -- the token is
            # deps, not a request field.
            "session_token": deps.session_token.model_dump(mode="json"),
            "entity_type": PAGE_ENTITY_TYPE,
            "nucc_codes": request.specialty_codes,
            "limit": request.limit,
            "direction": request.direction,
        }
        if request.state:
            body["state"] = request.state
        if request.city:
            body["city"] = request.city
        if request.county:
            body["county"] = request.county
        if request.zip:
            body["zip"] = request.zip
        if request.cursor:
            body["cursor"] = request.cursor
        for field in ("last_name", "first_name", "middle_name",
                      "provider_sex", "insurance"):
            value = getattr(request, field, None)
            if value:
                body[field] = value
        if request.sole_proprietor is not None:
            body["sole_proprietor"] = request.sole_proprietor

        url = findcare_url() + "/search"
        try:
            async with httpx.AsyncClient(timeout=None, verify=False) as client:
                r = await client.post(url, json=body)
                r.raise_for_status()
                raw = r.json()
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.WriteTimeout, httpx.PoolTimeout, httpx.ReadError,
                httpx.WriteError, httpx.RemoteProtocolError,
                httpx.HTTPStatusError) as exc:
            # Mode 2 (REQ-B-008): FC /search temporarily unavailable
            # (network/HTTP-status failure). Tool returns graceful
            # Response.error inline. NOT 503; no fatal_error tag.
            log.error("FindCare /search call failed: %s: %s",
                       type(exc).__name__, exc,
                       exc=ChatHealthyException(
                        mode="search_unavailable",
                        message=f"FindCare /search call failed: {type(exc).__name__}: {exc}",
                        component="ProviderSearchTool",
                        exception=exc,
                    ), if_not_debug_log=True)
            resp = self.Response(
                error="Provider search is taking longer than usual. "
                      "Please try the same search again in a moment.",
            )
            deps.stream({"kind": "providers", "data": resp.model_dump(exclude_none=True)})
            return resp

        resp = self.Response(
            providers=raw.get("providers") or [],
            has_more=bool(raw.get("has_more", False)),
            first_npi=raw.get("first_npi"),
            last_npi=raw.get("last_npi"),
            count=int(raw.get("count", 0) or 0),
            total_count=int(raw.get("total_count", 0) or 0),
            page_start=int(raw.get("page_start", 1) or 1),
            page_end=int(raw.get("page_end", 0) or 0),
            search_params=raw.get("search_params"),
            specialization_options=raw.get("specialization_options"),
            state=raw.get("state") or request.state,
            refinements=raw.get("refinements") or {},
            summary_message=raw.get("summary_message"),
        )
        deps.stream({"kind": "providers", "data": resp.model_dump(exclude_none=True)})
        return resp


TOOL = ProviderSearchTool()
