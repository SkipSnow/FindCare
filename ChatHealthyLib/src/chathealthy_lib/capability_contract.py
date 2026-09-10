# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""The one contract a capability's search answers to.

A capability is the unit: a search, a detail, a selection and a summary
over one kind of thing. This module holds what every capability's search
takes, what it returns, how it fails, and where it is served. It holds
those four things once so that splitting the routes cannot produce
modules that disagree in the ways the routes disagree now.

WHERE IT IS SERVED is the part that was written into the callers. Eight
tools each carried their own service address as a module default, and two
of them named a different host for the same service. A default is a
fallback, a fallback is a second answer nobody chose, and two fallbacks
for one service is how they came to disagree. Here the address is read
from the declaration the deploy writes, and its absence raises.

The declaration is ChatHealthyConfig.ToolConfiguration on the front-end
cluster, written from deployment_architecture.json at deploy time.
"""
from __future__ import annotations

import os
from typing import Any, ClassVar, Optional

from pydantic import BaseModel, Field

from .exceptions import ChatHealthyException
from .mongo_utilities import ChatHealthyMongoUtilities

COMPONENT = "CapabilityContract"

CONFIG_DATABASE = "ChatHealthyConfig"
CONFIG_COLLECTION = "ToolConfiguration"
CONFIG_IDENTITY = "frontendUser"
CONFIG_CLUSTER = "ChatHealthyFrontEnd"

# The logical server name the declaration uses, mapped to the variable the
# deploy resolves from peer_url:<target>. This mapping is the residual: the
# record declares the variable and declares the server, and does not
# declare that these two are the same service. It belongs in the record
# beside the server name, and it is here until the record carries it.
SERVER_ADDRESS_VARIABLE = {
    "find_care": "FINDCARE_INTERNAL_URL",
    "evaluate_care": "EVALCARE_INTERNAL_URL",
    "shared_services": "SHAREDSERVICES_INTERNAL_URL",
}


class DialogueTurn(BaseModel):
    """One turn of what was already said, in the order it was said."""

    actor: str = Field(description="Who spoke: the user, or the application.")
    content: str = Field(description="What was said, verbatim.")


class CapabilitySearchRequest(BaseModel):
    """What a capability's search takes, and nothing else.

    The utterance and the prior dialogue. Not a resolved parameter set:
    a caller that passes resolved slots has already decided what the
    search means, which is the capability's decision and not the
    caller's. A capability that needs a resolved slot reads it from the
    session it was given, or asks for it.
    """

    utterance: str = Field(description="What the user said, verbatim.")
    prior_dialogue: list[DialogueTurn] = Field(
        default_factory=list,
        description="Every turn already spoken in this session, oldest "
                    "first. Empty on the first utterance.")


class CapabilitySearchResponse(BaseModel):
    """The envelope every capability's search returns.

    A concrete capability subclasses this and types its own results. The
    envelope is here so that position in a long result, and the count a
    summary reports, mean the same thing in every capability.
    """

    results: list[Any] = Field(
        default_factory=list,
        description="This page of results, in the capability's own order.")
    cursor: Optional[str] = Field(
        default=None,
        description="Position this page was taken relative to. Absent "
                    "means the first page.")
    next_cursor: Optional[str] = Field(
        default=None,
        description="Position the next page would be taken relative to. "
                    "Absent means there is no next page.")
    total: Optional[int] = Field(
        default=None,
        description="How many results the search matched, where the "
                    "capability can say. Absent means it cannot, which is "
                    "not the same as zero.")


class CapabilityContract:
    """What a capability declares about itself.

    A capability names itself once, in the spelling the dispatcher uses.
    Everything else about where it lives is read.
    """

    CAPABILITY: ClassVar[str]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not getattr(cls, "CAPABILITY", None):
            raise ChatHealthyException(
                mode="capability_contract_subclass_missing_name",
                message=(f"CapabilityContract subclass {cls.__name__} "
                         f"declares no CAPABILITY"),
                component=COMPONENT,
            )


def _declaration() -> dict:
    utilities = ChatHealthyMongoUtilities()
    client = utilities.getConnection(CONFIG_IDENTITY, CONFIG_CLUSTER)
    record = client[CONFIG_DATABASE][CONFIG_COLLECTION].find_one(
        {"tools": {"$exists": True}})
    if not record:
        raise ChatHealthyException(
            mode="capability_declaration_absent",
            message=(f"{CONFIG_DATABASE}.{CONFIG_COLLECTION} holds no record "
                     f"carrying tools. Nothing declares where a capability "
                     f"is served, so no capability can be reached."),
            component=COMPONENT,
        )
    return record


def declared_capabilities() -> dict[str, dict]:
    """Every capability the declaration names, keyed by its name."""
    declared = {}
    for entry in _declaration().get("tools") or []:
        name = entry.get("tool")
        if name:
            declared[name] = entry
    return declared


def served_at(capability: str) -> str:
    """The address a capability is served at, read from the declaration.

    Raises where the declaration does not name the capability, does not
    say which server holds it, or does not say at what endpoint. Each of
    those is a capability that cannot be reached, and each was previously
    a module default that answered anyway.
    """
    entry = declared_capabilities().get(capability)
    if entry is None:
        raise ChatHealthyException(
            mode="capability_not_declared",
            message=(f"{capability!r} is not named in "
                     f"{CONFIG_DATABASE}.{CONFIG_COLLECTION}. A capability "
                     f"that runs and is not declared cannot be reached from "
                     f"its declaration."),
            component=COMPONENT,
            context={"capability": capability,
                     "declared": sorted(declared_capabilities())},
        )

    server = entry.get("server")
    endpoint = entry.get("endpoint")
    if not server or not endpoint:
        raise ChatHealthyException(
            mode="capability_declaration_incomplete",
            message=(f"{capability!r} is declared without a server or an "
                     f"endpoint, so the declaration cannot say where it is "
                     f"served."),
            component=COMPONENT,
            context={"capability": capability, "entry": entry},
        )

    variable = SERVER_ADDRESS_VARIABLE.get(server)
    if variable is None:
        raise ChatHealthyException(
            mode="capability_server_unknown",
            message=(f"{capability!r} declares server {server!r}, which maps "
                     f"to no address variable."),
            component=COMPONENT,
            context={"capability": capability, "server": server,
                     "known": sorted(SERVER_ADDRESS_VARIABLE)},
        )

    base = os.getenv(variable)
    if not base:
        raise ChatHealthyException(
            mode="capability_address_unset",
            message=(f"{capability!r} is served by {server!r} at {variable}, "
                     f"which is unset in this process. The deploy resolves "
                     f"it from the record; an unset value means the target "
                     f"was not given the peer it needs."),
            component=COMPONENT,
            context={"capability": capability, "variable": variable},
        )

    return base.rstrip("/") + "/" + endpoint.lstrip("/")
