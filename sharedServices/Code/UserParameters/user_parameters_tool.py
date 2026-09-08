"""The one writer of the live user parameters.

Every other tool READS parameters off the session and writes none of its
own. There are three routes in and no fourth: a tool, the gateway, and the
utterance manager. They are three CALLERS of this one implementation
rather than three write paths, which is what makes the same request
produce the same stored value and the same refusal whoever asked -- by
construction rather than by discipline, because there is one validation
site.

A tool may address the page it belongs to; a tool that could write another
page's parameters would defeat the independence the page namespaces exist
for. The gateway and the utterance manager may address any page: the
gateway because it dispatches across pages and must clear a stale position
or an open detail no tool owns, the utterance manager because one thing a
person says may bear on two pages at once.

A parameter is addressed as a pair -- the page and the attribute -- and
there is no unqualified form to accept (S-006-REQ-B-001). Validation is
two lookups against the declaration: the page against the four, then the
attribute against that page's declared attributes.

Verbs are set / clear / clear_page / clear_all / read. There is no "merge"
verb: a parameter is whatever the last thing to set it left there, so two
changes to one parameter leave the later one, and a caller that wants to
combine values does so before calling.

A request may carry a changes[] list. Every change is validated, the whole
new parameter state is built, and it is written once -- which is how a
request changing more than one parameter leaves all of them changed or
none of them changed without a transaction, since the state is one
document.
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

import sys as _ch_sys
import pathlib as _ch_pl

for _ch_d in _ch_pl.Path(__file__).resolve().parents:
    if (_ch_d / ".git").exists():
        _ch_lib = _ch_d / "ChatHealthyLib" / "src"
        if str(_ch_lib) not in _ch_sys.path:
            _ch_sys.path.insert(0, str(_ch_lib))
        break

from chathealthy_lib.authentication.agent_deps import AgentDeps  # noqa: E402
from chathealthy_lib.authentication.chathealthy_tool import ChatHealthyTool  # noqa: E402
from chathealthy_lib.authentication.user_parameters import (  # noqa: E402
    Geography, PAGES, ParameterEntry, ProviderName, Specialty, UserParameters,
)
from chathealthy_lib.exceptions import ChatHealthyException  # noqa: E402
from chathealthy_lib.logging_service import ChatHealthyLoggingService  # noqa: E402
from chathealthy_lib.runtime_data_collections import declared_attributes  # noqa: E402

# Where a session lives. Named here rather than imported from the auth
# tool so this tool does not depend on it for two constants.
SESSION_DB = "Users"
SESSION_COLLECTION = "sessions"

log = ChatHealthyLoggingService()


class Change(BaseModel):
    """One parameter change. Several may travel on one request."""
    page: str = Field(
        description="One of the four pages. A name carrying a fifth page is "
                    "refused before its attribute is examined.")
    name: str = Field(
        description="The attribute, which that page must declare.")
    value: Any = Field(
        default=None,
        description="The new value, coerced by the declared value type.")


class Request(BaseModel):
    verb: Literal["set", "clear", "clear_page", "clear_all", "read"] = Field(
        default="read",
        description="read returns the parameters; set writes one or more; "
                    "clear removes one; clear_page empties one page; "
                    "clear_all empties every page.")
    page: Optional[str] = Field(
        default=None,
        description="Which page. Required for set, clear and clear_page: a "
                    "parameter name is a page and an attribute together, and "
                    "there is no default page.")
    name: Optional[str] = Field(
        default=None,
        description="Which attribute of that page.")
    value: Any = Field(
        default=None,
        description="The new value, validated into that attribute's shape.")
    changes: list[Change] = Field(
        default_factory=list,
        description="Several changes on one request. All of them are written "
                    "or none of them are.")
    route: Literal["tool", "gateway", "utterance_manager"] = Field(
        description="Which of the three routes is asking. No default: a "
                    "caller that does not say which route it is is refused, "
                    "because a default would make the commonest caller "
                    "invisible.")
    origin: Literal["deterministic", "non_deterministic"] = Field(
        default="deterministic",
        description="Whether a rule determined this value or a model "
                    "inferred it. Independent of the route.")
    carried_from: Optional[str] = Field(
        default=None,
        description="The source page, when this write is a carry-over.")


class Response(BaseModel):
    parameters: dict = Field(default_factory=dict)
    changed: list[str] = Field(default_factory=list)
    # The parameters that did not take, named. A change that did not take
    # is never reported as made (S-004-REQ-B-003, REQ-B-005).
    not_preserved: list[str] = Field(default_factory=list)
    error: Optional[str] = None


def _coerce(value_type: str, value: Any):
    """Validate a value by its DECLARED type, not by its attribute name.

    Keying off the declared value_type is what lets geography on the
    provider page and geography on the facility page coerce through one
    branch instead of two that can drift.
    """
    if value is None:
        return None
    if value_type == "geography":
        return (value if isinstance(value, Geography)
                else Geography(**dict(value))).model_dump(exclude_none=True)
    if value_type == "specialties":
        return [(s if isinstance(s, Specialty) else Specialty(**dict(s))
                 ).model_dump() for s in (value or [])]
    if value_type == "string_list":
        return [str(c) for c in (value or []) if str(c).strip()]
    if value_type == "provider_name":
        parts = (value if isinstance(value, ProviderName)
                 else ProviderName(**dict(value)))
        # Uppercased on the way in, because the records are uppercase and a
        # case-insensitive match cannot use the name index.
        return ProviderName(last=parts.last.strip().upper(),
                            first=parts.first.strip().upper(),
                            middle=parts.middle.strip().upper()
                            ).model_dump()
    if value_type == "sex_code":
        code = str(value or "").strip().upper()
        if code not in ("F", "M", "X", "U"):
            raise ChatHealthyException(
                mode="value_error",
                component="UserParametersTool",
                message=f"{code!r} is not a sex code. NPPES uses F, M, "
                        f"X (neither male nor female) and U (undisclosed).")
        return code
    if value_type == "boolean":
        return bool(value)
    if value_type == "integer":
        return int(value)
    if value_type == "position":
        # The first and last key of the page in view, which is what both
        # paging directions need.
        got = dict(value or {})
        return {"first": str(got.get("first") or ""),
                "last": str(got.get("last") or "")}
    if value_type == "string":
        return str(value or "").strip()
    raise ChatHealthyException(
        mode="value_error",
        component="UserParametersTool",
        message=f"the declaration gives value_type {value_type!r}, which this "
                "tool has no branch for.")


class UserParametersTool(ChatHealthyTool):
    TOOL_NAME = "user_parameters"
    Request = Request
    Response = Response
    Change = Change

    @staticmethod
    def _refuse(page: str, name: str) -> Optional[str]:
        """The refusal for one name, or None when the name is accepted."""
        if page not in PAGES:
            return (f"{page!r} is not one of the four pages: "
                    f"{', '.join(PAGES)}.")
        declared = declared_attributes(page)
        if name not in declared:
            return (f"page {page!r} declares no attribute {name!r}. It "
                    f"declares: {', '.join(sorted(declared)) or '(none)'}.")
        return None

    async def run(self, deps: AgentDeps, request: "Request") -> "Response":
        params = deps.user_object.userParameters or UserParameters()

        if request.verb == "read":
            return Response(parameters=params.model_dump(exclude_none=True))

        if request.verb == "clear_all":
            cleared = params.clear_all()
            deps.user_object.userParameters = params
            return Response(parameters=params.model_dump(exclude_none=True),
                            changed=cleared)

        if request.verb == "clear_page":
            page = (request.page or "").strip()
            if page not in PAGES:
                return Response(
                    parameters=params.model_dump(exclude_none=True),
                    error=f"{page!r} is not one of the four pages: "
                          f"{', '.join(PAGES)}.")
            cleared = params.clear_page(page)
            deps.user_object.userParameters = params
            return Response(parameters=params.model_dump(exclude_none=True),
                            changed=[f"{page}.{a}" for a in cleared])

        changes = list(request.changes)
        if not changes and request.name is not None:
            changes = [Change(page=(request.page or ""), name=request.name,
                              value=request.value)]
        if not changes:
            return Response(parameters=params.model_dump(exclude_none=True),
                            error="no parameter named. A parameter is "
                                  "addressed as a page and an attribute.")

        # Every change validated before any is written. Any change failing
        # validation means none is written (S-004-REQ-B-006).
        refused: list[str] = []
        prepared: list[tuple[str, str, Any]] = []
        for change in changes:
            page = (change.page or "").strip()
            name = (change.name or "").strip()
            refusal = self._refuse(page, name)
            if refusal is not None:
                refused.append(f"{page}.{name}: {refusal}")
                continue
            if request.verb == "clear":
                prepared.append((page, name, None))
                continue
            value_type = declared_attributes(page)[name]
            try:
                prepared.append((page, name, _coerce(value_type, change.value)))
            except ChatHealthyException as exc:
                refused.append(f"{page}.{name}: {exc.message}")
            except Exception as exc:  # noqa: BLE001 - converted at the boundary
                refused.append(f"{page}.{name}: rejected: {exc}")

        if refused:
            # Nothing is written, so every parameter holds the value it held
            # before and nothing has to restore it (S-004-REQ-B-004).
            return Response(
                parameters=params.model_dump(exclude_none=True),
                not_preserved=[f"{p}.{n}" for p, n, _ in
                               [(c.page, c.name, None) for c in changes]],
                error="; ".join(refused))

        determination = "rule" if request.origin == "deterministic" else "model"
        changed: list[str] = []
        for page, name, value in prepared:
            if request.verb == "clear":
                params.clear(page, name)
            else:
                params.set(page, name, ParameterEntry(
                    value=value,
                    route=request.route,
                    determination=determination,
                    carried_from=request.carried_from,
                ))
            changed.append(f"{page}.{name}")

        # The write, once, and to the session itself rather than only to
        # this process's copy. Every server writes the parameters it owns
        # at the moment it owns them: the end-of-turn persist carries no
        # parameter, because a process that wrote its copy back would carry
        # its view of a page over whatever another server wrote meanwhile.
        # Setting the attributes named here touches nothing else in the
        # document, so two servers writing different attributes of the same
        # session in one turn both survive.
        deps.user_object.userParameters = params
        self._persist(deps, prepared, params, request.verb == "clear")
        return Response(parameters=params.model_dump(exclude_none=True),
                        changed=changed)

    def _persist(self, deps: AgentDeps, prepared: list, params, clearing: bool) -> None:
        """Write just the attributes this call changed, to the session.

        Raises, never logs: the caller is the one that knows what the write
        was for, and a parameter that silently failed to persist is a
        parameter the next turn reads as never set.
        """
        from chathealthy_lib.authentication.session_token import SessionToken
        token = deps.session_token
        if not isinstance(token, SessionToken):
            raise ChatHealthyException(
                mode="security_violation",
                component="UserParameters",
                message="a parameter write needs the session it belongs to, "
                        "and this call carries no session token")
        assignments: dict = {}
        removals: dict = {}
        for page, name, _ in prepared:
            address = f"userParameters.pages.{page}.{name}"
            if clearing:
                removals[address] = ""
            else:
                assignments[address] = params.entry(page, name).model_dump(
                    mode="json", exclude_none=True)
        operation: dict = {}
        if assignments:
            operation["$set"] = assignments
        if removals:
            operation["$unset"] = removals
        if not operation:
            return
        coll = deps.mongo_frontend[SESSION_DB][SESSION_COLLECTION]
        coll.update_one({"_id": token.session_guid()}, operation)


TOOL = UserParametersTool()
