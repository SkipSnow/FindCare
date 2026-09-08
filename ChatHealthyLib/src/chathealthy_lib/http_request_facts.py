# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).

"""What an HTTP request carries, as one object, reachable from any line
serving it and passable to the next server.

A request carries three things every component may need: the session token
it arrived with, the name-value pairs it was posted with, and its headers.
None of them should be threaded down as arguments -- a parameter passed
through six frames is a parameter every one of those frames now knows about.

So the server states them once, where the request arrives, and everything
below reads them here. The application calls facts() and knows nothing else:
not the web framework, not where they are held. Both stay ours to change.

They are one pydantic object rather than three values because a request that
crosses to another server carries the same three facts, and an object that
validates is what should cross. The storage is a context variable today
because one request is one asyncio task and a context variable is per task,
so two requests in flight can never see each other's; move to a
worker-per-request server and the storage becomes a thread-local with no
caller changed.

Asking outside a request is an error rather than an empty answer. Code
reaching for the posted body has a body in mind, and handing it nothing
would let it carry on serving a request that does not exist.
"""

from contextvars import ContextVar
from typing import Any, Optional

from pydantic import BaseModel, Field

from .authentication.session_token import SessionToken
from .exceptions import ChatHealthyException

_COMPONENT = "http_request_facts"


class HTTPRequestFacts(BaseModel):
    """The three facts one HTTP request carries."""

    session_token: SessionToken = Field(
        description="The token this request arrived with, already verified "
                    "by the door that stated these facts.")
    posted: dict[str, Any] = Field(
        default_factory=dict,
        description="The name-value pairs the request was posted with.")
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="The request's headers, keyed in lower case because a "
                    "header name is case-insensitive on the wire and a caller "
                    "should not have to guess which case arrived.")

    def session_guid(self) -> str:
        """The session this request belongs to.

        The GUID keys the session document, so a component holding these
        facts can read or write the session without being handed anything.
        """
        return self.session_token.session_guid()


_facts: ContextVar[Optional[HTTPRequestFacts]] = ContextVar(
    "ch_http_request_facts", default=None)


def state_the_facts(*, token: SessionToken, posted: Optional[dict] = None,
                    headers: Optional[dict] = None) -> HTTPRequestFacts:
    """Say what this request carries. A door calls this; nothing else does.

    A component that states its own facts is a component answering for a
    request it did not receive.
    """
    stated = HTTPRequestFacts(
        session_token=token,
        posted=dict(posted or {}),
        headers={str(name).lower(): value
                 for name, value in (headers or {}).items()},
    )
    _facts.set(stated)
    return stated


def facts() -> HTTPRequestFacts:
    """What this request carries."""
    held = _facts.get()
    if held is None:
        raise ChatHealthyException(
            mode="no_request_facts",
            component=_COMPONENT,
            message="no facts stated for this request: either this code is not "
                    "serving one, or the door it came through did not state "
                    "them")
    return held
