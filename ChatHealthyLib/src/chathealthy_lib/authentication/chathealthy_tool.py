# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""ChatHealthyTool — abstract base for every tool in the application.

Sits between pydantic-ai's Agent and our concrete tools. The base class
holds abstract functions only; it does NOT pin a runtime, a deps type,
or any shared state. Concrete tools:

  * Declare TOOL_NAME (str), Request (pydantic BaseModel), Response
    (pydantic BaseModel) as class attributes.
  * Override `run(deps, request) -> Response` as an async method.

Some concrete tools wrap a pydantic-ai Agent internally (the LLM-driven
ones, e.g. SpecialtyFilterTool). Others are pure-Python (e.g.
ProviderSearchAndSelectionTool — straight DB query). Both expose the
same `run()` surface so the orchestrator dispatches identically.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar, Type

from pydantic import BaseModel

from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException

log = ChatHealthyLoggingService()


class ChatHealthyTool(ABC):
    TOOL_NAME: ClassVar[str]
    Request:   ClassVar[Type[BaseModel]]
    Response:  ClassVar[Type[BaseModel]]

    @abstractmethod
    async def run(self, deps, request):
        """Compute the response from deps + request. Mutate
        `deps.user_object` in place when the tool's work produces session
        state changes; the gate persists the user_object back to
        Users.sessions at the end of the run. Tools do NOT log their own
        invocation — the base class does that uniformly in run_and_log().
        """
        raise NotImplementedError

    async def run_and_log(self, deps, request):
        """Single uniform entry point for tools that are dispatched after
        AuthN. Runs the tool, then appends a tool_invocation entry to the
        user_object's session_conversation_history. Concrete tools never
        have to remember to log themselves — the base class does it.

        Bootstrap tools (e.g. AuthN) whose deps do NOT yet carry a
        user_object should be invoked via `run()` directly, not this
        method.

        A tool that raises is recorded before the exception continues on
        its way. The run used to be outside every guard, so a tool that
        raised appended nothing at all and the action log simply stopped
        -- the one entry that would say why is the one that was never
        written.
        """
        from chathealthy_lib.authentication.agent_deps import append_action
        try:
            args_dump = (
                request.model_dump(exclude_none=True) if request is not None else {}
            )
        except Exception as _exc:
            # Mode 1 (REQ-B-008): args dump for action log only; defaults
            # to {}; action append still happens. log.info + default debug.
            log.info("tool args model_dump failed (using {}): %s", _exc, exc=ChatHealthyException(
                                                                             mode="tool_args_dump_failed",
                                                                             message=f"tool args model_dump failed (using {{}}): {_exc}",
                                                                             component="ChatHealthyTool",
                                                                             exception=_exc,
                                                                         ))
            args_dump = {}

        try:
            result = await self.run(deps, request)
        except BaseException as exc:
            failure: dict = {
                "exception": type(exc).__name__,
                "message": str(exc)[:1000],
            }
            if isinstance(exc, ChatHealthyException):
                failure["mode"] = exc.mode
                failure["component"] = exc.component
                if exc.context:
                    failure["context"] = exc.context
            append_action(
                deps.user_object,
                tool_name=self.TOOL_NAME,
                input_json=args_dump,
                output_json=failure,
            )
            raise

        try:
            result_dump = (
                result.model_dump(exclude_none=True) if result is not None else {}
            )
        except Exception as _exc:
            # Mode 1 (REQ-B-008): result dump for action log only; defaults
            # to {}; action append still happens. log.info + default debug.
            log.info("tool result model_dump failed (using {}): %s", _exc, exc=ChatHealthyException(
                                                                               mode="tool_result_dump_failed",
                                                                               message=f"tool result model_dump failed (using {{}}): {_exc}",
                                                                               component="ChatHealthyTool",
                                                                               exception=_exc,
                                                                           ))
            result_dump = {}
        append_action(
            deps.user_object,
            tool_name=self.TOOL_NAME,
            input_json=args_dump,
            output_json=result_dump,
        )
        return result

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        for attr in ("TOOL_NAME", "Request", "Response"):
            if not getattr(cls, attr, None):
                raise ChatHealthyException(
                    mode="chathealthy_tool_subclass_missing_attribute",
                    message=(
                        f"ChatHealthyTool subclass {cls.__name__} missing "
                        f"required class attribute {attr!r}"
                    ),
                    component="ChatHealthyTool",
                )
