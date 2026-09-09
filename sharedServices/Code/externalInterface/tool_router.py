# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# tool_router.py — Safe tool dispatch replacing globals().get() (F-05 fix).
#
# Explicit allowlist registry. If a tool name is not in TOOL_REGISTRY,
# it cannot be called — period. This is the server-side enforcement
# layer for GOV-004 ("The model may suggest. The system must decide.")
#
# Design: ARCH-001 Phase 1

import json
from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException

log = ChatHealthyLoggingService()


class ToolRouter:
    """Pydantic-validated allowlist tool dispatch.

    Phase 6: validates inputs via Pydantic models before calling handlers.
    GOV-004: "The model may suggest. The system must decide."
    """

    def __init__(self):
        self._registry: dict[str, callable] = {}
        self._models: dict[str, type] = {}  # tool_name -> Pydantic model class

    def register(self, tool_name: str, handler: callable, model: type = None) -> None:
        """Register a tool handler with optional Pydantic input model."""
        self._registry[tool_name] = handler
        if model:
            self._models[tool_name] = model
        log.debug("Registered tool: %s -> %s (model: %s)", tool_name, handler.__name__ if hasattr(handler, '__name__') else str(handler), model.__name__ if model else "None")

    def register_with_models(self, mapping: list[tuple]) -> None:
        """Register tools with Pydantic models: [(name, handler, model), ...]"""
        for entry in mapping:
            name, handler = entry[0], entry[1]
            model = entry[2] if len(entry) > 2 else None
            self.register(name, handler, model)

    @property
    def registered_tools(self) -> list[str]:
        """List of all registered tool names."""
        return list(self._registry.keys())

    def dispatch(self, tool_name: str, arguments: dict) -> dict:
        """Dispatch a tool call with optional Pydantic validation.

        F-05: rejects unregistered tools.
        Phase 6: validates inputs via Pydantic if model registered.
        """
        if tool_name not in self._registry:
            log.warning("BLOCKED: unregistered tool '%s' — not in allowlist", tool_name)
            return {"error": f"Tool '{tool_name}' is not registered. This call has been blocked."}

        # Phase 6: Pydantic validation
        model = self._models.get(tool_name)
        if model:
            try:
                validated = model(**arguments)
                arguments = validated.model_dump()
            except Exception as exc:
                # Mode 2 (REQ-B-008): the LLM produced tool arguments that
                # didn't match the Pydantic schema; user gets a graceful
                # error dict. LLM/schema drift — operator MUST know so the
                # prompt or model can be re-tuned.
                log.error("VALIDATION FAILED for '%s': %s", tool_name, exc, exc=ChatHealthyException(
                                                                               mode="tool_input_validation_failed",
                                                                               message=f"VALIDATION FAILED for {tool_name!r}: {exc}",
                                                                               component="ToolRouter",
                                                                               exception=exc,
                                                                           ), if_not_debug_log=True)
                return {"error": f"Tool '{tool_name}' input validation failed: {str(exc)}"}

        handler = self._registry[tool_name]
        log.info("Dispatching tool: %s (validated: %s)", tool_name, bool(model))
        try:
            return handler(**arguments)
        except Exception as exc:
            # Mode 2 (REQ-B-008): a tool handler raised an unhandled
            # library exception that didn't translate to ChatHealthyException
            # locally; we return graceful error dict to caller (no 503)
            # but operator MUST know — this catch site exists exactly so
            # the tool doesn't bubble unhandled exceptions to the catch-all.
            log.error("Tool '%s' failed: %s", tool_name, exc, exc=ChatHealthyException(
                                                                              mode="tool_handler_failed",
                                                                              message=f"Tool {tool_name!r} failed: {exc}",
                                                                              component="ToolRouter",
                                                                              exception=exc,
                                                                          ), if_not_debug_log=True)
            return {"error": f"Tool '{tool_name}' failed: {str(exc)}"}

    def handle_tool_calls(self, tool_use_blocks, messages, format_history_fn=None) -> list:
        """Process Anthropic tool_use blocks. Drop-in replacement for _handle_tool_calls.

        Args:
            tool_use_blocks: list of tool_use blocks from Anthropic response
            messages: conversation messages (for chat_history injection)
            format_history_fn: optional function to format chat history
        """
        tool_results = []
        for block in tool_use_blocks:
            name = block.name
            arguments = dict(block.input)

            result = self.dispatch(name, arguments)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": json.dumps(result),
            })
        return tool_results

    def handle_normalized_tool_calls(self, tool_calls: list, messages, format_history_fn=None) -> list:
        """Process normalized tool calls from llm_client (OpenAI format).

        Args:
            tool_calls: list of {"id": "...", "function": {"name": "...", "arguments": "..."}}
            messages: conversation messages
            format_history_fn: optional function to format chat history

        Returns:
            list of {"role": "tool", "tool_call_id": "...", "content": "..."}
        """
        tool_results = []
        for tc in tool_calls:
            name = tc["function"]["name"]
            args_str = tc["function"].get("arguments", "{}")
            arguments = json.loads(args_str) if isinstance(args_str, str) else args_str

            result = self.dispatch(name, arguments)
            tool_results.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": json.dumps(result),
            })
        return tool_results
