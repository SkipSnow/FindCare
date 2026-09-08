# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# What every page's mining does the same way.
#
# A page owns its prompt and the shape it answers with; it does not own
# the reading of prompts.json, the rendering of prior dialogue, or the
# building of an agent. Those were one copy per page, and a copy per page
# is a rule that drifts a page at a time.

import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from chathealthy_lib.exceptions import ChatHealthyException
from chathealthy_lib.llm import run_llm_sync

# Resolve project root from this file's location:
#   FindCare/ProviderManagement/utterance_mining.py
#   parents: [0]ProviderManagement [1]FindCare [2]<project root>
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROMPTS_JSON_PATH = PROJECT_ROOT / "brain" / "machine_artifacts" / "content" / "prompts.json"

COMPONENT = "UtteranceMining"


def prompt_record(record_id: str, component: str) -> dict:
    """The whole record, because a page reads both its prompt and the
    model that prompt was written for."""
    with PROMPTS_JSON_PATH.open(encoding="utf-8") as f:
        d = json.load(f)
    for r in d.get("records", []):
        if r.get("_record_id") == record_id:
            return r
    raise ChatHealthyException(
        mode="key_error",
        component=component,
        message=f"prompts.json: no record with _record_id={record_id!r}")


def _model_name(record: dict, component: str) -> str:
    models = record.get("models") or []
    if not models or not models[0].get("name"):
        raise ChatHealthyException(
            mode="key_error",
            component=component,
            message=f"prompts.json: record {record.get('_record_id')!r} "
                    f"names no model")
    return models[0]["name"]


_AGENTS: dict[str, object] = {}


def _mining_agent(record_id: str, output_type: type[BaseModel],
                  component: str):
    """One agent per prompt record, built on first use. What it stops
    keeping is a client, a credential, a retry policy and a vendor quirk —
    all of that is the facade's."""
    if record_id not in _AGENTS:
        from pydantic_ai import Agent
        record = prompt_record(record_id, component)
        _AGENTS[record_id] = Agent(
            f"openai:{_model_name(record, component)}",
            output_type=output_type,
            system_prompt=record.get("system_prompt", ""),
        )
    return _AGENTS[record_id]


def _history_lines(history: Optional[list]) -> str:
    lines = []
    for entry in history or []:
        if isinstance(entry, dict):
            actor = entry.get("actor") or entry.get("role") or ""
            text = entry.get("text") or entry.get("content") or ""
        else:
            actor = ""
            text = entry
        text = str(text).strip()
        if not text:
            continue
        lines.append(f"{actor}: {text}" if actor else text)
    return "\n".join(lines)


def user_message(utterance: str, history: Optional[list]) -> str:
    prior = _history_lines(history)
    if prior:
        return (f"Conversation before this utterance:\n{prior}\n\n"
                f"Latest utterance:\n{utterance}")
    return f"Latest utterance:\n{utterance}"


def mine(record_id: str, output_type: type[BaseModel], utterance: str,
         history: Optional[list], *, component: str, call_site: str):
    """A page's own extraction. Raises on any LLM failure — an unmined
    utterance is not a search with no parameters."""
    result = run_llm_sync(
        _mining_agent(record_id, output_type, component),
        user_message(utterance, history),
        call_site=call_site,
        provider="openai", server="find_care", component=component)
    return result.output


REFINEMENT_PROMPT_RECORD = "page_refinement_request_system_prompt"


class RefinementRequest(BaseModel):
    question: str


def _state_of_the_page(page: str, missing: list[str], optional: list[str],
                       in_force: dict) -> str:
    """What the model is told about where the person stands.

    Every line is read from the declaration and the session, so a parameter
    that becomes required is described to the model without anything here
    naming it.
    """
    held = [f"{name} = {value!r}"
            for name, value in sorted((in_force or {}).items())
            if value not in (None, "", [], {})]
    return (
        f"Page: {page}\n"
        f"Required and missing: {', '.join(missing) or 'none'}\n"
        f"Optional on this page: {', '.join(optional) or 'none'}\n"
        f"Already in force: {'; '.join(held) or 'nothing'}")


def ask_for_missing(page: str, missing: list[str], optional: list[str],
                    in_force: dict, utterance: str, history: Optional[list],
                    *, component: str, call_site: str) -> str:
    """The question a page asks when what it requires is not in force.

    The page, what it requires, what is merely allowed and what the person
    has already said all reach the model as facts, so making an attribute
    required in the declaration is enough to have it asked for. Nothing
    here knows what any of them mean.
    """
    result = run_llm_sync(
        _mining_agent(REFINEMENT_PROMPT_RECORD, RefinementRequest, component),
        f"{_state_of_the_page(page, missing, optional, in_force)}\n\n"
        f"{user_message(utterance, history)}",
        call_site=call_site,
        provider="openai", server="find_care", component=component)
    return result.output.question
