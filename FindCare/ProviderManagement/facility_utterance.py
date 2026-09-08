# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# Facility page — utterance mining.
#
# The classifier upstream routes and holds no domain knowledge. This page
# reads the utterance itself and answers with the parameters it needs, so
# adding a page adds a prompt rather than enlarging one shared classifier.
#
# The system prompt and the model are loaded from brain/machine_artifacts/
# content/prompts.json, record `facility_utterance_mining_system_prompt`.

import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from chathealthy_lib.exceptions import ChatHealthyException
from chathealthy_lib.llm import run_llm_sync

# Resolve project root from this file's location:
#   FindCare/ProviderManagement/facility_utterance.py
#   parents: [0]ProviderManagement [1]FindCare [2]<project root>
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROMPTS_JSON_PATH = PROJECT_ROOT / "brain" / "machine_artifacts" / "content" / "prompts.json"

MINING_RECORD_ID = "facility_utterance_mining_system_prompt"

COMPONENT = "FacilityUtterance"


class MinedGeography(BaseModel):
    """Where the person wants the place to be."""
    state: str = ""
    city: str = ""
    county: str = ""
    zip: str = ""


class MinedFacilityParameters(BaseModel):
    """What the facility page needs from an utterance, and nothing else."""
    facility_type: str = ""
    facility_name: str = ""
    geography: MinedGeography = Field(default_factory=MinedGeography)


def _prompt_record(record_id: str) -> dict:
    """The whole record, because the page reads both its prompt and the
    model that prompt was written for."""
    with PROMPTS_JSON_PATH.open(encoding="utf-8") as f:
        d = json.load(f)
    for r in d.get("records", []):
        if r.get("_record_id") == record_id:
            return r
    raise ChatHealthyException(
        mode="key_error",
        component=COMPONENT,
        message=f"prompts.json: no record with _record_id={record_id!r}")


def _model_name(record: dict) -> str:
    models = record.get("models") or []
    if not models or not models[0].get("name"):
        raise ChatHealthyException(
            mode="key_error",
            component=COMPONENT,
            message=f"prompts.json: record {record.get('_record_id')!r} "
                    f"names no model")
    return models[0]["name"]


_AGENT = None


def _mining_agent():
    """One agent, built on first use. What it stops keeping is a client, a
    credential, a retry policy and a vendor quirk — all of that is the
    facade's."""
    global _AGENT
    if _AGENT is None:
        from pydantic_ai import Agent
        record = _prompt_record(MINING_RECORD_ID)
        _AGENT = Agent(
            f"openai:{_model_name(record)}",
            output_type=MinedFacilityParameters,
            system_prompt=record.get("system_prompt", ""),
        )
    return _AGENT


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


def _user_message(utterance: str, history: Optional[list]) -> str:
    prior = _history_lines(history)
    if prior:
        return (f"Conversation before this utterance:\n{prior}\n\n"
                f"Latest utterance:\n{utterance}")
    return f"Latest utterance:\n{utterance}"


def mine_facility_parameters(
        utterance: str,
        history: Optional[list] = None) -> MinedFacilityParameters:
    """The page's own extraction. Raises on any LLM failure — an unmined
    utterance is not a search with no parameters."""
    result = run_llm_sync(
        _mining_agent(), _user_message(utterance, history),
        call_site="FacilityUtterance.mine_facility_parameters",
        provider="openai", server="find_care", component=COMPONENT)
    return result.output
