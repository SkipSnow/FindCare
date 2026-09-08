# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""UtteranceManager — the classifier ChatHealthyTool.

Reads up to the last 10 USER utterances off deps.user_object, calls an LLM
to classify the latest utterance in context, builds the canonical
IntentDocument (including any pending_disambiguation marker and the
LLM-authored top-level user_message), streams the user_message if
present, writes the document back to deps.user_object.intent, and
returns.

Per https://dev.chathealthy.ai/schemas/ChatHealthyUtteranceManagerOutputSchema.json
"""
from __future__ import annotations

import asyncio
import json
from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field
from pydantic_ai import Agent, ModelRetry

from chathealthy_lib.authentication.agent_deps import AgentDeps
from chathealthy_lib.authentication.chathealthy_tool import ChatHealthyTool
from chathealthy_lib import run_llm

from chathealthy_lib.authentication.intent_document import (
    Argument,
    IntentCloseConnection200,
    IntentDocument,
    IntentFindAFacility,
    IntentFindAProvider,
    IntentFindClinicalTrials,
    IntentSafetyLockout,
    IntentSpecialtySearch,
    PendingDisambiguation,
)

log = ChatHealthyLoggingService()

# The classifier runs on every turn: fast and cheap is the right trade.
LLM_MODEL = "google:gemini-2.5-flash"

# Manufacture runs only when the system cannot proceed -- rare, and the
# turn is already lost if the question is a bad one. It reasons over the
# whole session to work out what is established and what is missing, which
# is not what a fast classifier model is for.
REASONING_LLM_MODEL = "anthropic:claude-opus-5"

MAX_USER_UTTERANCE_WINDOW = 10  # EPIC-002-F-010-S-001-REQ-B-006


# ────────────────────────────────────────────────────────────────────
# Classifier structured output
# ────────────────────────────────────────────────────────────────────


class GeoFacts(BaseModel):
    state: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    zip: Optional[str] = None


class PendingDisambiguationOut(BaseModel):
    """LLM-emitted pending-disambiguation marker. Mirrors the canonical
    PendingDisambiguation shape on the IntentDocument."""
    kind: str
    candidate: dict[str, Any] = Field(default_factory=dict)


class Correction(BaseModel):
    """One spelling substitution applied by rule 1.5. The classifier emits
    these as a structured list so the frontend can apply red styling to the
    original word without parsing prose for parentheses (which would false-
    positive on legitimate LLM-authored parens). EPIC-002-F-010-S-001-REQ-B-011."""
    original: str
    corrected: str


class ClassifierOutput(BaseModel):
    """Structured output of the UM classifier LLM. Field names match the
    canonical IntentDocument so downstream Python can copy them through
    with minimal translation."""

    target_action: Literal[
        "specialtySearch", "findAProvider", "findAFacility",
        "findClinicalTrials", "closeConnection200", "safetyLockout",
    ]
    complaint: Optional[str] = None
    geography: Optional[GeoFacts] = None
    user_location: Optional[str] = None
    user_message: Optional[str] = None
    pending_disambiguation: Optional[PendingDisambiguationOut] = None
    corrections: list[Correction] = Field(default_factory=list)
    # findClinicalTrials refinement fields — extracted by the LLM per the
    # prompt rules in section 5b (family/relational words, pronouns, etc.).
    # All optional; condition (carried as `complaint`) is the only required
    # field for findClinicalTrials.
    age_years: Optional[int] = None
    sex: Optional[Literal["m", "f"]] = None
    geographic_scope: Optional[Literal["international", "us"]] = None
    # findAProvider narrowings the person stated in words. Distinct from
    # `sex` above, which is the trial participant's, not the provider's.
    provider_last_name: Optional[str] = None
    provider_first_name: Optional[str] = None
    provider_middle_name: Optional[str] = None
    provider_sex: Optional[Literal["F", "M", "X", "U"]] = None
    insurance: Optional[str] = None
    # The person's own phrase for what they were looking for -- "bone
    # docs", "shrinks". Not the complaint, which is by definition the
    # translation and never the words they used, and not the NUCC display
    # name. The summary must say this back to them
    # (EPIC-006-F-001-S-005-REQ-B-001). Interpreting what a person said is
    # this component's single concern, and this is one more thing said.
    search_phrase: Optional[str] = None
    # findAFacility. A facility is named outright, or by the person who
    # administers it, and either alone is enough to search on. The
    # administrator's name is decomposed into the same structured shape a
    # care giver's name takes, because the person may give one name or
    # several, in either order, and is not required to say which is which
    # (EPIC-006-F-006-S-001-REQ-B-008).
    facility_name: Optional[str] = None
    # The KIND of place, in the person's own words -- "urgent care
    # clinic". Never the complaint: a complaint is what is wrong with a
    # person and a facility kind is what sort of place they want, and the
    # two are different parameters on different pages.
    facility_type: Optional[str] = None
    administrator_last_name: Optional[str] = None
    administrator_first_name: Optional[str] = None
    administrator_middle_name: Optional[str] = None
    sole_proprietor: Optional[bool] = None
    # Whether this turn narrows the search already running rather than
    # asking a new question. Asked outright because inferring it from an
    # absent complaint does not work: told to leave the field null the
    # model writes "general health care", then "Medicare-covered health
    # services" -- it will not decline to fill a field, but it will
    # answer a question.
    narrows_current_search: Optional[bool] = None
    # Narrowings this turn REMOVES. A field left absent means unchanged,
    # because a stated preference keeps applying, so without this there
    # is no way to say a filter should stop applying and every narrowing
    # is one-way once spoken.
    cleared_narrowings: Optional[list[str]] = None
    # Audit-only label carried on the safetyLockout intent. LockoutTool
    # writes it onto {env}_Safety.emergency_incidents but renders the
    # user-facing prose from the trigger utterance, not from this label.
    lockout_reason: Optional[str] = None


# ────────────────────────────────────────────────────────────────────
# Embedded canonical schema (kept in sync with Website/schemas/…)
# ────────────────────────────────────────────────────────────────────

CANONICAL_INTENT_DOCUMENT_SCHEMA = r"""{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://dev.chathealthy.ai/schemas/ChatHealthyUtteranceManagerOutputSchema.json",
  "title": "ChatHealthy UtteranceManager Output",
  "description": "Structured output of the UtteranceManager (UM) classifier used by the ChatHealthy Universal Router (UR) to call the correct tool to do work on behalf of an end user. UM examines the user's latest utterance with others as necessary, and the prior intent document carried on user_object.intent.

PERSISTENCE: this document lives on the session as `user_object.intent`. UM's first action on every invocation is to read user_object.intent and construct the Pydantic object from it; if the field is empty (first turn of the session), UM constructs a fresh document and assigns it to user_object.intent. UM updates the document over the course of its classification work and writes the updated version back to user_object.intent BEFORE returning control to UR. The document survives across turns — UM accumulates intents and arguments turn-by-turn rather than starting fresh each time.

DIVISION OF RESPONSIBILITY:
  * UM (producing side) — decides which tool the turn is for and emits the intent entry for it, carrying whatever the person said. UM does NOT judge whether the tool has enough to work with: what a tool requires is declared where that tool lives, the tool enforces it against its own declaration, and the tool asks the person for what it lacks. Routing is UM's whole job.
  * UR (dispatch) — UR checks that target_action is in the catalog and names an entry in intents[], and dispatches it. UR does NOT judge sufficiency either: a tool that cannot run says so and asks, which is an answer to the person rather than an exception in a log.
  * Each tool (consuming side) — receives the document (or its intent's slice), reads the arguments it cares about, applies any tool-specific business rules, executes its work. Tools add their own defensive asserts without relying on UR having pre-validated.

STREAMING CONTRACT: every dispatched tool flushes the stream (awaits at least one event-loop tick after its last deps.stream(...) call) before returning control to UR, so when closeConnection200 is the next dispatch all bytes prior tools wrote are on the wire before the StreamingResponse terminates.

MULTI-INTENT: intents[] can carry more than one entry. UM may have detected findAProvider on turn 1 (geography missing) and still be tracking that intent on turn 2 when the user provides the missing geography. UM may also have detected a secondary intent (e.g., a safety concern) in the same utterance. target_action is always the single intent UR actions this turn; the other intents remain in the document for future turns.

CATALOG (this deploy's closed set): specialtySearch, findAProvider, findAFacility, findClinicalTrials, closeConnection200, safetyLockout. UM clamps to one of these four; UR raises on any out-of-catalog target_action. safetyLockout is special — UR may dispatch it WITHOUT calling UM whenever user_object.is_locked_out is true (UR hydrates that flag from {env}_Safety.emergency_incidents by IP at /gate entry); UM only emits target_action=safetyLockout on a turn where the user is NOT yet locked out and the classifier judges the utterance signals immediate medical attention.",
  "type": "object",
  "additionalProperties": false,
  "required": ["target_action", "intents"],
  "properties": {
    "target_action": {
      "description": "The single action UR must dispatch next. UM picks one of the catalog values; UR's match/case is keyed off this field. MUST correspond to a name in intents[] — UM enforces that invariant before emitting. NOTE: when user_object.is_locked_out is true (UR stamps it from {env}_Safety.emergency_incidents during hydration), UR dispatches safetyLockout directly without calling UM at all; UM's only role for safetyLockout is to recognise immediate-medical-attention utterances on turns where the user is NOT yet locked out and emit target_action=safetyLockout so UR locks them.",
      "enum": ["specialtySearch", "findAProvider", "findAFacility", "findClinicalTrials", "closeConnection200", "safetyLockout"]
    },
    "intents": {
      "type": "array",
      "description": "All intents UM has recognized for this document across turns. Each entry is one of the typed intent shapes defined in $defs. uniqueItems is enforced on the (name) field via the per-shape const — two entries with the same name MUST NOT exist in this array.",
      "minItems": 1,
      "maxItems": 4,
      "uniqueItems": true,
      "items": {
        "oneOf": [
          { "$ref": "#/$defs/IntentSpecialtySearch" },
          { "$ref": "#/$defs/IntentFindAProvider" },
          { "$ref": "#/$defs/IntentCloseConnection200" },
          { "$ref": "#/$defs/IntentSafetyLockout" }
        ]
      }
    },
    "user_message": {
      "type": "string",
      "maxLength": 4096,
      "description": "LLM-authored prose intended for the user (clarification question, follow-up, friendly framing). When non-empty UM streams it as {kind:'prompt', data:{text: user_message}} and awaits an event-loop tick to flush before returning to UR. When absent or empty UM streams nothing. The LLM owns this prose; no hardcoded chat strings appear in UM, CloseConnection200Tool, or any other component on the dispatch path."
    }
  },
  "$defs": {
    "Argument": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "value", "type", "required"],
      "description": "Canonical argument shape. Every argument across every intent uses this same four-field shape so the router (and any introspection tool) can walk arguments uniformly. value is always carried as a string; the consumer parses it according to type. Code guards in UM (emit-side) and in each consuming tool (receive-side) enforce semantic constraints the JSON Schema cannot reach.",
      "properties": {
        "name": {
          "type": "string",
          "description": "Argument name. Per-intent the set of valid names is enumerated by that intent's argument oneOf. MUST be one of the names the intent declares; UM-side and tool-side code guards enforce.",
          "minLength": 1,
          "maxLength": 64,
          "pattern": "^[a-z][a-z0-9_]{0,63}$"
        },
        "value": {
          "type": "string",
          "description": "The argument's value, serialized as a string. For type='boolean', exactly 'true' or 'false'. For type='integer'/'number', the decimal string with no leading zeros or whitespace. For type='object'/'array', a JSON-encoded string the consumer parses with json.loads. For type='string', the raw text. MUST be non-empty when the argument is required: true.",
          "minLength": 0,
          "maxLength": 32768
        },
        "type": {
          "description": "Names the JSON-native type that value, once parsed, will produce. Constrained to the JSON Schema 2020-12 primitive type set.",
          "enum": ["string", "boolean", "integer", "number", "object", "array"]
        },
        "required": {
          "type": "boolean",
          "description": "Whether the dispatched tool treats this argument as required. It is a note about the tool, not an instruction to you: emit the action the turn is for and carry whatever the person gave, even when an argument marked required is empty. The tool holds its own declaration, refuses on its own terms, and asks the person for what it needs."
        }
      }
    },
    "PendingDisambiguation": {
      "type": "object",
      "additionalProperties": false,
      "required": ["kind", "candidate"],
      "description": "Marker carried on an intent entry when the classifier could not fully fill a required slot but has a plausible candidate value for it. UM sets it; UR does not dispatch the intent's action while pending_disambiguation is set. UM clears it on a subsequent turn when the user resolves the disambiguation (yes/no answer), at which point the slot is filled and target_action is upgraded.",
      "properties": {
        "kind": {
          "type": "string",
          "minLength": 1,
          "maxLength": 64,
          "description": "The disambiguation category. FindCare's geography slot uses 'geography_state'."
        },
        "candidate": {
          "type": "object",
          "description": "Structured candidate value the classifier proposed (e.g., {\"state\":\"WI\"})."
        },
        "scaffolding": {
          "type": "object",
          "description": "Optional free-form context the next-turn resolver needs."
        }
      }
    },
    "IntentSpecialtySearch": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "arguments"],
      "description": "Intent entry for an utterance where UM has extracted a complaint phrase but no usable geography. UR dispatches to SpecialtyFilter, which translates the complaint into NUCC specialty codes; the FE renders the specialty list so the user can see candidate provider types even before they tell us where they are. specialtySearch is the partial-information counterpart to findAProvider; once the user supplies geography on a subsequent turn UM upgrades the target_action to findAProvider. The optional nucc_codes argument carries the SpecialtyFilter output and persists across turns so UR can skip re-running SpecialtyFilter when the user resolves a pending disambiguation.",
      "properties": {
        "name": {
          "const": "specialtySearch"
        },
        "pending_disambiguation": { "$ref": "#/$defs/PendingDisambiguation" },
        "arguments": {
          "type": "array",
          "description": "One required argument (complaint) and one optional argument (nucc_codes) carrying the cached SpecialtyFilter output.",
          "minItems": 1,
          "maxItems": 2,
          "uniqueItems": true,
          "items": {
            "oneOf": [
              {
                "allOf": [
                  { "$ref": "#/$defs/Argument" },
                  { "properties": { "name": { "const": "complaint" }, "type": { "const": "string" }, "required": { "const": true }, "value": { "minLength": 1, "maxLength": 1024 } } }
                ],
                "description": "Natural-language complaint phrase UM extracted (e.g. 'back pain'). SpecialtyFilter translates this into NUCC codes via its own LLM call."
              },
              {
                "allOf": [
                  { "$ref": "#/$defs/Argument" },
                  { "properties": { "name": { "const": "nucc_codes" }, "type": { "const": "array" }, "required": { "const": false }, "value": { "minLength": 2, "maxLength": 4096 } } }
                ],
                "description": "Cached SpecialtyFilter output. value is a JSON-encoded array of {code, name, score, ...} objects. UR writes this after SpecialtyFilter runs the first time and reads it on follow-up turns to avoid re-running SpecialtyFilter on the same complaint."
              }
            ]
          }
        }
      }
    },
    "IntentFindAProvider": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "arguments"],
      "description": "Intent entry for an utterance UM classified as the person looking for a care giver. Emit it whenever that is what the turn is about, however much or little of a place they named: what a search needs is the care-giver page's to declare and to ask about. pending_disambiguation may be set alongside it when a place name was given without a state and there is a plausible candidate to propose. The optional nucc_codes argument carries the SpecialtyFilter output across turns.",
      "properties": {
        "name": {
          "const": "findAProvider"
        },
        "pending_disambiguation": { "$ref": "#/$defs/PendingDisambiguation" },
        "arguments": {
          "type": "array",
          "description": "Up to three arguments: complaint (required), geography (required when dispatched as findAProvider; may be partial while pending_disambiguation is set), and an optional nucc_codes carrying the cached SpecialtyFilter output.",
          "minItems": 1,
          "maxItems": 3,
          "uniqueItems": true,
          "items": {
            "oneOf": [
              {
                "allOf": [
                  { "$ref": "#/$defs/Argument" },
                  {
                    "properties": {
                      "name": { "const": "complaint" },
                      "type": { "const": "string" },
                      "required": { "const": true },
                      "value": { "minLength": 1, "maxLength": 1024 }
                    }
                  }
                ],
                "description": "Natural-language complaint phrase UM extracted (e.g. 'back pain', 'persistent cough'). SpecialtyFilter downstream translates this into NUCC codes via its own LLM call. value is the phrase as the user expressed it (or as UM paraphrased it from the prior conversation history)."
              },
              {
                "allOf": [
                  { "$ref": "#/$defs/Argument" },
                  {
                    "properties": {
                      "name": { "const": "geography" },
                      "type": { "const": "object" },
                      "required": { "const": true },
                      "value": { "minLength": 2, "maxLength": 512 }
                    }
                  }
                ],
                "description": "Structured location facts. value is a JSON-encoded object the consumer parses with json.loads. Carry whatever the person named -- a state, a city, a ZIP, a county, or only one of them. There is no minimum to meet here: which of these a search cannot run without is declared where the search lives, and that page asks the person for it."
              },
              {
                "allOf": [
                  { "$ref": "#/$defs/Argument" },
                  {
                    "properties": {
                      "name": { "const": "nucc_codes" },
                      "type": { "const": "array" },
                      "required": { "const": false },
                      "value": { "minLength": 2, "maxLength": 4096 }
                    }
                  }
                ],
                "description": "Cached SpecialtyFilter output. value is a JSON-encoded array of {code, name, score, ...} objects. UR writes this after SpecialtyFilter runs the first time and reads it on follow-up turns to avoid re-running SpecialtyFilter on the same complaint."
              }
            ]
          }
        }
      }
    },
    "IntentSafetyLockout": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "arguments"],
      "description": "Intent entry for the safety-lockout action. UM emits this on a turn where the LLM classifier determines the user's utterance signals immediate medical attention (the prompt asks the LLM to make that determination 100%; no hardcoded keyword match outside the prompt). UR also dispatches LockoutTool when user_object.is_locked_out is already true (UR's pre-UM hydration stamped the flag from {env}_Safety.emergency_incidents); in that path UM is NOT called, so this intent entry only appears on the locking-now turn. LockoutTool reads the trigger utterance from user_object.session_conversation_history (the latest person utterance) and the IP from user_object.ip_address; no required arguments on this intent.",
      "properties": {
        "name": {
          "const": "safetyLockout"
        },
        "pending_disambiguation": { "$ref": "#/$defs/PendingDisambiguation" },
        "arguments": {
          "type": "array",
          "description": "Exactly one argument: lockout_reason (string). UM provides a short LLM-authored category label (e.g. 'cardiac symptoms', 'severe trauma', 'overdose') purely for the audit record; the user-facing 'why' is rendered by LockoutTool from the verbatim trigger utterance, not from this label.",
          "minItems": 1,
          "maxItems": 1,
          "uniqueItems": true,
          "items": {
            "allOf": [
              { "$ref": "#/$defs/Argument" },
              {
                "properties": {
                  "name": { "const": "lockout_reason" },
                  "type": { "const": "string" },
                  "required": { "const": true },
                  "value": { "minLength": 1, "maxLength": 256 }
                }
              }
            ],
            "description": "Audit-only label for the safety classification. LockoutTool stores it on the {env}_Safety.emergency_incidents record alongside the trigger_message but does NOT include it in user-facing prose."
          }
        }
      }
    },
    "IntentCloseConnection200": {
      "type": "object",
      "additionalProperties": false,
      "required": ["name", "arguments"],
      "description": "Intent entry for the close-with-200 action UR dispatches. UM authors the user-facing prose (top-level user_message) on any turn that needs to talk to the user; UR streams it and is responsible for closing the connection. The closeConnection200 tool has no knowledge of what was said to the user or what the user said to elicit the response — its only job is to close the StreamingResponse with HTTP 200 OK.",
      "properties": {
        "name": {
          "const": "closeConnection200"
        },
        "pending_disambiguation": { "$ref": "#/$defs/PendingDisambiguation" },
        "arguments": {
          "type": "array",
          "description": "Exactly one argument: close_connection (boolean, always true).",
          "minItems": 1,
          "maxItems": 1,
          "uniqueItems": true,
          "items": {
            "allOf": [
              { "$ref": "#/$defs/Argument" },
              {
                "properties": {
                  "name": { "const": "close_connection" },
                  "type": { "const": "boolean" },
                  "required": { "const": true },
                  "value": { "const": "true" }
                }
              }
            ],
            "description": "Explicit confirmation boolean. value MUST be the string 'true' (parsed as boolean true) on every closeConnection200 intent entry. The closing tool asserts on this before terminating the StreamingResponse with HTTP 200 OK."
          }
        }
      }
    }
  }
}
"""


CLASSIFIER_SYSTEM_PROMPT = """\
You are the utterance classifier for ChatHealthy.ai (the UtteranceManager,
"UM" in the schema below). Your job: examine the user's recent utterance
window AND the prior IntentDocument carried on user_object.intent, and
return a structured output that captures (a) the next target_action the
Universal Router (UR) must dispatch, (b) any per-intent
pending_disambiguation marker, and (c) the top-level user_message you
want streamed to the user before UR dispatches the action. Downstream
Python translates your output into the canonical IntentDocument.

WHERE YOU SIT IN THE PROCESS:
  - The user types an utterance into the ChatHealthy.ai client.
  - The client POSTs the utterance to SharedServices /gate.
  - /gate authenticates the session, appends the new utterance to
    user_object.session_conversation_history.utterances, and hands the
    user_object to the Universal Router (UR).
  - UR dispatches you (UM) FIRST. You receive up to the last 10 dialogue
    lines (person AND system, interleaved, oldest-first) AND the prior
    IntentDocument off user_object.intent. Each line is prefixed with
    "user: " or "system: " — the system lines are prior user_message
    prose YOU wrote to the user on earlier turns.
  - You return your structured output.
  - Python translates your output into the canonical IntentDocument and
    writes it back onto user_object.intent.
  - If user_message is non-empty in your output, UM streams it as
    {kind:"prompt", data:{text: user_message}} and flushes before
    returning to UR. THIS IS THE ONLY PROSE THE USER SEES FROM
    THIS TURN. No other component streams chat text on this turn.
  - UR then reads user_object.intent.target_action and dispatches the
    matching downstream tool: SpecialtyFilter alone (specialtySearch),
    SpecialtyFilter+ProviderSearch (findAProvider), or
    CloseConnection200Tool.

RULE 0 — UNIVERSAL UTTERANCE PRECEDENCE (highest priority; applies to
every other rule below). The transcript window is ordered oldest-first
and the LATEST 'user:' line is the current turn. When facts in the
current turn CONTRADICT facts from earlier turns or from the prior
IntentDocument — different condition, different location, different
age, different sex, different scope, different intent of any kind —
the CURRENT TURN ALWAYS WINS. Drop the contradicted older fact entirely
and use the current value. This applies to condition, geography, age,
sex, geographic_scope, complaint, target_action, AND any other slot.
"Contradiction" includes outright replacement ('actually, the condition
is asthma'), retraction ('never mind the trial, find me a doctor'),
correction of a misheard value, OR a NEW utterance whose meaning
implies a different filled slot. Older utterances may only CONTRIBUTE
context; they may never OVERRIDE a slot the current utterance fills.

READ THE SCHEMA'S COMMENTS. Every description field in the schema below
carries the WHY behind a rule. Read them and let them guide your output.

Canonical IntentDocument schema (read every description, not just the
normative type/enum bits):

""" + CANONICAL_INTENT_DOCUMENT_SCHEMA + """

Your structured output is a JSON object with these fields:

{
  "target_action": "specialtySearch" | "findAProvider" | "closeConnection200",
  "complaint": string | null,
  "geography": { "state": string | null, "city": string | null, "county": string | null, "zip": string | null } | null,
  "user_message": string | null,
  "pending_disambiguation": { "kind": string, "candidate": object } | null,
  "narrows_current_search": boolean | null,
  "cleared_narrowings": [string] | null
}

How the Python translator uses each field:
  - target_action: copied to IntentDocument.target_action.
  - complaint: becomes the complaint Argument on IntentSpecialtySearch
    and/or IntentFindAProvider. Required whenever THIS TURN names what
    the person is looking for (specialtySearch, findAProvider, or
    ambiguous-but-resolvable holding pattern).
    NULL when the turn names none. A turn that only narrows a search
    already running -- "just find me the medicare docs", "now the male
    ones", "only solo practitioners" -- names no complaint, and the one
    already in force still stands. NEVER invent a placeholder such as
    "general health care" or restate the kind of provider the previous
    turn established. Writing anything there tells the system the person
    asked a NEW question: the specialty list is resolved again from the
    words of the narrowing, the panel the person is looking at is
    replaced, and the narrowing is applied to a different set of
    providers than the one on their screen -- so asking for the male
    ones among 5 returned 11.
  - geography: becomes the geography Argument on IntentFindAProvider
    (JSON-encoded). May be partial (e.g., {"city":"milwaukee"} alone)
    when pending_disambiguation is set. Must be sufficient (zip, state,
    state+city, or state+county) when target_action is findAProvider.
    geography.county carries the NAME ONLY. The field is already named
    county, so the word is never part of its value, and neither is any
    other kind-word the place may be called by -- parish, borough,
    municipio, district, region, or city in the states that have
    independent ones. "Los Angeles County" -> "Los Angeles".
    "Orleans Parish" -> "Orleans". "in LA county" -> "Los Angeles".
    Write the name the person meant and nothing else; the search resolves
    it to whatever form the records hold.
  - narrows_current_search: true when this turn narrows the search
    already running rather than asking a new question. Judge it by what
    the person is looking for, not by their wording:
      "just find me the medicare docs"  -> true
      "now the male ones"               -> true
      "only solo practitioners"         -> true
      "actually make it Sacramento"     -> true  (same providers, new place)
      "find me a dentist instead"       -> false (different providers)
      "what about clinical trials"      -> false
    When true the specialty list already on screen is kept and the
    narrowing is applied to it. When false it is resolved afresh.
    Getting this wrong is expensive in one direction: a false 'false'
    replaces the panel the person is reading and applies their narrowing
    to a different set of providers, so asking for the male ones among 5
    came back 11.
  - user_message: streamed to the user verbatim as {kind:"prompt"}.
    The LLM owns this prose; the runtime never substitutes hardcoded
    chat strings. Set it on any turn that needs to talk to the user.
  - pending_disambiguation: set when you cannot fully fill a required
    slot but have a plausible candidate. Holds {kind, candidate}.
    For FindCare's geography slot use kind="geography_state" and
    candidate={"state":"WI"} (or similar). Persists across turns until
    cleared.

DECISION RULES (apply in this order):

  0. SAFETY LOCKOUT TAKES PRECEDENCE OVER EVERY OTHER RULE, INCLUDING
     PENDING DISAMBIGUATION. Before anything else, judge whether the
     latest user utterance signals that the user (or someone they are
     speaking for) needs IMMEDIATE MEDICAL ATTENTION right now. The
     classifier — not a keyword list — owns this determination. Make it
     yourself with care; the only hardcoded match in the runtime is the
     operator backdoor literal in LockoutTool, and that's intentionally
     not your concern.

     A signal of immediate medical attention typically has all three of:
       - a specific body location or named body system (chest, head,
         airway, etc.) OR a specific named acute condition (overdose,
         seizure, stroke symptoms, severe bleeding, anaphylaxis, etc.),
       - acute onset or severity stated or strongly implied,
       - a life-threat or limb-threat or sensorium-threat implication.
     Casual phrasing like "I want a chest pain doctor" or "my back
     hurts, can I find a chiro" or "I need someone for my anxiety" is
     NOT immediate medical attention — those are provider-search
     intents. Statements like "I'm having crushing chest pain right
     now and can't breathe" or "I think I'm having a stroke" or "I
     took too many pills" or "I want to hurt myself tonight" ARE.

     Example keyword categories you MAY consult for context but MUST
     NOT match on alone: cardiac (chest crushing, can't breathe,
     numb arm), cerebrovascular (face drooping, slurred speech, sudden
     weakness), trauma (gunshot, bleeding heavily, unconscious),
     toxicological (overdose, took too many, swallowed bleach),
     self-harm (hurt myself, end my life, suicide). The presence of
     one of these words does NOT mean the utterance is an emergency;
     YOU decide based on the full sentence and its tone.

     When the determination is yes:
       - Set target_action to "safetyLockout".
       - Add (or replace) the corresponding intent entry in intents[]
         with name="safetyLockout" and a single lockout_reason
         argument (short audit-label string — e.g. "cardiac symptoms",
         "self-harm statement", "trauma"; max 256 chars). lockout_reason
         is for the {env}_Safety.emergency_incidents audit record, NOT
         for the user-facing prose.
       - Leave user_message empty; LockoutTool will author the
         deterministic "when you said '...'" + 911 + operator phone
         text from the trigger utterance directly.
       - Do NOT also emit specialtySearch / findAProvider on the same
         turn — safetyLockout supersedes them.

  1. PENDING DISAMBIGUATION TAKES PRECEDENCE OVER EVERY OTHER RULE.
     Before considering anything else, check the prior IntentDocument
     summary. If it lists a pending_disambiguation, the latest user
     utterance MUST be interpreted as a yes/no/answer to that pending
     question. The prior system: line in the transcript is the question
     you (UM) asked the user on the previous turn — the user's latest
     line is the answer.

       - Affirmative ("yes", "yeah", "yep", "y", "sure", "ok", "right",
         "correct", "uh-huh", "confirmed", any equivalent): fill the
         candidate value into geography (or whichever slot the
         pending_disambiguation names) and upgrade target_action to
         the now-fully-specified action (e.g., findAProvider). DO NOT
         re-emit pending_disambiguation. user_message MAY be a brief
         acknowledgement or null.

       - Negative ("no", "nope", "nah", "n", "negative", "wrong", any
         equivalent): the candidate was wrong. Leave target_action at
         the partial-information action; emit a follow-up user_message
         asking a different way (e.g., "Which state did you mean?");
         pending_disambiguation MAY be re-emitted with a refined
         candidate or left null if you cannot guess.

       - Direct answer (e.g., user replies with the actual missing
         value: "Wisconsin" / "WI" / "michigan"): fill that value
         into the slot, upgrade target_action, do not re-emit pending.

     A "yes" utterance on a turn with a pending disambiguation is
     ALWAYS routable. The pending question gives the "yes" its full
     meaning.

  1.5. SPELLING CORRECTION. User utterances often contain typos —
     especially in city names, complaint terms, and specialty names.
     A USER UTTERANCE MUST NEVER PROPAGATE AS A FATAL ERROR. If a
     misspelling leaves you unable to extract any meaningful slot,
     route to rule 2 — never let downstream code raise on it.

     SPELLING ONLY — NEVER TRANSLATE SEMANTICS. This rule corrects
     misspellings, not word choice. A word that is correctly spelled
     in any register of English MUST NOT be "corrected" to a synonym,
     a more formal term, a clinical term, or any other re-wording.
     Slang, colloquial, lay, and informal terms are correctly spelled
     and MUST pass through verbatim — they are NOT misspellings.

     A misspelling is a word that the user TRIED to spell but got
     wrong (one or two character edits away from a real word, or an
     obviously phonetic mis-rendering). Examples and counter-examples:

       MISSPELLINGS (correct these):
         - "wilington"   -> "Wilmington"   (one missing letter)
         - "winington"   -> "Wilmington"   (transposition)
         - "willingtun"  -> "Wilmington"   (phonetic)
         - "psyciatrist" -> "psychiatrist" (missing 'h')
         - "chest pian"  -> "chest pain"   (transposition)

       NOT MISSPELLINGS — leave verbatim, NEVER substitute:
         - "shrink"      stays "shrink"   (slang for psychiatrist)
         - "doc"         stays "doc"      (informal for doctor)
         - "OB"          stays "OB"       (abbreviation)
         - "ENT"         stays "ENT"      (abbreviation)
         - "foot doctor" stays "foot doctor" (lay term for podiatrist)
         - "eye doctor"  stays "eye doctor"  (lay term for optometrist/ophthalmologist)
         - "head shrinker" stays "head shrinker" (slang)
         - "gyno"        stays "gyno"     (informal)
         - "PT"          stays "PT"       (abbreviation)
         - "back pain"   stays "back pain" (already a real phrase)

     Translation from lay/slang/abbreviated terms to clinical or NUCC
     terminology happens DOWNSTREAM in the SpecialtyFilter pipeline,
     NOT in the Utterance Manager. Your job is to faithfully extract
     what the user typed, fixing only typos.

     For each word in the utterance that you suspect is a TYPO
     (NOT a slang/lay term to translate) of a healthcare condition,
     body part, US city, US state, US county, or NUCC specialty:

       - If you are >=75% confident of the intended word, substitute
         the corrected form in the output fields (complaint,
         geography.city, geography.state, geography.county). Proceed
         with decision rules 2-6 below as if the user had typed it
         correctly.

         CRITICAL — STRUCTURED FIELDS ARE NON-OPTIONAL FOR PLACE-NAME
         CORRECTIONS. If the corrected word is a city, state, or
         county name, you MUST also populate the corresponding
         structured field on the geography object on your output:
           - corrected city  -> geography.city  = <corrected city>
           - corrected state -> geography.state = <2-letter USPS code>
           - corrected county -> geography.county = <corrected county
             NAME ONLY, no County/Parish/Municipio/Borough kind-word>
         The user_message + corrections[] annotation alone is NOT
         enough. The geography object MUST carry the corrected fact
         in its dedicated slot — downstream code reads geography, not
         user_message, to decide whether to dispatch findAProvider.

         If you cannot populate the geography field for any reason
         (e.g., you corrected a city but can't decide the state, or
         the utterance still leaves geography ambiguous), you MUST
         fall through to Rule 4 or Rule 5 below (specialtySearch +
         user_message asking for the missing piece). NEVER emit
         target_action=findAProvider unless the geography object is
         fully populated to the sufficiency rule (zip alone, state
         alone, state+city, or state+county).

         IN ADDITION you MUST do TWO things on the same turn so the
         user sees that their spelling was interpreted, not ignored:

           (a) Append one entry per correction to the corrections[]
               list on your output: {"original": <user's word>,
               "corrected": <your corrected word>}. The original
               value is verbatim from the user's utterance. Multiple
               corrections in one utterance produce multiple entries.
               The frontend uses this structured list to apply red
               visual highlighting to the original word — never trust
               text-parsing on the user_message prose to find what to
               highlight; the corrections[] list is the source of
               truth.

           (b) Set user_message to the user's full corrected
               utterance, with the original misspelled token written
               inline after each corrected word as
                   <corrected_word> (corrected from '<original_word>')
               using straight single quotes around the original. For
               example, the user's "find me a shrink in wilington DE"
               becomes user_message =
                   "find me a shrink in Wilmington (corrected from 'wilington') DE"
               Multiple corrections appear in their original order.
               The two outputs MUST be consistent: every entry in
               corrections[] MUST appear in user_message in the
               "(corrected from '<original>')" form, and every such
               form in user_message MUST have a matching entry in
               corrections[].

       - If you are <75% confident, do NOT guess. Set target_action to
         "closeConnection200". Set user_message to a brief
         clarification question naming up to three plausible candidates
         (e.g., "Did you mean Wilmington, Williston, or Williamsburg?").
         Do not populate complaint or geography on this turn. Do not
         set pending_disambiguation. Leave corrections[] empty — no
         substitution was applied.

     Examples of >=75% confident corrections (each produces both a
     corrections[] entry AND a user_message acknowledgment):
       - "wilington DE"   -> "Wilmington DE"
       - "winington DE"   -> "Wilmington DE"
       - "shink"          -> "shrink"
       - "chest pian"     -> "chest pain"
       - "psyciatrist"    -> "psychiatrist"

     Examples where confidence is <75% and you must ask:
       - "Springfeld" alone (no state context) — many US cities named
         Springfield-like; ask for the state or the intended city.
       - "phsyological"   - could be "psychological" or "physiological".

  2. IF YOU CANNOT ROUTE THE UTTERANCE TO A TOOL, ASK FOR
     CLARIFICATION. Every other rule below names a target_action and
     the tool that serves it. If the latest utterance, evaluated
     ALONE, does not give you what one of those rules needs, set
     target_action to "closeConnection200" and set user_message to a
     brief, friendly request for what is missing. Say what you need,
     not that you failed. Never guess, and never let prior context
     supply a meaning the utterance does not have.

  2.5. A PLACE, NOT A PERSON. Decide first whether the person is asking
     for somewhere care is delivered rather than someone who delivers
     it. A facility is a legal entity -- a hospital, clinic, pharmacy,
     dialysis centre, laboratory, nursing home, imaging centre, urgent
     care, surgery centre, hospice, home health agency. A care giver is
     a person: a doctor, nurse, therapist, dentist, specialist.

     When the request is for a place, set target_action to
     "findAFacility". Any ONE of the three ways of naming it is enough
     and none of them is required alongside another:
       - facility_name: the organization named outright ("Cedars-Sinai",
         "Rite Aid"). Give it exactly as the person said it.
       - facility_type: the KIND of place, in the person's own words
         ("hospital", "urgent care clinic", "pharmacy"). This is NOT a
         complaint. A complaint is what is wrong with a person; a
         facility kind is what sort of place they want. On a
         findAFacility turn leave complaint NULL -- always, even when the
         person named a kind of place. Putting a place into complaint
         writes a building into the parameter that means an ailment.
       - administrator_last_name / _first_name / _middle_name: the
         person who administers it, when the person names a human being
         as the one who runs the place ("the clinic Susan Wey runs").
         Decompose whatever they gave into these three; if you cannot
         tell which part is which, put the single name you were given in
         administrator_last_name.
     ALWAYS populate geography on a findAFacility turn, by the same
     reading rules 3-5 apply to a care giver. If THIS turn names a place,
     put that place in geography -- it REPLACES whatever was established
     before, and a turn that names a new city is naming a different
     place. If this turn names none, repeat the established geography.
     Returning null for geography on a facility turn is always wrong: it
     leaves the search running on wherever the person was looking last,
     which is not what they asked for.

     A facility search runs with geography alone and needs no complaint,
     so do NOT downgrade to specialtySearch when a place is named
     without a kind.

     "Find me a hospital in Long Beach CA" -> findAFacility,
     facility_type "hospital", complaint null, geography
     {state: CA, city: Long Beach}.
     "Find me a doctor in Long Beach CA" -> findAProvider, because a
     doctor is a person.

  3. If the person is looking for a care giver, set target_action to
     "findAProvider" and populate complaint and whatever geography they
     gave. Do this whether or not the place is complete. How much of a
     place is enough is not yours to judge: the care-giver page holds
     that rule, asks the person for what it lacks, and records what they
     said so their next answer resolves it.

     Judging it here sent an incomplete place to specialtySearch
     instead, and that page owns no location -- so nothing recorded the
     city the person named, nothing searched when they supplied the
     state, and "yes" had nothing to attach itself to.

  4. If a place name is given without a state (e.g., "milwaukee" alone),
     still set target_action to "findAProvider" and populate the city.
     Set pending_disambiguation = {"kind":"geography_state",
     "candidate":{"state": YOUR-BEST-GUESS}} and propose the candidate
     in user_message (e.g., "Did you mean Milwaukee, Wisconsin?").

  5. Use "specialtySearch" only when the person is asking what kinds of
     care giver exist rather than for a care giver -- "what sort of
     doctor treats this?". A request for care with no place named is
     still a request for care: target_action is "findAProvider", and
     the page asks where.

  5b. CLINICAL TRIALS. If the user is asking for clinical trials,
      research studies, "trials", "studies", "experimental treatment",
      or anything that names a condition and asks about trial/study
      participation, handle as follows. user_location is OPTIONAL on
      this action — we'll proceed without it, but if supplied we
      compute travel time and distance to each trial site.

      The participant's age and sex are OPTIONAL inputs that refine
      the CT.gov search when present. geographic_scope (international
      vs US) is the fourth optional refinement. Encourage the user to
      supply them. The condition is the only thing the search needs,
      and nothing waits on a refinement.

        - The four refinements are age, sex, location and scope.
          None is required and none is asked for. Gender is not a
          refinement in this flow; sex is.

          On the FIRST turn where the person expresses
          clinical-trial interest, set
          target_action="findClinicalTrials" and route. Extract
          whatever they gave -- age, sex, location, scope -- from the
          current utterance or the prior IntentDocument, including
          what is implied by family words ("son" -> sex=m, age from
          "9 years old"), pronouns ("she has migraines" -> sex=f),
          or place and scope phrases ("Las Vegas NV", "US-based
          trial").

          Do not stop to collect any of them. The condition is the
          only thing the trials page requires; those four are
          refinements it uses when it has them and searches
          perfectly well without. Scope left unset means every trial
          worldwide, which is the widest answer and the safest one
          to give somebody who did not say.

          Interrogating the person for four optional facts before
          searching made them answer a questionnaire to see results
          that the condition alone would have produced.

          RULE 1.5 STILL APPLIES TO THIS user_message AND TO
          corrections[]. If the user's condition contained a
          misspelling you corrected, <condition> in the template
          above MUST be written as
              <corrected_condition> (corrected from '<original>')
          using straight single quotes, AND the corrections[] list on
          your output MUST carry the same {original, corrected}
          entry. The frontend renders both the ask-first turn and the
          results turn through the same correction-display path; the
          marker convention does not change because the action is
          closeConnection200 on this turn.

        - On the FOLLOW-UP turn after that prompt: capture EVERY
          refinement the user supplied. You MUST extract:
            * age_years (integer) — any of "I'm 28", "I am 28
              years old", "28", "age 28", "28yo", etc. → 28
            * sex (lowercase single character, "m" or "f") — you
              MUST resolve the user's natural-language answer to
              one of {"m", "f"}. Examples (this list is illustrative;
              apply the same reasoning to other phrasings):
                - direct labels: "Male", "M", "boy", "man",
                  "I'm a guy", "masculino" → "m"; "Female", "F",
                  "girl", "woman", "I'm a woman" → "f"
                - male family / relationship words (when the
                  utterance is about THAT person): "son",
                  "brother", "father", "dad", "uncle", "nephew",
                  "grandfather", "grandpa", "husband", "boyfriend",
                  "fiancé" → "m"
                - female family / relationship words: "daughter",
                  "sister", "mother", "mom", "aunt", "niece",
                  "grandmother", "grandma", "wife", "girlfriend",
                  "fiancée" → "f"
                - third-person pronouns about the subject when
                  no contradicting label is present: "he", "his",
                  "him" → "m"; "she", "her", "hers" → "f"
              If the answer is ambiguous, non-binary, or the
              utterance mixes signals you cannot reconcile, leave
              sex unset (the search dispatches without a sex
              filter). Gender is NOT extracted in this flow.
            * user_location (free-text string) — any location ("Los
              Angeles", "30605", "near Boston", etc.).
            * geographic_scope (one of "international" or "us") —
              resolve the user's answer: any of "international",
              "worldwide", "all", "everywhere", "anywhere" → "international";
              any of "US", "USA", "United States", "domestic",
              "US only", "United States only", "American" → "us".
              If the answer is unclear or the user picks a single
              non-US country, leave geographic_scope unset and the
              system defaults to international.
          Populate ALL extracted fields as Arguments on the
          findClinicalTrials intent. If the user declines ("skip",
          "no", "no thanks", "just show me"), proceed with whatever
          fields are populated. Always populate complaint with the
          original condition and set
          target_action="findClinicalTrials".

      Set complaint to the medical condition the user is asking
      about, normalized (e.g. "type 2 diabetes", "lung cancer",
      "depression").

      If the user described a symptom rather than a clinical
      diagnosis (e.g. "chronic headaches", "back pain",
      "shortness of breath", "frequent dizziness"), expand the
      complaint to the specific clinical condition(s) the
      symptom most commonly maps to. Join multiple alternatives
      with " OR " inside a single complaint string. Examples
      (the list is illustrative; apply the same reasoning to
      other symptoms):
          "chronic headaches"   -> "chronic migraine OR tension-type headache OR cluster headache"
          "back pain"           -> "lumbar disc herniation OR lumbar radiculopathy OR sciatica OR chronic low back pain"
          "shortness of breath" -> "asthma OR COPD OR pulmonary fibrosis OR heart failure"
          "frequent dizziness"  -> "vertigo OR Meniere's disease OR vestibular neuronitis OR BPPV"
      Layperson symptom strings yield zero or weakly-relevant
      results downstream; specific clinical condition names
      yield usable ones. If the user already named a specific
      clinical condition (e.g. "type 2 diabetes", "non-small
      cell lung cancer"), use it verbatim without OR-expansion.

      Condition is the ONLY required field for
      findClinicalTrials; age, sex, user_location, and
      geographic_scope are all optional refinements per the rules
      above.

  5.5. NARROWINGS THE PERSON STATED, on a findAProvider or
     specialtySearch turn. Extract only what they actually said; leave
     every one absent otherwise. None of these changes target_action --
     they ride alongside it.

     A turn that states ONLY a narrowing sets its narrowing field and
     leaves complaint NULL. Narrowings accumulate: each one stated is
     added to those already in force, and one is dropped only when the
     person says to drop it. "just find me the medicare docs" then "now
     the male docs" means male AND medicare, not male instead of
     medicare.

     cleared_narrowings names the narrowings this turn REMOVES, by
     field name: provider_sex, insurance, sole_proprietor,
     provider_name. Asking for everything is a removal, not a value:
     "include both males and females" -> ["provider_sex"];
     "any insurance is fine" -> ["insurance"]; "forget the name" ->
     ["provider_name"]; "show me everyone again" -> every narrowing
     currently in force. A field named here carries no value.

     provider_last_name / provider_first_name / provider_middle_name --
       a provider named outright: "I'm looking for Dr. Sarah Chen",
       "is James P Smith taking patients". Split the name; do not guess
       parts they did not say.

     provider_sex -- a stated preference about the provider:
       "a female doctor" -> F ; "I'd prefer a man" -> M.
       X and U are what a provider said about themselves: X is a
       provider who stated they are neither male nor female, U is one
       who declined to state. Emit either ONLY when the person asks for
       that specific group. Never emit them to mean "no preference".
       Wanting every sex is not a value: "include both males and
       females", "any gender", "I don't mind" name provider_sex in
       cleared_narrowings and set no value.

     insurance -- the payer they carry: "I have Anthem" -> ANTHEM,
       "I'm on Medicaid" -> MEDICAID, "Blue Cross" ->
       BLUE_CROSS_BLUE_SHIELD. Upper case, underscores for spaces.
       A question ABOUT insurance is not a statement of which they have:
       "do they take my insurance?" sets nothing.

     sole_proprietor -- true when they want someone practising on their
       own account: "a solo practitioner", "not part of a big group".

  6. There is no other outcome. Every utterance either gives a rule
     above what it needs, or it does not and rule 2 applies. Why it
     does not is not yours to judge.

State codes are 2-letter USPS uppercase. ZIP codes are 5 digits.
"""


# ────────────────────────────────────────────────────────────────────
# Request / Response
# ────────────────────────────────────────────────────────────────────


class Request(BaseModel):
    """UM accepts two trigger types per EPIC-002-F-010-S-003-REQ-B-001.

    trigger_type='utterance' (default): UM reads the latest person
    utterance off deps.user_object.session_conversation_history and runs
    the classifier prompt. This is the interpret path.

    trigger_type='manufacture': UR has dispatched UM with a
    manufacture_utterance_reason dict of structured facts. UM does NOT
    read free-text; it runs the manufacture prompt with the reason +
    prior dialogue and authors a brief user-facing user_message.
    target_action is always closeConnection200 on this path
    (EPIC-002-F-010-S-003-REQ-B-002).
    """
    model_config = {"extra": "ignore"}
    trigger_type: Literal["utterance", "manufacture"] = Field(
        default="utterance",
        description="utterance: the person typed something and it is to be "
                    "classified. manufacture: nobody typed anything and a "
                    "message has to be authored to ask them for what is "
                    "missing.")
    manufacture_utterance_reason: dict[str, Any] = Field(
        default_factory=dict,
        description="Manufacture only: which path could not proceed. Internal "
                    "plumbing, never shown to the person.")


class Response(BaseModel):
    """UM writes its IntentDocument to deps.user_object.intent. The Response
    just carries the target_action for the router's convenience."""
    target_action: str


# ────────────────────────────────────────────────────────────────────
# Utterance window (EPIC-002-F-010-S-001-REQ-B-006)
# ────────────────────────────────────────────────────────────────────


def recent_transcript(deps: AgentDeps, max_count: int = MAX_USER_UTTERANCE_WINDOW) -> list[str]:
    """Return the most recent up-to-max_count dialogue lines (person AND
    system), oldest first, each rendered as 'user: <text>' or
    'system: <text>'. The narrative form lets the LLM resolve follow-up
    turns like 'yes' against the system's prior proposal."""
    out: list[str] = []
    utterances = deps.user_object.session_conversation_history.utterances
    for u in reversed(utterances):
        actor = getattr(u, "actor", None) or (u.get("actor") if isinstance(u, dict) else None)
        text = getattr(u, "text", None) or (u.get("text") if isinstance(u, dict) else "")
        if actor not in ("person", "system") or not text:
            continue
        label = "user" if actor == "person" else "system"
        out.append(f"{label}: {text}")
        if len(out) >= max_count:
            break
    return list(reversed(out))


# ────────────────────────────────────────────────────────────────────
# Classifier call
# ────────────────────────────────────────────────────────────────────


classifier_agent = Agent(
    LLM_MODEL,
    output_type=ClassifierOutput,
    system_prompt=CLASSIFIER_SYSTEM_PROMPT,
    # The classifier had none, so it took the default of one attempt. The
    # our other agents carry 3 and 5.
    retries=3,
)


@classifier_agent.output_validator
def _provider_search_has_a_place(output: ClassifierOutput) -> ClassifierOutput:
    """A provider search with no geography at all is an illegal state.

    The schema cannot say this: every geography field is optional, because
    a user names a state, or a city and a state, or a zip alone, and no
    single field is required. All of them empty is different -- it is not a
    sparse answer, it is no answer, and there is no provider search that
    means anything without a place.

    Left to validation alone the model returns it perfectly well-formed:
    on identical input it reported the place five times in six and dropped
    it the sixth. Nothing was malformed, so `retries` had nothing to act
    on. Raising here is what turns an illegal state into a validation
    failure, which is the thing retries count.

    The message says what to do, not that it failed: a retry that only
    reports the error invites the same answer back.
    """
    if output.target_action != "findAProvider":
        return output
    geo = output.geography
    if geo is not None and any(
        (value or "").strip() for value in
        (geo.state, geo.city, geo.county, geo.zip)
    ):
        return output
    # The thrower logs here, against Rule-065 statement 4's general shape,
    # because the catcher is pydantic-ai and it will never log to ours. Left
    # silent, a rule that fired one turn in six left no trace at all -- and
    # would leave none if it started firing every turn.
    log.info("LLM validator REJECT rule=provider_search_has_a_place "
             "target_action=%s geography=%s",
             output.target_action,
             geo.model_dump(exclude_none=True) if geo is not None else None)
    raise ModelRetry(
        "You set target_action=findAProvider but every geography field is "
        "empty. A provider search always has a place: the user named one in "
        "this turn, or one is listed as already established. Put it in the "
        "geography field. If there is genuinely no place anywhere, the "
        "action is specialtySearch, not findAProvider."
    )


def summarize_prior(prior: Optional[IntentDocument]) -> str:
    if prior is None:
        return "(no prior turns)"
    parts = [
        f"Prior target_action was {prior.target_action!r}.",
        f"Prior intents tracked: {[i.name for i in prior.intents]}.",
    ]
    if prior.user_message:
        parts.append(f"Prior user_message was: {prior.user_message!r}.")
    # Surface every prior intent's Arguments so the next-turn LLM can see
    # what slots have already been filled (age, sex, geography,
    # geographic_scope, complaint, etc.) without re-asking. Per RULE 0 the
    # current turn may still override any of these on contradiction.
    for entry in prior.intents:
        args = getattr(entry, "arguments", None) or []
        arg_pairs = []
        for a in args:
            name = getattr(a, "name", None) or (a.get("name") if isinstance(a, dict) else None)
            value = getattr(a, "value", None) or (a.get("value") if isinstance(a, dict) else None)
            if name and value not in (None, "", []):
                arg_pairs.append(f"{name}={value!r}")
        if arg_pairs:
            parts.append(f"Prior {entry.name} arguments: {', '.join(arg_pairs)}.")
        pd = getattr(entry, "pending_disambiguation", None)
        if pd is not None:
            parts.append(
                f"Prior pending_disambiguation on {entry.name}: "
                f"kind={pd.kind!r} candidate={pd.candidate!r}."
            )
    return " ".join(parts)


MANUFACTURE_SYSTEM_PROMPT = """You are writing one message to a person
who is looking for healthcare and has got stuck.

You are given this session as JSON: the parameters it has established,
the intent document as it stands, the dialogue so far, and the internal
record of why you were called. Read the JSON. It is the state, not a
description of it.

Produce the ONE question that moves this forward.

  - Say back what we already have. It is in the parameters. A person
    who is told what we understood knows we were listening, and knows
    what is left to answer.
  - Ask for what is missing. One thing -- whichever is stopping the
    intent document's target_action from being carried out.
  - NEVER ask for something the parameters already hold. That is the
    worst thing this message can do.
  - The newest turns of the dialogue outweigh the older ones. Where
    they disagree, the newest wins. Older turns tell you what has
    already been asked, so you do not ask it twice.
  - If the record says the system itself is unavailable, there is
    nothing to ask for: say we cannot answer right now and ask them to
    try again shortly.

Never say we failed, never blame what they typed, and never call
anything nonsense or unintelligible. We did not understand -- that is a
fact about us, not about them.

Never quote the JSON, its keys, its ids, or any internal name.

Never write a phrase that would fit any session, like "Please provide
your location." If it could have been written before reading the JSON,
it is wrong.

At most 150 words and at most 6 sentences. Usually far less."""


class ManufactureOutput(BaseModel):
    """Structured output of the UM manufacture-path LLM. Narrower than
    the classifier output — no target_action choice (always
    closeConnection200), no intent classification, no geography
    extraction. Just the LLM-authored user-facing user_message.

    900 characters is the hard stop behind the prompt's 150-word, six-
    sentence limit -- a cap the schema can express, where a word count is
    not. The prompt is what actually holds the length; this stops a
    runaway."""
    user_message: str = Field(min_length=1, max_length=900)


manufacture_agent = Agent(
    REASONING_LLM_MODEL,
    output_type=ManufactureOutput,
    system_prompt=MANUFACTURE_SYSTEM_PROMPT,
)


async def call_manufacture_llm(
    deps,
    transcript: list[str],
    reason: dict[str, Any],
    prior: Optional[IntentDocument],
    parameters=None,
) -> ManufactureOutput:
    """Manufacture-path LLM call.

    Receives the session -- the dialogue, what has been established, and
    what the system was about to do -- and reasons its way to the question
    worth asking. The reason dict says which gesture called it; it is not
    the content of the question.
    """
    window_block = (
        "\n".join(f"  {i+1}. {line}" for i, line in enumerate(transcript))
        if transcript else "  (no prior dialogue yet)"
    )
    reason_block = json.dumps(reason, ensure_ascii=False, indent=2)
    prior_summary = summarize_prior(prior)
    user_msg = (
        f"THE DIALOGUE (oldest first, {len(transcript)} lines):\n"
        f"{window_block}\n\n"
        f"WHAT THE SYSTEM CAN DO:\n"
        f"{tool_contracts_block()}\n\n"
        f"WHAT IS ESTABLISHED so far this session:\n"
        f"{session_state_block(parameters)}\n\n"
        f"WHAT THE SYSTEM WAS ABOUT TO DO: {prior_summary}\n\n"
        f"WHY YOU WERE CALLED (internal plumbing; never quote it):\n"
        f"{reason_block}\n\n"
        "Reason over all of the above, then ask the one question that "
        "moves this forward. Return the structured output."
    )
    log.debug("UM manufacture input: %s", user_msg)
    result = await _run_agent(
        deps, manufacture_agent, user_msg, call_site="UM._manufacture_agent")
    log.debug("UM manufacture output: %s", result.output.model_dump_json())
    return result.output


def session_guid(deps) -> str:
    """The correlation id for everything one turn does.

    Every line a model call writes carries it, so the state going in and
    the answer coming out can be put back together afterwards -- and so a
    retry inside pydantic-ai's own frame, which nothing of ours catches,
    can still be tied to the turn it happened on.
    """
    if deps is None:
        return "no-session"
    try:
        return deps.session_token.get_auth_token()
    except ChatHealthyException:
        return "no-session"


async def _run_agent(deps, agent, prompt: str, *, call_site: str):
    """One route to this tool's agents, carrying the identity every call
    reports under.

    Logs the session going in and the answer coming out, both stamped with
    the session guid. A model call was previously invisible: a turn that
    took three attempts returned the same object as one that took one, and
    nothing recorded what the model was given or what it said.

    No catch here. From agent.run forward a failure is chathealthy_lib.llm's
    to convert, and it does -- a run that fails arrives as a
    ChatHealthyException carrying a mode, the exchange, and the original.
    Catching it again to re-wrap it would be a second conversion of the
    same failure and would bury the mode the library already decided.

    An output validator rejecting an illegal answer never reaches here at
    all: ModelRetry is pydantic-ai's callback protocol, caught in its own
    frame and turned into another turn at the model. That is why the
    validator logs for itself -- there is no catcher of ours to do it.
    """
    guid = session_guid(deps)
    session = (deps.user_object.model_dump_json(exclude_none=True)
               if deps is not None and deps.user_object is not None else "{}")
    log.info("LLM call BEGIN guid=%s call_site=%s session=%s prompt=%s",
             guid, call_site, session, prompt)
    result = await run_llm(
        agent, prompt,
        call_site=call_site,
        provider="gemini",
        server="shared_services",
        component="UM",
    )
    log.info("LLM call END guid=%s call_site=%s output=%s",
             guid, call_site, result.output.model_dump_json())
    return result


async def _structure_location(text: str) -> dict:
    """Turn a free-text place into the geography parameter's shape.

    The trials path reports a place as prose because that is what its own
    tool consumes. The parameter is structured, so it is structured here
    rather than stored as a second shape of the same fact.

    Never fatal: a place that cannot be structured leaves geography as it
    was, which is the same outcome as a turn that named no place.
    """
    try:
        from chathealthy_lib.geo_extractor import extract_location
        located = await asyncio.to_thread(extract_location, text)
        return located.model_dump(exclude_none=True)
    except Exception as exc:
        log.info("could not structure %r as geography: %s", text, exc,
                 exc=ChatHealthyException(
                     mode="geography_not_structured",
                     message=f"could not structure {text!r} as geography: {exc}",
                     component="UtteranceManager",
                     exception=exc if isinstance(exc, Exception) else None,
                 ))
        return {}


def tool_contracts_block() -> str:
    """What the system can be asked to do, and what comes back.

    Generated into the build from the source it was made from, so it names
    the tools this deploy carries. Absent outside a build, and a prompt
    that cannot say what is reachable says so rather than guessing.
    """
    try:
        from tool_registry import ToolRegistry
    except ImportError:
        return "  (the tool registry was not generated into this build)"
    return (
        "Each block below is one thing the system can do. WHAT EACH NEEDS "
        "lists what must be known before it can run -- a fact missing there "
        "is the thing to ask for. WHAT EACH RETURNS is what comes back, so "
        "you can tell whether an answer is even available.\n\n"
        f"WHAT EACH NEEDS:\n{ToolRegistry.jsons_in()}\n\n"
        f"WHAT EACH RETURNS:\n{ToolRegistry.jsons_out()}"
    )


def session_state_block(parameters) -> str:
    """Everything established this session, and what each part means.

    The classifier gets geography alone. Asking a good question needs the
    whole set: what the user is looking for, where, which specialties they
    kept, where they are in the list. That is the difference between
    apologising and asking for the one thing missing.
    """
    if parameters is None:
        return "  (nothing established yet)"

    lines = []

    complaint = parameters.get("NUCC", "complaint")
    if complaint:
        lines.append(f"  They are looking for care for: {complaint}")
    else:
        lines.append("  We do NOT know what kind of care they want.")

    geo = _geography_of(parameters, "individualProvider")
    if geo is not None and not geo.is_empty():
        where = ", ".join(v for v in (geo.city, geo.county, geo.state, geo.zip) if v)
        lines.append(f"  They are looking in: {where}")
    else:
        lines.append("  We do NOT know where they are looking.")

    offered = len(parameters.get("NUCC", "offeredSpecialties") or [])
    if offered:
        kept = len(parameters.get("NUCC", "selectedSpecialtyCodes") or [])
        lines.append(
            f"  We showed them {offered} kinds of provider and they kept "
            f"{kept if kept else 'all of them'}.")

    if parameters.has("individualProvider", "position"):
        lines.append("  They have already paged past the first set of results.")

    if parameters.has("individualProvider", "openNpi"):
        lines.append("  They are reading one provider's details right now.")

    return "\n".join(lines)


def _geography_of(parameters, page: str):
    """The geography in force on one page, as its model."""
    from chathealthy_lib.authentication.user_parameters import Geography
    if not parameters:
        return None
    # Four declared parameters, one shape. Only the state is required, so
    # they are declared and written separately and assembled where read.
    parts = {part: parameters.get(page, part) or ""
             for part in ("state", "city", "county", "zip")}
    if not any(parts.values()):
        return None
    return Geography(**parts)


def _known_parameters_block(parameters) -> str:
    """What the user has already established, for the classifier to read.

    Without this the classifier decides from the utterance alone, so "find
    me a psychiatrist" after the user has already said New York looks like
    a question with no place in it and is classified as a specialty search
    rather than a provider search. The place is known; only the classifier
    did not know it.
    """
    if parameters is None:
        return ""
    geo = _geography_of(parameters, "individualProvider")
    if geo is None or geo.is_empty():
        return ""
    parts = [f"{name}={value}" for name, value
             in geo.model_dump(exclude_none=True).items() if value]
    if not parts:
        return ""
    return (
        f"Geography the user has ALREADY established this session: "
        f"{', '.join(parts)}. Treat it as if the user had just said it: a "
        f"request for providers is not missing a location, and you MUST NOT "
        f"ask for a location the user has already given. Repeat it in the "
        f"geography field unless this turn names a different place.\n\n"
    )


async def call_classifier_llm(
    deps, transcript: list[str], prior: Optional[IntentDocument],
    parameters=None,
) -> ClassifierOutput:
    """Single LLM call. Receives the recent transcript as already-labeled
    lines (each prefixed with 'user: ' or 'system: '), the prior
    IntentDocument summary, and the user's live parameters. Classifies the
    latest 'user: ...' line."""
    if not transcript:
        raise ChatHealthyException(
            mode="um_empty_transcript_window",
            message="UtteranceManager: empty transcript window",
            component="UtteranceManager",
        )
    window_block = "\n".join(f"  {i+1}. {line}" for i, line in enumerate(transcript))
    prior_summary = summarize_prior(prior)
    user_msg = (
        f"Recent dialogue (last {len(transcript)} lines, oldest first):\n"
        f"{window_block}\n\n"
        f"Prior IntentDocument summary: {prior_summary}\n\n"
        f"{_known_parameters_block(parameters)}"
        "Classify the LATEST 'user:' line (the most recent person turn) "
        "using the decision rules in the system prompt. If the prior "
        "IntentDocument summary lists a pending_disambiguation, Rule 1 "
        "applies and overrides every other rule. Return the structured "
        "output."
    )
    result = await _run_agent(
        deps, classifier_agent, user_msg, call_site="UM._classifier_agent")
    return result.output


# ────────────────────────────────────────────────────────────────────
# Intent builders
# ────────────────────────────────────────────────────────────────────


def existing_intent(document: IntentDocument, name: str) -> Optional[Any]:
    return next((i for i in document.intents if i.name == name), None)


def cached_nucc_codes(entry: Any) -> Optional[str]:
    """Return the JSON-encoded nucc_codes argument value from an intent
    entry if present, else None."""
    if entry is None:
        return None
    for arg in entry.arguments:
        if arg.name == "nucc_codes":
            return arg.value
    return None


def build_specialty_search_intent(
    complaint: str,
    nucc_codes_json: Optional[str] = None,
) -> IntentSpecialtySearch:
    args = [Argument(name="complaint", value=complaint, type="string", required=True)]
    if nucc_codes_json:
        args.append(Argument(
            name="nucc_codes", value=nucc_codes_json, type="array", required=False,
        ))
    return IntentSpecialtySearch(name="specialtySearch", arguments=args)


def build_find_a_provider_intent(
    complaint: str,
    geography: dict[str, Any],
    nucc_codes_json: Optional[str] = None,
    pending: Optional[PendingDisambiguation] = None,
) -> IntentFindAProvider:
    args = [Argument(name="complaint", value=complaint, type="string", required=True)]
    geo_compact = {k: v for k, v in (geography or {}).items() if v}
    if geo_compact:
        args.append(Argument(
            name="geography",
            value=json.dumps(geo_compact),
            type="object",
            required=True,
        ))
    if nucc_codes_json:
        args.append(Argument(
            name="nucc_codes", value=nucc_codes_json, type="array", required=False,
        ))
    return IntentFindAProvider(
        name="findAProvider",
        arguments=args,
        pending_disambiguation=pending,
    )


def build_find_a_facility_intent(
    facility_type: str = "",
    geography: dict[str, Any] = None,
    facility_name: str = "",
    administrator_name: dict[str, str] = None,
) -> IntentFindAFacility:
    """The entry a facility search is dispatched from.

    A facility is named in one of three ways and any of them alone is
    enough to search on: the facility itself, the kind of facility, or the
    person who administers it. Geography narrows all three. At least one
    argument is always present because the classifier does not reach this
    branch without one.
    """
    args: list[Argument] = []
    if facility_type:
        args.append(Argument(name="facility_type", value=facility_type,
                             type="string", required=False))
    if facility_name:
        args.append(Argument(name="facility_name", value=facility_name,
                             type="string", required=False))
    administrator = {k: v for k, v in (administrator_name or {}).items() if v}
    if administrator:
        args.append(Argument(name="administrator_name",
                             value=json.dumps(administrator),
                             type="object", required=False))
    geo_compact = {k: v for k, v in (geography or {}).items() if v}
    if geo_compact:
        args.append(Argument(name="geography", value=json.dumps(geo_compact),
                             type="object", required=False))
    return IntentFindAFacility(name="findAFacility", arguments=args)


def build_find_clinical_trials_intent(
    complaint: str,
    user_location: Optional[str] = None,
    cursor: Optional[str] = None,
    age_years: Optional[int] = None,
    sex: Optional[str] = None,
    geographic_scope: Optional[str] = None,
) -> "IntentFindClinicalTrials":
    from chathealthy_lib.authentication.intent_document import IntentFindClinicalTrials
    args = [Argument(name="complaint", value=complaint, type="string", required=True)]
    if user_location:
        args.append(Argument(
            name="user_location", value=user_location, type="string", required=False,
        ))
    if cursor:
        args.append(Argument(
            name="cursor", value=cursor, type="string", required=False,
        ))
    if isinstance(age_years, int):
        args.append(Argument(
            name="age_years", value=str(age_years), type="integer", required=False,
        ))
    if sex:
        args.append(Argument(
            name="sex", value=str(sex), type="string", required=False,
        ))
    if geographic_scope:
        args.append(Argument(
            name="geographic_scope", value=str(geographic_scope), type="string", required=False,
        ))
    return IntentFindClinicalTrials(
        name="findClinicalTrials",
        arguments=args,
        pending_disambiguation=None,
    )


def build_close_connection_200_intent() -> IntentCloseConnection200:
    return IntentCloseConnection200(
        name="closeConnection200",
        arguments=[
            Argument(name="close_connection", value="true", type="boolean", required=True),
        ],
    )


def build_safety_lockout_intent(lockout_reason: str) -> "IntentSafetyLockout":
    """UM emits target_action=safetyLockout when the classifier judges the
    latest utterance signals immediate medical attention. The intent
    carries a single lockout_reason audit-label argument (max 256 chars);
    LockoutTool writes it onto the {env}_Safety.emergency_incidents row
    and renders the user-facing prose from the verbatim trigger
    utterance, not from this label."""
    from chathealthy_lib.authentication.intent_document import IntentSafetyLockout
    label = (lockout_reason or "").strip() or "immediate medical attention"
    return IntentSafetyLockout(
        name="safetyLockout",
        arguments=[
            Argument(
                name="lockout_reason",
                value=label[:256],
                type="string",
                required=True,
            ),
        ],
    )


def merge_intents(
    document: IntentDocument,
    new_intents: list[Any],
    target_action: str,
    user_message: Optional[str],
) -> IntentDocument:
    """Replace existing entries by name with the new ones, leaving any
    other entries intact. Cap at 3 to honor the schema."""
    new_names = {i.name for i in new_intents}
    keep = [i for i in document.intents if i.name not in new_names]
    keep.extend(new_intents)
    return IntentDocument(
        target_action=target_action,  # type: ignore[arg-type]
        intents=keep[-3:],
        user_message=user_message,
    )


# ────────────────────────────────────────────────────────────────────
# Tool entry point
# ────────────────────────────────────────────────────────────────────


def _latest_person_utterance(deps: AgentDeps) -> str:
    """What the user last said, verbatim. Empty when they said nothing."""
    for u in reversed(deps.user_object.session_conversation_history.utterances):
        actor = getattr(u, "actor", None) or (u.get("actor") if isinstance(u, dict) else None)
        text = getattr(u, "text", None) or (u.get("text") if isinstance(u, dict) else "")
        if actor == "person" and text:
            return str(text).strip()
    return ""


def _complaint_on(document) -> str:
    """The complaint argument already carried by an intent entry."""
    for entry in getattr(document, "intents", []) or []:
        for arg in getattr(entry, "arguments", []) or []:
            if arg.name == "complaint" and arg.value:
                return str(arg.value).strip()
    return ""


def to_pending(out: Optional[PendingDisambiguationOut]) -> Optional[PendingDisambiguation]:
    if out is None:
        return None
    return PendingDisambiguation(kind=out.kind, candidate=out.candidate)


class UtteranceManagerTool(ChatHealthyTool):
    """First-class tool the router dispatches to for op == 'utterance'.
    Classifies the latest utterance and writes the resulting IntentDocument
    onto deps.user_object.intent before returning to the router."""

    TOOL_NAME = "utterance_manager"
    Request = Request
    Response = Response

    async def run(self, deps: AgentDeps, request: "Request") -> "Response":
        # REQ-B-012: any exception inside UM is caught here and converted to
        # a closeConnection200 with a single clarification message. The
        # primary defenses (prompt rules 0-6, sufficiency checks) keep this
        # path cold; this is the backstop that guarantees a user utterance
        # NEVER produces a fatal 5xx.
        try:
            if request.trigger_type == "manufacture":
                return await self._run_manufacture(deps, request)
            return await self._run_interpret(deps, request)
        except Exception as exc:
            return await self._terminate_with_clarification(deps, exc)

    async def _terminate_with_clarification(
        self, deps: AgentDeps, exc: BaseException,
    ) -> "Response":
        """Single error-management site for the UM tool. ANY unhandled
        exception arrives here and is converted to a closeConnection200
        with one clarification message. No branching on exception type,
        no per-cause prose — one path, one outcome."""
        # Mode 2 (REQ-B-008): UM's one-catch-all-rule: ANY exception is
        # downgraded to a graceful clarification message to the user.
        # Operator MUST know — these are real failures masked by graceful
        # handling. log.error + always-log.
        log.error(
            "UM caught unhandled exception; downgrading to clarification: %s",
            exc,
            exc=ChatHealthyException(
                mode="um_unhandled_exception_downgraded",
                message=f"UM caught unhandled exception: {exc}",
                component="UtteranceManagerTool",
                exception=exc if isinstance(exc, Exception) else None,
            ),
            if_not_debug_log=True,
        )
        # Quote it back. "I had trouble understanding that" leaves the user
        # guessing which "that" -- whether the system heard them at all,
        # heard something else, or dropped the turn. Showing the words it
        # is holding makes the failure checkable by the person best placed
        # to check it, and tells them whether rephrasing is even the
        # problem.
        heard = _latest_person_utterance(deps)
        fallback = (
            f"I had trouble understanding “{heard}”. Could you say "
            f"it a different way?"
            if heard else
            "I did not receive anything to read. Could you say that again?"
        )
        prior = deps.user_object.intent
        base_doc = prior or IntentDocument(
            target_action="closeConnection200",
            intents=[build_close_connection_200_intent()],
        )
        new_doc = merge_intents(
            base_doc,
            [build_close_connection_200_intent()],
            target_action="closeConnection200",
            user_message=fallback,
        )
        from chathealthy_lib.authentication.agent_deps import append_system_utterance
        deps.stream({"kind": "prompt", "data": {"text": fallback}})
        append_system_utterance(deps.user_object, fallback)
        # No tool painted MainWindow this turn — direct the client to
        # restore the welcome bubble so the user reads the prompt against
        # a clean MainWindow surface.
        deps.stream({"kind": "show_welcome", "data": {}})
        deps.user_object.intent = new_doc
        await asyncio.sleep(0)
        return self.Response(target_action="closeConnection200")

    async def _run_manufacture(self, deps: AgentDeps, request: "Request") -> "Response":
        """Manufacture path per EPIC-002-F-010-S-003. UR populated
        request.manufacture_utterance_reason with structured facts. UM
        authors a context-sensitive user_message via the manufacture
        LLM and morphs the IntentDocument to target_action=
        closeConnection200."""
        transcript = recent_transcript(deps)  # may be empty
        prior = deps.user_object.intent

        llm_result = await call_manufacture_llm(
            deps, transcript, request.manufacture_utterance_reason, prior,
            deps.user_object.userParameters,
        )
        user_message = (llm_result.user_message or "").strip()
        if not user_message:
            raise ChatHealthyException(
                mode="um_manufacture_empty_user_message",
                message="UtteranceManager manufacture-path: LLM returned empty user_message",
                component="UtteranceManager",
            )

        # Build the resulting IntentDocument. Carry forward any prior
        # intents[] entries (cached nucc_codes, partial geography,
        # pending_disambiguation markers) so the next-turn interpret
        # path can resolve naturally. The only thing this turn dictates
        # is target_action=closeConnection200 + the close intent entry.
        base_doc = prior or IntentDocument(
            target_action="closeConnection200",
            intents=[build_close_connection_200_intent()],
        )
        new_doc = merge_intents(
            base_doc,
            [build_close_connection_200_intent()],
            target_action="closeConnection200",
            user_message=user_message,
        )

        # Stream + persist the manufactured prose, same shape as the
        # interpret path's user_message handling.
        from chathealthy_lib.authentication.agent_deps import append_system_utterance
        deps.stream({"kind": "prompt", "data": {"text": user_message}})
        append_system_utterance(deps.user_object, user_message)
        # Manufacture path always closes without a tool paint — direct the
        # client to restore the welcome bubble.
        deps.stream({"kind": "show_welcome", "data": {}})

        deps.user_object.intent = new_doc
        await asyncio.sleep(0)
        return self.Response(target_action=new_doc.target_action)

    async def _run_interpret(self, deps: AgentDeps, request: "Request") -> "Response":
        transcript = recent_transcript(deps)
        if not transcript:
            raise ChatHealthyException(
                mode="um_no_utterances_on_user_object",
                message="UtteranceManager: no utterances on user_object",
                component="UtteranceManager",
            )
        # latest_text must be the LATEST PERSON utterance (raw text, no
        # 'user: ' prefix). Read it directly off the live bucket so the
        # intent-builders get the verbatim string.
        latest_text = ""
        for u in reversed(deps.user_object.session_conversation_history.utterances):
            actor = getattr(u, "actor", None) or (u.get("actor") if isinstance(u, dict) else None)
            text = getattr(u, "text", None) or (u.get("text") if isinstance(u, dict) else "")
            if actor == "person" and text:
                latest_text = str(text).strip()
                break
        if not latest_text:
            raise ChatHealthyException(
                mode="um_no_person_utterance_on_user_object",
                message="UtteranceManager: no person utterance on user_object",
                component="UtteranceManager",
            )
        prior = deps.user_object.intent

        llm_result = await call_classifier_llm(
            deps, transcript, prior, deps.user_object.userParameters)
        target_action = llm_result.target_action
        complaint = (llm_result.complaint or "").strip()

        # UM corrects spelling, routes, and asks when it cannot route. It
        # decides nothing about the state of a search: whether a panel is
        # stale is the specialty tool's to judge, because the tool holds the
        # panel. What UM must not do is clobber -- a turn that answers "did
        # you mean California?" is completing a slot on the intent already in
        # hand, so the complaint already on that document stands and the
        # model's fresh guess for it ("general health concern") is dropped.
        if not complaint and prior is not None:
            complaint = _complaint_on(prior)

        geography = llm_result.geography.model_dump() if llm_result.geography else {}
        user_location = (llm_result.user_location or "").strip() or None
        user_message = (llm_result.user_message or "").strip() or None
        pending = to_pending(llm_result.pending_disambiguation)

        base_doc = prior or IntentDocument(
            target_action="closeConnection200",
            intents=[build_close_connection_200_intent()],
        )

        # Cache lookups so we can carry SpecialtyFilter output across turns.
        cached_specialty = cached_nucc_codes(existing_intent(base_doc, "specialtySearch"))
        cached_findap = cached_nucc_codes(existing_intent(base_doc, "findAProvider"))
        cached_nucc = cached_specialty or cached_findap

        if target_action == "safetyLockout":
            # Rule 0: UM judged immediate medical attention. user_message
            # stays empty (LockoutTool authors the verbatim "when you
            # said '...'" + 911 + operator phone text). Only the
            # lockout_reason audit-label arg lands on the intent entry.
            new_doc = merge_intents(
                base_doc,
                [build_safety_lockout_intent(llm_result.lockout_reason or "")],
                target_action,
                user_message=None,
            )

        elif target_action == "specialtySearch":
                # Routing is this component's whole job. Whether the tool it
                # routes to has enough to work with is that tool's to judge,
                # against its own declaration, and to ask about when it does not.
                # Refusing here left the person with an exception where a question
                # belonged.
            built: list[Any] = [
                build_specialty_search_intent(complaint, cached_nucc),
            ]
            # Ambiguous-but-resolvable: also park a findAProvider entry
            # with the partial geography and the pending disambiguation
            # marker, so a follow-up "yes" can upgrade it cleanly.
            if pending is not None:
                built.append(build_find_a_provider_intent(
                    complaint,
                    geography,  # partial allowed
                    nucc_codes_json=cached_nucc,
                    pending=pending,
                ))
            new_doc = merge_intents(base_doc, built, target_action, user_message)

        elif target_action == "findAProvider":
            # An all-empty geography never arrives here: the classifier's
            # output validator rejects it and the agent retries. Enforcing
            # it there rather than recovering from it here keeps one
            # statement of the rule, in the contract the model answers to.
                # Routing is this component's whole job. Whether the tool it
                # routes to has enough to work with is that tool's to judge,
                # against its own declaration, and to ask about when it does not.
                # Refusing here left the person with an exception where a question
                # belonged.
            # Whether a place is enough to search on is not decided here.
            # It is declared -- the care-giver page requires a state and
            # accepts a city, a ZIP or a county -- and the page enforces
            # its own declaration, asks for what is missing, and writes
            # what was said.
            #
            # Refusing here sent the turn to specialtySearch instead, so
            # the page never ran: nothing wrote the city the person named,
            # nothing searched once they supplied the state, and the
            # answer to the question had nothing to attach itself to.
            built = [
                build_specialty_search_intent(complaint, cached_nucc),
                build_find_a_provider_intent(
                    complaint, geography, nucc_codes_json=cached_nucc, pending=None,
                ),
            ]
            new_doc = merge_intents(base_doc, built, target_action, user_message)

        elif target_action == "findAFacility":
            # Any one of the three ways of naming a facility is enough, so
            # this branch refuses only the turn that named none of them.
            facility_name = (llm_result.facility_name or "").strip()
            facility_type = (llm_result.facility_type or "").strip()
            administrator = {
                "last": (llm_result.administrator_last_name or "").strip(),
                "first": (llm_result.administrator_first_name or "").strip(),
                "middle": (llm_result.administrator_middle_name or "").strip(),
            }
                # Routing is this component's whole job. Whether the tool it
                # routes to has enough to work with is that tool's to judge,
                # against its own declaration, and to ask about when it does not.
                # Refusing here left the person with an exception where a question
                # belonged.
            new_doc = merge_intents(
                base_doc,
                [build_find_a_facility_intent(
                    facility_type=facility_type,
                    geography=geography,
                    facility_name=facility_name,
                    administrator_name=administrator,
                )],
                target_action, user_message)

        elif target_action == "findClinicalTrials":
                # Routing is this component's whole job. Whether the tool it
                # routes to has enough to work with is that tool's to judge,
                # against its own declaration, and to ask about when it does not.
                # Refusing here left the person with an exception where a question
                # belonged.
            new_doc = merge_intents(
                base_doc,
                [build_find_clinical_trials_intent(
                    complaint,
                    user_location,
                    age_years=llm_result.age_years,
                    sex=llm_result.sex,
                    geographic_scope=llm_result.geographic_scope,
                )],
                target_action,
                user_message,
            )

        elif target_action == "closeConnection200":
            new_doc = merge_intents(
                base_doc,
                [build_close_connection_200_intent()],
                target_action,
                user_message,
            )

        else:
            # No route. Not an error -- a turn this component could not
            # place, which is the one thing it is entitled to conclude.
            # It hands the turn to the path that asks the person, rather
            # than raising and leaving them with a dead screen.
            return await self._run_manufacture(
                deps,
                Request(
                    trigger_type="manufacture",
                    manufacture_utterance_reason={
                        "outcome": "no tool could be chosen for this turn",
                        "unplaced_target_action": target_action,
                    },
                ),
            )

        # Stream the LLM-authored user_message before returning, per
        # REQ-B-009 and REQ-B-010. Also append it to the dialogue bucket
        # as a system utterance so it (a) appears verbatim on the splash
        # next to the prior person turn, and (b) is part of the labeled
        # transcript UM hands itself on the NEXT turn, which is what
        # gives a follow-up "yes" its referent.
        if new_doc.user_message:
            from chathealthy_lib.authentication.agent_deps import append_system_utterance
            data: dict[str, Any] = {"text": new_doc.user_message}
            if llm_result.corrections:
                data["corrections"] = [
                    {"original": c.original, "corrected": c.corrected}
                    for c in llm_result.corrections
                ]
            deps.stream({"kind": "prompt", "data": data})
            append_system_utterance(deps.user_object, new_doc.user_message)

        # When UM closes the turn without dispatching a content-painting
        # tool (target_action=closeConnection200), no tool will paint
        # MainWindow. Direct the client to restore the welcome bubble so
        # the user reads any streamed prompt against a clean surface.
        # When target_action names a tool, the tool paints MainWindow and
        # this directive is NOT emitted.
        if new_doc.target_action == "closeConnection200":
            deps.stream({"kind": "show_welcome", "data": {}})

        deps.user_object.intent = new_doc

        # Geography is the user's, not this turn's, and not this action's.
        # The classifier reports a place in one of two fields depending on
        # what the user was asking about: `geography`, structured, when the
        # question is about providers, and `user_location`, free text, when
        # it is about trials. That is one fact under two names, which is
        # why saying "New York" while looking at trials never reached the
        # provider search. Both are written to the one parameter.
        #
        # Written only when this turn named a place: a turn that names none
        # must not erase the place already set, which is what lets the user
        # say it once.
        live_geo = geography
        if not live_geo and user_location:
            live_geo = await _structure_location(user_location)
        from UserParameters import user_parameters_tool
        if live_geo:
            # Written to the page this classification is about. Whether it
            # reaches the facility page is a stated carry-over triple and
            # not a second write here: a parameter of the same name does
            # not carry between pages by being called the same thing.
            stated = live_geo if isinstance(live_geo, dict) else (
                live_geo.model_dump() if hasattr(live_geo, "model_dump") else {})
            changes = [
                user_parameters_tool.Change(
                    page="individualProvider", name=part, value=value)
                for part, value in stated.items()
                if part in ("state", "city", "zip", "county") and value]
            if changes:
                await user_parameters_tool.TOOL.run_and_log(
                    deps,
                    user_parameters_tool.Request(
                        verb="set", changes=changes,
                        route="utterance_manager",
                        origin="non_deterministic",
                    ),
                )

        # The classifier already produced this. It reads the utterance and
        # emits what the user is asking about, which is the definition of
        # the complaint -- there was never a second translator to build.
        if complaint:
            await user_parameters_tool.TOOL.run_and_log(
                deps,
                user_parameters_tool.Request(
                    verb="set", page="NUCC", name="complaint",
                    value=complaint,
                    route="utterance_manager", origin="non_deterministic",
                ),
            )

        # The person's own phrase, on the page whose summary has to say it
        # back to them. This is why no second model call re-reads the
        # utterance: this component has already read it.
        search_phrase = (llm_result.search_phrase or "").strip()
        if search_phrase:
            await user_parameters_tool.TOOL.run_and_log(
                deps,
                user_parameters_tool.Request(
                    verb="set", page="individualProvider", name="complaint",
                    value=search_phrase,
                    route="utterance_manager", origin="non_deterministic",
                ),
            )


        # What the person named a facility by. Written to the facility
        # page, whose attributes these are.
        facility_writes = []
        if (llm_result.facility_type or "").strip():
            facility_writes.append(user_parameters_tool.Change(
                page="facility", name="facilityType",
                value=llm_result.facility_type.strip()))
        if (llm_result.facility_name or "").strip():
            facility_writes.append(user_parameters_tool.Change(
                page="facility", name="facilityName",
                value=llm_result.facility_name.strip()))
        administrator_parts = {
            "last": (llm_result.administrator_last_name or "").strip(),
            "first": (llm_result.administrator_first_name or "").strip(),
            "middle": (llm_result.administrator_middle_name or "").strip(),
        }
        if any(administrator_parts.values()):
            facility_writes.append(user_parameters_tool.Change(
                page="facility", name="administratorName",
                value=administrator_parts))
        if facility_writes:
            await user_parameters_tool.TOOL.run_and_log(
                deps,
                user_parameters_tool.Request(
                    verb="set", route="utterance_manager",
                    origin="non_deterministic", changes=facility_writes,
                ),
            )

        # Narrowings the person stated in words. Same rule as geography:
        # written only when this turn named one, so "a female doctor" said
        # once keeps applying and a later turn that mentions nothing does
        # not silently drop it.
        name_parts = {
            "last": llm_result.provider_last_name or "",
            "first": llm_result.provider_first_name or "",
            "middle": llm_result.provider_middle_name or "",
        }
        if any(name_parts.values()):
            await user_parameters_tool.TOOL.run_and_log(
                deps,
                user_parameters_tool.Request(
                    verb="set", page="individualProvider", name="providerName",
                    value=name_parts,
                    route="utterance_manager", origin="non_deterministic",
                ),
            )
        # A narrowing the turn removes. Absent means unchanged, so without
        # this a filter could be set by speech and never lifted by it.
        CLEARABLE = {"provider_sex": "providerSex",
                     "insurance": "insurance",
                     "sole_proprietor": "soleProprietor",
                     "provider_name": "providerName"}
        for field in (llm_result.cleared_narrowings or ()):
            attribute = CLEARABLE.get(field)
            if attribute is None:
                continue
            await user_parameters_tool.TOOL.run_and_log(
                deps,
                user_parameters_tool.Request(
                    verb="clear", page="individualProvider", name=attribute,
                    route="utterance_manager", origin="non_deterministic",
                ),
            )

        for attribute, value in (("providerSex", llm_result.provider_sex),
                                 ("insurance", llm_result.insurance),
                                 ("soleProprietor", llm_result.sole_proprietor)):
            if value is None or value == "":
                continue
            await user_parameters_tool.TOOL.run_and_log(
                deps,
                user_parameters_tool.Request(
                    verb="set", page="individualProvider", name=attribute,
                    value=value,
                    route="utterance_manager", origin="non_deterministic",
                ),
            )

        await asyncio.sleep(0)
        return self.Response(target_action=new_doc.target_action)


TOOL = UtteranceManagerTool()
