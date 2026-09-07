# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""UserObject — Pydantic model for the ChatHealthy session-bound user state.

Mirrors the canonical schema at
https://dev.chathealthy.ai/schemas/ChatHealthyUserStateSchema.json, plus
two additions per Skip 2026-05-15:

  * `current_session_token` — the signed security token, restamped on
    every gate hit (new NONCE each time).
  * `expires_at` — reset to NOW + 300s on each restamp; the canonical
    "this session expires at" marker.

Absence-means-not-yet-known: every other field is Optional and only
populated when its fact exists (e.g., `registered_profile` appears
once OAuth completes, never as a ceremonial empty stub).

Persistence: stored in `admin.sessions`, ONE doc per live session,
keyed by `_id = GUID`. The document body is the UserObject serialized
to JSON (model_dump(mode='python', exclude_none=True)) — flat: every
UserObject field at the top level alongside `_id`.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum, auto
from typing import Annotated, Literal, Optional, Union, get_origin

from pydantic import BaseModel, Field

from chathealthy_lib.authentication.session_token import SessionToken

from chathealthy_lib.authentication.intent_document import IntentDocument


from chathealthy_lib.authentication.user_parameters import UserParameters


class MergeRole(Enum):
    STORED_WINS = auto()
    GUEST_WINS = auto()
    CUMULATIVE_NESTED = auto()
    CUMULATIVE_COUNTER = auto()


def _field_role(field_info) -> MergeRole:
    for m in field_info.metadata:
        if isinstance(m, MergeRole):
            return m
    return MergeRole.STORED_WINS


def _merge_nested(stored, guest):
    if stored is None and guest is None:
        return None
    cls = type(stored if stored is not None else guest)
    s = stored if stored is not None else cls()
    g = guest if guest is not None else cls()
    out: dict = {}
    for name, fi in cls.model_fields.items():
        if get_origin(fi.annotation) is list:
            out[name] = list(getattr(s, name) or []) + list(getattr(g, name) or [])
        else:
            out[name] = getattr(s, name)
    return cls.model_validate(out)


def _merge_counter(stored, guest):
    if stored is None and guest is None:
        return None
    cls = type(stored if stored is not None else guest)
    s = stored if stored is not None else cls()
    g = guest if guest is not None else cls()
    out: dict = {}
    for name, fi in cls.model_fields.items():
        sv = getattr(s, name)
        gv = getattr(g, name)
        if isinstance(sv, int) and isinstance(gv, int):
            out[name] = sv + gv
        else:
            out[name] = sv
    return cls.model_validate(out)


class Utterance(BaseModel):
    """One line of verbatim dialogue between person and system.

    Both actors land in the same bucket so the dialogue reads top-to-bottom
    as a narrative — exactly the form the LLM classifier receives when
    UM rebuilds the transcript as "user: ...\\nsystem: ...".
    """
    n: int = Field(ge=1, description="Per-session sequence, monotonic from 1.")
    at: str = Field(description="ISO-8601 local time + literal ' PST' suffix.")
    actor: Literal["person", "system"]
    text: str


class Action(BaseModel):
    """One system-recorded event in the session. Includes tool invocations
    (UM, UR-dispatched tools), the on_load marker (action #1 of every
    session), and recorded UX gestures (button clicks, etc.).
    """
    n: int = Field(ge=1, description="Per-session sequence, monotonic from 1.")
    at: str = Field(description="ISO-8601 local time + literal ' PST' suffix.")
    tool_name: str = Field(description="'on_load' for action #1; otherwise the tool's TOOL_NAME or 'ux_event'.")
    input_json: dict = Field(default_factory=dict)
    output_json: dict = Field(default_factory=dict)


class SessionConversationHistory(BaseModel):
    """Two buckets per Skip 2026-06-10: a single dialogue narrative and a
    parallel system-action log. Each bucket has its own per-session counter
    starting at 1. Timestamps on every entry handle cross-bucket ordering.
    """
    utterances: list[Utterance] = Field(default_factory=list)
    actions: list[Action] = Field(default_factory=list)


class PortalAccess(BaseModel):
    """Populated when InsuranceInfo.portal_allowed is true."""
    last_update_date: Optional[date] = None
    accumulator_facts: list[dict] = Field(default_factory=list)
    last_three_claim_eobs: list[dict] = Field(default_factory=list, max_length=3)
    credentials: Optional[dict] = None


class InsuranceInfo(BaseModel):
    plan: Optional[str] = None
    plan_state: Optional[str] = None
    plan_url: Optional[str] = None
    plan_begin_date: Optional[date] = None
    plan_end_date: Optional[date] = None
    portal_allowed: Optional[bool] = None
    portal_access: Optional[PortalAccess] = None


class EMR(BaseModel):
    portal_address: Optional[str] = None
    credentials: Optional[dict] = None
    fhir_summary_document: Optional[dict] = None


class FinancialFacts(BaseModel):
    """Schema spec: list of {condition, spend_limit} entries."""
    max_spend_out_of_pocket: list[dict] = Field(default_factory=list)


class RegisteredUserProfileFacts(BaseModel):
    gender: Optional[str] = None
    sex: Optional[Literal["Male", "Female", "Other", "Did not disclose"]] = None
    age: Optional[int] = Field(default=None, ge=0)
    insurance_info: Optional[InsuranceInfo] = None
    emr: Optional[EMR] = None
    unstructured_facts: dict = Field(default_factory=dict)
    financial_facts: Optional[FinancialFacts] = None


class RegisteredProfile(BaseModel):
    user_id: str
    user_type: Literal["Guest", "Owner", "Customer", "Prospect"]
    oauth_source: Literal["Google"]
    auth_token: str  # OAuth provider's token, distinct from current_session_token
    profile_facts: RegisteredUserProfileFacts


class NotRegisteredFollowup(BaseModel):
    has_asked_for_follow_ups: bool
    follow_up_record_id: Optional[str] = None


class OAuthIdentity(BaseModel):
    identity_provider: str
    identity_provider_user_id: str
    email: str


class Lockout(BaseModel):
    """Active safety-lockout state populated by UR's hydration step from
    {env}_Safety.emergency_incidents (keyed by user_object.ip_address).
    None on the user_object means there is no active lockout. UR sets
    user_object.is_locked_out=True and stamps this sub-object straight
    from the matching DB record before any tool dispatch.

    Fields:
      - expires_at:        when the lockout naturally expires
      - trigger_utterance: the verbatim text that caused the lockout, so
                           LockoutTool's Task B reminder can render the
                           same "when you said '...'" prose Task C did
                           on the locking turn
      - history:           snapshot of utterances at lockout time, for
                           in-session forensics; the {env}_Safety doc
                           keeps the canonical audit trail
    """
    expires_at: datetime
    trigger_utterance: str
    history: list[dict] = Field(default_factory=list)


class UserObject(BaseModel):
    """Session-bound user state. Field roles declared via Annotated[..., MergeRole.X]
    drive merge() at runtime; no per-field merge code anywhere in this class."""
    current_session_token: Annotated[
        Union[SessionToken, Literal["NULL"]], MergeRole.GUEST_WINS,
    ]
    expires_at: Annotated[datetime, MergeRole.GUEST_WINS]
    session_conversation_history: Annotated[
        SessionConversationHistory, MergeRole.CUMULATIVE_NESTED,
    ] = Field(default_factory=SessionConversationHistory)
    is_locked_out: Annotated[Optional[bool], MergeRole.STORED_WINS] = None
    lockout: Annotated[Optional[Lockout], MergeRole.STORED_WINS] = None
    ip_address: Annotated[Optional[str], MergeRole.GUEST_WINS] = None
    # Which arrangement the person is looking at. Established with the
    # session and fixed for its life. Only the browser knows it -- a
    # narrow window on a computer is a phone as far as the layout is
    # concerned -- so it is reported once, when /auth/issue makes the
    # session.
    form_factor: Annotated[
        Optional[Literal["phone", "desktop"]], MergeRole.STORED_WINS,
    ] = None
    is_registered: Annotated[Optional[bool], MergeRole.STORED_WINS] = None
    user_id: Annotated[Optional[str], MergeRole.STORED_WINS] = None
    user_type: Annotated[
        Optional[Literal["Guest", "Owner", "Customer", "Prospect"]],
        MergeRole.STORED_WINS,
    ] = None
    public_username: Annotated[Optional[str], MergeRole.STORED_WINS] = None
    OAuthIdentities: Annotated[
        list[OAuthIdentity], MergeRole.STORED_WINS,
    ] = Field(default_factory=list)
    registered_profile: Annotated[
        Optional[RegisteredProfile], MergeRole.STORED_WINS,
    ] = None
    not_registered_followup: Annotated[
        Optional[NotRegisteredFollowup], MergeRole.STORED_WINS,
    ] = None
    intent: Annotated[
        Optional["IntentDocument"], MergeRole.GUEST_WINS,
    ] = None
    # The live user parameters. State, not history: current values only.
    # The history of how they got there is session_conversation_history,
    # which run_and_log writes on every tool invocation.
    #
    # GUEST_WINS for the same reason intent is: these belong to the session
    # in front of the user, and a stored copy from an earlier login must not
    # overwrite what they just asked for.
    userParameters: Annotated[
        UserParameters, MergeRole.GUEST_WINS,
    ] = Field(default_factory=UserParameters)
    selected_providers: Annotated[
        list[str], MergeRole.GUEST_WINS,
    ] = Field(default_factory=list)
    # The facilities the person has chosen. Its own set, because a
    # facility answer and a care-giver answer stand together and neither
    # displaces the other (EPIC-006-F-008-S-001-REQ-B-006).
    selected_facilities: Annotated[
        list[str], MergeRole.GUEST_WINS,
    ] = Field(default_factory=list)
    selected_clinical_trials: Annotated[
        list[str], MergeRole.GUEST_WINS,
    ] = Field(default_factory=list)
    # Stash for OAuth result so the React HeaderWidget can poll /gate after
    # the popup closes and render the success/fail banner. Cleared by the
    # claim_oauth_result op once the widget has read it.
    pending_oauth_result: Annotated[
        Optional[dict], MergeRole.GUEST_WINS,
    ] = None

    def merge(self, guest: "UserObject") -> "UserObject":
        merged: dict = {}
        for name, fi in type(self).model_fields.items():
            role = _field_role(fi)
            stored_val = getattr(self, name)
            guest_val = getattr(guest, name)
            if role is MergeRole.STORED_WINS:
                merged[name] = stored_val
            elif role is MergeRole.GUEST_WINS:
                merged[name] = guest_val
            elif role is MergeRole.CUMULATIVE_NESTED:
                merged[name] = _merge_nested(stored_val, guest_val)
            elif role is MergeRole.CUMULATIVE_COUNTER:
                merged[name] = _merge_counter(stored_val, guest_val)
        return type(self).model_validate(merged)

    def persist_user_state(self, text: str) -> None:
        """Append the typed user text to the session conversation history
        as a person utterance with the next sequence number."""
        from chathealthy_lib.authentication.agent_deps import append_person_utterance
        append_person_utterance(self, text)
