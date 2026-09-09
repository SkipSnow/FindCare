# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""Input/output Pydantic models for the ClinicalTrialsTool.

EPIC-006-F-005-S-003-REQ-B-001 / REQ-B-002. Field set tracks the V7 in-scope
attribute list (identification, status, sponsorCollaborators, oversight,
description, conditions, design, armsInterventions, outcomes, eligibility,
contactsLocations, references, ipdSharingStatement, derived/conditionBrowse,
derived/interventionBrowse, document, annotationSection)."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class Location(BaseModel):
    facility: str = ""
    status: str = ""
    city: str = ""
    state: str = ""
    country: str = ""
    zip: str = ""


class CentralContact(BaseModel):
    name: str = ""
    role: str = ""
    phone: str = ""
    phone_ext: Optional[str] = None
    email: Optional[str] = None
    npi: Optional[str] = None


class OverallOfficial(BaseModel):
    name: str = ""
    affiliation: str = ""
    role: str = ""


class Reference(BaseModel):
    pmid: str = ""
    type: str = ""
    citation: str = ""


class SeeAlsoLink(BaseModel):
    label: str = ""
    url: str = ""


class SecondaryId(BaseModel):
    id: str = ""
    type: str = ""
    domain: str = ""


class Organization(BaseModel):
    full_name: str = ""
    class_: str = Field(default="", alias="class")
    model_config = {"populate_by_name": True}


class ResponsibleParty(BaseModel):
    type: str = ""
    investigator_full_name: str = ""
    investigator_title: str = ""
    investigator_affiliation: str = ""
    old_name_title: str = ""
    old_organization: str = ""


class Collaborator(BaseModel):
    name: str = ""
    class_: str = Field(default="", alias="class")
    model_config = {"populate_by_name": True}


class DesignInfo(BaseModel):
    allocation: str = ""
    intervention_model: str = ""
    intervention_model_description: str = ""
    primary_purpose: str = ""
    observational_model: str = ""
    time_perspective: str = ""
    masking: str = ""
    masking_description: str = ""
    who_masked: list[str] = Field(default_factory=list)


class BioSpec(BaseModel):
    retention: str = ""
    description: str = ""


class ArmGroup(BaseModel):
    label: str = ""
    type: str = ""
    description: str = ""
    intervention_names: list[str] = Field(default_factory=list)


class Intervention(BaseModel):
    type: str = ""
    name: str = ""
    description: str = ""
    arm_group_labels: list[str] = Field(default_factory=list)
    other_names: list[str] = Field(default_factory=list)


class Outcome(BaseModel):
    measure: str = ""
    description: str = ""
    time_frame: str = ""


class IpdSharing(BaseModel):
    ipd_sharing: str = ""
    description: str = ""
    info_types: list[str] = Field(default_factory=list)
    time_frame: str = ""
    access_criteria: str = ""
    url: str = ""


class BrowseLeaf(BaseModel):
    id: str = ""
    name: str = ""
    as_found: str = ""
    relevance: str = ""


class BrowseBranch(BaseModel):
    abbrev: str = ""
    name: str = ""


class LargeDoc(BaseModel):
    type_abbrev: str = ""
    has_protocol: bool = False
    has_sap: bool = False
    has_icf: bool = False
    label: str = ""
    date: str = ""
    upload_date: str = ""
    filename: str = ""
    size: Optional[int] = None


class UnpostedEvent(BaseModel):
    type: str = ""
    date: str = ""


class UnpostedAnnotation(BaseModel):
    unposted_responsible_party: str = ""
    unposted_events: list[UnpostedEvent] = Field(default_factory=list)


class ViolationEvent(BaseModel):
    type: str = ""
    description: str = ""
    creation_date: str = ""
    issued_date: str = ""
    release_date: str = ""
    posted_date: str = ""


class ViolationAnnotation(BaseModel):
    violation_events: list[ViolationEvent] = Field(default_factory=list)


class Trial(BaseModel):
    # identification
    nct_id: str
    brief_title: str = ""
    official_title: str = ""
    acronym: str = ""
    organization: Organization = Field(default_factory=Organization)
    secondary_id_infos: list[SecondaryId] = Field(default_factory=list)

    # status
    overall_status: str = ""
    last_known_status: str = ""
    start_date: str = ""
    primary_completion_date: str = ""
    last_update_post_date: str = ""
    study_first_submit_date: str = ""

    # sponsorCollaborators
    lead_sponsor_name: str = ""
    lead_sponsor_class: str = ""
    responsible_party: ResponsibleParty = Field(default_factory=ResponsibleParty)
    collaborators: list[Collaborator] = Field(default_factory=list)

    # oversight
    oversight_has_dmc: bool = False
    is_fda_regulated_drug: bool = False
    is_fda_regulated_device: bool = False
    is_unapproved_device: bool = False
    is_ppsd: bool = False
    fdaaa_801_violation: bool = False

    # description
    brief_summary: str = ""
    detailed_description: str = ""

    # conditions
    conditions: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)

    # design
    study_type: str = ""
    phases: list[str] = Field(default_factory=list)
    enrollment_count: Optional[int] = None
    enrollment_type: str = ""
    design_info: DesignInfo = Field(default_factory=DesignInfo)
    bio_spec: BioSpec = Field(default_factory=BioSpec)
    target_duration: str = ""
    expanded_access_types: list[str] = Field(default_factory=list)

    # armsInterventions
    arm_groups: list[ArmGroup] = Field(default_factory=list)
    interventions: list[Intervention] = Field(default_factory=list)

    # outcomes
    primary_outcomes: list[Outcome] = Field(default_factory=list)
    secondary_outcomes: list[Outcome] = Field(default_factory=list)
    other_outcomes: list[Outcome] = Field(default_factory=list)

    # eligibility
    eligibility_criteria: str = ""
    healthy_volunteers: bool = False
    sex: str = ""
    gender_description: str = ""
    minimum_age: str = ""
    maximum_age: str = ""

    # contactsLocations
    central_contacts: list[CentralContact] = Field(default_factory=list)
    overall_officials: list[OverallOfficial] = Field(default_factory=list)
    locations: list[Location] = Field(default_factory=list)

    # references
    references: list[Reference] = Field(default_factory=list)
    see_also_links: list[SeeAlsoLink] = Field(default_factory=list)

    # ipdSharingStatement
    ipd_sharing: IpdSharing = Field(default_factory=IpdSharing)

    # derivedSection — conditionBrowse + interventionBrowse
    condition_browse_leaves: list[BrowseLeaf] = Field(default_factory=list)
    condition_browse_branches: list[BrowseBranch] = Field(default_factory=list)
    intervention_browse_leaves: list[BrowseLeaf] = Field(default_factory=list)
    intervention_browse_branches: list[BrowseBranch] = Field(default_factory=list)

    # documentSection
    large_docs: list[LargeDoc] = Field(default_factory=list)

    # annotationSection
    unposted_annotation: UnpostedAnnotation = Field(default_factory=UnpostedAnnotation)
    violation_annotation: ViolationAnnotation = Field(default_factory=ViolationAnnotation)

    # derived locally
    study_url: str = ""
    derived_nucc_codes: list[str] = Field(default_factory=list)


class Request(BaseModel):
    # EPIC-006-F-005-S-001-REQ-B-069 (sex only — gender intentionally absent)
    # EPIC-006-F-005-S-001-REQ-B-074 (geographic_scope: international | us)
    # No location of the person: no requirement asks a trial search to
    # narrow by where the person is, and a parameter no requirement
    # consumes is forbidden by EPIC-006-F-008-S-006-REQ-B-004.
    model_config = {"extra": "ignore"}
    condition: str
    page_size: int = 10
    cursor: Optional[str] = None
    age_years: Optional[int] = None
    sex: Optional[str] = None
    geographic_scope: Optional[str] = "international"


class SearchContext(BaseModel):
    """Echoes the actual query parameters the tool used to fetch this page.
    The iframe stores this and re-issues it verbatim on pagination so the
    user's natural-language utterance doesn't leak into the CT.gov call.
    Every value echoed is a value that reached the registry, so a
    narrowing that did not narrow anything cannot appear as though it had.
    EPIC-006-F-005-S-002-REQ-B-008."""
    condition: str = ""
    age_years: Optional[int] = None
    sex: Optional[str] = None
    geographic_scope: Optional[str] = "international"


class Response(BaseModel):
    trials: list[Trial] = Field(default_factory=list)
    cursor: Optional[str] = None
    total_count: Optional[int] = None
    page_size: Optional[int] = None
    error: Optional[str] = None
    search_context: SearchContext = Field(default_factory=SearchContext)
    # The ways of narrowing the search the person has not yet used
    # (EPIC-006-F-005-S-001-REQ-B-081). FindCare states which exist; the
    # utterance manager owns asking for them.
    refinements_not_used: list[str] = Field(default_factory=list)


class ClinicalTrialsInput(BaseModel):
    """What the trial-search TOOL accepts.

    Distinct from Request above, which is what the tool sends onward to the
    registry. This is the shape the router validates a caller against.
    """
    condition: str = Field(..., description="Medical condition to search trials for")
    location: str = Field("", description="Location filter")
    user_location: str = Field("", description="User's location for travel time — any location worldwide")
    max_results: int = Field(5, description="Max results")
