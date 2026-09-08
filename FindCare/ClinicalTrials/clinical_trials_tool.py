# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""ClinicalTrialsTool — EPIC-006-F-005.

Fetches recruiting trials from ClinicalTrials.gov v2 by condition and
streams them to the client. No per-trial enrichment: NUCC-code
derivation and NPI attachment produced fields the widget never
displayed, and holding the stream open across those late passes kept
the input prompt disabled after the user could already see results.

A single outer try/except converts any exception to an empty result with
an error message — REQ-B-012 says no fatal exception escapes."""
from __future__ import annotations

from typing import Any, Optional

import httpx

from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException

from chathealthy_lib.authentication.agent_deps import AgentDeps
from chathealthy_lib.authentication.chathealthy_tool import ChatHealthyTool

try:
    from ClinicalTrials.clinical_trials_models import (
        ArmGroup, BioSpec, BrowseBranch, BrowseLeaf, CentralContact,
        Collaborator, DesignInfo, Intervention, IpdSharing, LargeDoc,
        Location, Organization, Outcome, OverallOfficial, Reference,
        Request, Response, ResponsibleParty, SearchContext, SecondaryId,
        SeeAlsoLink, Trial, UnpostedAnnotation, UnpostedEvent,
        ViolationAnnotation, ViolationEvent,
    )
except ImportError:
    from FindCare.ClinicalTrials.clinical_trials_models import (
        ArmGroup, BioSpec, BrowseBranch, BrowseLeaf, CentralContact,
        Collaborator, DesignInfo, Intervention, IpdSharing, LargeDoc,
        Location, Organization, Outcome, OverallOfficial, Reference,
        Request, Response, ResponsibleParty, SearchContext, SecondaryId,
        SeeAlsoLink, Trial, UnpostedAnnotation, UnpostedEvent,
        ViolationAnnotation, ViolationEvent,
    )

log = ChatHealthyLoggingService()

CT_GOV_URL = "https://clinicaltrials.gov/api/v2/studies"

# Page size the registry is asked for per cursor step. The registry
# pages by the token it returns and the tool follows that token, so this
# is how much of the result arrives per hop and not how much of it the
# tool will ever see.
_CT_GOV_PAGE_SIZE = 100


def _s(v) -> str:
    return (v or "").strip() if isinstance(v, str) else ""


def _parse_trial(study: dict) -> Trial:
    ps = study.get("protocolSection", {}) or {}
    id_mod = ps.get("identificationModule", {}) or {}
    status_mod = ps.get("statusModule", {}) or {}
    spons_mod = ps.get("sponsorCollaboratorsModule", {}) or {}
    over_mod = ps.get("oversightModule", {}) or {}
    desc_mod = ps.get("descriptionModule", {}) or {}
    cond_mod = ps.get("conditionsModule", {}) or {}
    design_mod = ps.get("designModule", {}) or {}
    arms_mod = ps.get("armsInterventionsModule", {}) or {}
    outcomes_mod = ps.get("outcomesModule", {}) or {}
    elig_mod = ps.get("eligibilityModule", {}) or {}
    contacts_mod = ps.get("contactsLocationsModule", {}) or {}
    refs_mod = ps.get("referencesModule", {}) or {}
    ipd_mod = ps.get("ipdSharingStatementModule", {}) or {}
    derived = study.get("derivedSection", {}) or {}
    doc_section = study.get("documentSection", {}) or {}
    annot = study.get("annotationSection", {}) or {}

    nct_id = id_mod.get("nctId", "")

    # identification
    org_raw = id_mod.get("organization", {}) or {}
    organization = Organization(
        full_name=_s(org_raw.get("fullName")),
        **{"class": _s(org_raw.get("class"))},
    )
    secondary_id_infos = [
        SecondaryId(
            id=_s(s.get("id")),
            type=_s(s.get("type")),
            domain=_s(s.get("domain")),
        )
        for s in (id_mod.get("secondaryIdInfos") or [])
    ]

    # sponsorCollaborators
    lead_sp = spons_mod.get("leadSponsor", {}) or {}
    rp_raw = spons_mod.get("responsibleParty", {}) or {}
    responsible_party = ResponsibleParty(
        type=_s(rp_raw.get("type")),
        investigator_full_name=_s(rp_raw.get("investigatorFullName")),
        investigator_title=_s(rp_raw.get("investigatorTitle")),
        investigator_affiliation=_s(rp_raw.get("investigatorAffiliation")),
        old_name_title=_s(rp_raw.get("oldNameTitle")),
        old_organization=_s(rp_raw.get("oldOrganization")),
    )
    collaborators = [
        Collaborator(
            name=_s(c.get("name")),
            **{"class": _s(c.get("class"))},
        )
        for c in (spons_mod.get("collaborators") or [])
    ]

    # design
    di_raw = design_mod.get("designInfo", {}) or {}
    masking_raw = di_raw.get("maskingInfo", {}) or {}
    design_info = DesignInfo(
        allocation=_s(di_raw.get("allocation")),
        intervention_model=_s(di_raw.get("interventionModel")),
        intervention_model_description=_s(di_raw.get("interventionModelDescription")),
        primary_purpose=_s(di_raw.get("primaryPurpose")),
        observational_model=_s(di_raw.get("observationalModel")),
        time_perspective=_s(di_raw.get("timePerspective")),
        masking=_s(masking_raw.get("masking")),
        masking_description=_s(masking_raw.get("maskingDescription")),
        who_masked=[_s(w) for w in (masking_raw.get("whoMasked") or []) if w],
    )
    bs_raw = design_mod.get("bioSpec", {}) or {}
    bio_spec = BioSpec(
        retention=_s(bs_raw.get("retention")),
        description=_s(bs_raw.get("description")),
    )
    enrollment_info = design_mod.get("enrollmentInfo", {}) or {}
    enrollment = enrollment_info.get("count")

    # armsInterventions
    arm_groups = [
        ArmGroup(
            label=_s(a.get("label")),
            type=_s(a.get("type")),
            description=_s(a.get("description")),
            intervention_names=[_s(n) for n in (a.get("interventionNames") or []) if n],
        )
        for a in (arms_mod.get("armGroups") or [])
    ]
    interventions = [
        Intervention(
            type=_s(i.get("type")),
            name=_s(i.get("name")),
            description=_s(i.get("description")),
            arm_group_labels=[_s(l) for l in (i.get("armGroupLabels") or []) if l],
            other_names=[_s(n) for n in (i.get("otherNames") or []) if n],
        )
        for i in (arms_mod.get("interventions") or [])
    ]

    # outcomes
    def _outcomes(key):
        return [
            Outcome(
                measure=_s(o.get("measure")),
                description=_s(o.get("description")),
                time_frame=_s(o.get("timeFrame")),
            )
            for o in (outcomes_mod.get(key) or [])
        ]
    primary_outcomes = _outcomes("primaryOutcomes")
    secondary_outcomes = _outcomes("secondaryOutcomes")
    other_outcomes = _outcomes("otherOutcomes")

    # contactsLocations
    locations: list[Location] = []
    for loc in contacts_mod.get("locations") or []:
        locations.append(Location(
            facility=_s(loc.get("facility")),
            status=_s(loc.get("status")),
            city=_s(loc.get("city")),
            state=_s(loc.get("state")),
            country=_s(loc.get("country")),
            zip=_s(loc.get("zip")),
        ))

    central_contacts: list[CentralContact] = []
    for c in contacts_mod.get("centralContacts") or []:
        central_contacts.append(CentralContact(
            name=_s(c.get("name")),
            role=_s(c.get("role")),
            phone=_s(c.get("phone")),
            phone_ext=_s(c.get("phoneExt")) or None,
            email=_s(c.get("email")) or None,
        ))

    overall_officials: list[OverallOfficial] = []
    for o in contacts_mod.get("overallOfficials") or []:
        overall_officials.append(OverallOfficial(
            name=_s(o.get("name")),
            affiliation=_s(o.get("affiliation")),
            role=_s(o.get("role")),
        ))

    # references
    references = [
        Reference(
            pmid=_s(r.get("pmid")),
            type=_s(r.get("type")),
            citation=_s(r.get("citation")),
        )
        for r in (refs_mod.get("references") or [])
    ]
    see_also_links = [
        SeeAlsoLink(label=_s(l.get("label")), url=_s(l.get("url")))
        for l in (refs_mod.get("seeAlsoLinks") or [])
    ]

    # ipdSharingStatement
    ipd_sharing = IpdSharing(
        ipd_sharing=_s(ipd_mod.get("ipdSharing")),
        description=_s(ipd_mod.get("description")),
        info_types=[_s(t) for t in (ipd_mod.get("infoTypes") or []) if t],
        time_frame=_s(ipd_mod.get("timeFrame")),
        access_criteria=_s(ipd_mod.get("accessCriteria")),
        url=_s(ipd_mod.get("url")),
    )

    # derivedSection
    cond_browse = derived.get("conditionBrowseModule", {}) or {}
    interv_browse = derived.get("interventionBrowseModule", {}) or {}
    def _leaves(src):
        return [
            BrowseLeaf(
                id=_s(b.get("id")),
                name=_s(b.get("name")),
                as_found=_s(b.get("asFound")),
                relevance=_s(b.get("relevance")),
            )
            for b in (src.get("browseLeaves") or [])
        ]
    def _branches(src):
        return [
            BrowseBranch(abbrev=_s(b.get("abbrev")), name=_s(b.get("name")))
            for b in (src.get("browseBranches") or [])
        ]
    condition_browse_leaves = _leaves(cond_browse)
    condition_browse_branches = _branches(cond_browse)
    intervention_browse_leaves = _leaves(interv_browse)
    intervention_browse_branches = _branches(interv_browse)

    # documentSection
    large_doc_mod = doc_section.get("largeDocumentModule", {}) or {}
    large_docs = []
    for d in (large_doc_mod.get("largeDocs") or []):
        size_v = d.get("size")
        large_docs.append(LargeDoc(
            type_abbrev=_s(d.get("typeAbbrev")),
            has_protocol=bool(d.get("hasProtocol", False)),
            has_sap=bool(d.get("hasSap", False)),
            has_icf=bool(d.get("hasIcf", False)),
            label=_s(d.get("label")),
            date=_s(d.get("date")),
            upload_date=_s(d.get("uploadDate")),
            filename=_s(d.get("filename")),
            size=int(size_v) if isinstance(size_v, int) else None,
        ))

    # annotationSection
    unposted_raw = annot.get("unpostedAnnotation", {}) or {}
    unposted_annotation = UnpostedAnnotation(
        unposted_responsible_party=_s(unposted_raw.get("unpostedResponsibleParty")),
        unposted_events=[
            UnpostedEvent(type=_s(e.get("type")), date=_s(e.get("date")))
            for e in (unposted_raw.get("unpostedEvents") or [])
        ],
    )
    violation_raw = annot.get("violationAnnotation", {}) or {}
    violation_annotation = ViolationAnnotation(
        violation_events=[
            ViolationEvent(
                type=_s(e.get("type")),
                description=_s(e.get("description")),
                creation_date=_s(e.get("creationDate")),
                issued_date=_s(e.get("issuedDate")),
                release_date=_s(e.get("releaseDate")),
                posted_date=_s(e.get("postedDate")),
            )
            for e in (violation_raw.get("violationEvents") or [])
        ],
    )

    return Trial(
        nct_id=nct_id,
        brief_title=_s(id_mod.get("briefTitle")),
        official_title=_s(id_mod.get("officialTitle")),
        acronym=_s(id_mod.get("acronym")),
        organization=organization,
        secondary_id_infos=secondary_id_infos,
        overall_status=_s(status_mod.get("overallStatus")),
        last_known_status=_s(status_mod.get("lastKnownStatus")),
        start_date=_s((status_mod.get("startDateStruct") or {}).get("date")),
        primary_completion_date=_s((status_mod.get("primaryCompletionDateStruct") or {}).get("date")),
        last_update_post_date=_s((status_mod.get("lastUpdatePostDateStruct") or {}).get("date")),
        study_first_submit_date=_s(status_mod.get("studyFirstSubmitDate")),
        lead_sponsor_name=_s(lead_sp.get("name")),
        lead_sponsor_class=_s(lead_sp.get("class")),
        responsible_party=responsible_party,
        collaborators=collaborators,
        oversight_has_dmc=bool(over_mod.get("oversightHasDmc", False)),
        is_fda_regulated_drug=bool(over_mod.get("isFdaRegulatedDrug", False)),
        is_fda_regulated_device=bool(over_mod.get("isFdaRegulatedDevice", False)),
        is_unapproved_device=bool(over_mod.get("isUnapprovedDevice", False)),
        is_ppsd=bool(over_mod.get("isPpsd", False)),
        fdaaa_801_violation=bool(over_mod.get("fdaaa801Violation", False)),
        brief_summary=_s(desc_mod.get("briefSummary")),
        detailed_description=_s(desc_mod.get("detailedDescription")),
        conditions=[c for c in (cond_mod.get("conditions") or []) if c],
        keywords=[k for k in (cond_mod.get("keywords") or []) if k],
        study_type=_s(design_mod.get("studyType")),
        phases=list(design_mod.get("phases") or []),
        enrollment_count=int(enrollment) if isinstance(enrollment, int) else None,
        enrollment_type=_s(enrollment_info.get("type")),
        design_info=design_info,
        bio_spec=bio_spec,
        target_duration=_s(design_mod.get("targetDuration")),
        expanded_access_types=[_s(t) for t in (design_mod.get("expandedAccessTypes") or []) if t],
        arm_groups=arm_groups,
        interventions=interventions,
        primary_outcomes=primary_outcomes,
        secondary_outcomes=secondary_outcomes,
        other_outcomes=other_outcomes,
        eligibility_criteria=_s(elig_mod.get("eligibilityCriteria")),
        healthy_volunteers=bool(elig_mod.get("healthyVolunteers", False)),
        sex=_s(elig_mod.get("sex")),
        gender_description=_s(elig_mod.get("genderDescription")),
        minimum_age=_s(elig_mod.get("minimumAge")),
        maximum_age=_s(elig_mod.get("maximumAge")),
        central_contacts=central_contacts,
        overall_officials=overall_officials,
        locations=locations,
        references=references,
        see_also_links=see_also_links,
        ipd_sharing=ipd_sharing,
        condition_browse_leaves=condition_browse_leaves,
        condition_browse_branches=condition_browse_branches,
        intervention_browse_leaves=intervention_browse_leaves,
        intervention_browse_branches=intervention_browse_branches,
        large_docs=large_docs,
        unposted_annotation=unposted_annotation,
        violation_annotation=violation_annotation,
        study_url=f"https://clinicaltrials.gov/study/{nct_id}" if nct_id else "",
    )


def _parse_age_years(s: str) -> Optional[float]:
    """Parse a CT.gov eligibility-age string ('18 Years' / '6 Months' /
    '1 Day' / 'N/A') into years (float). Returns None if not parseable
    or marked 'N/A' (which means the trial does not constrain this side
    of the range)."""
    if not s:
        return None
    txt = str(s).strip().lower()
    if not txt or txt in ("n/a", "na", "none"):
        return None
    parts = txt.split()
    try:
        n = float(parts[0])
    except (ValueError, IndexError):
        return None
    unit = parts[1] if len(parts) > 1 else "year"
    if unit.startswith("year"):
        return n
    if unit.startswith("month"):
        return n / 12.0
    if unit.startswith("week"):
        return n / 52.0
    if unit.startswith("day"):
        return n / 365.0
    if unit.startswith("hour"):
        return n / (365.0 * 24)
    return n


def _age_in_range(target: int, minimum_age: str, maximum_age: str) -> bool:
    """True iff the integer target age fits inside the trial's declared
    eligibility window. Unparseable / N/A bounds are treated as open
    (always satisfied on that side)."""
    lo = _parse_age_years(minimum_age)
    hi = _parse_age_years(maximum_age)
    if lo is not None and target < lo:
        return False
    if hi is not None and target > hi:
        return False
    return True


async def _fetch_ct_gov(
    condition: str,
    page_size: int,
    cursor: Optional[str],
    age_years: Optional[int] = None,
    sex: Optional[str] = None,
    geographic_scope: Optional[str] = "international",
) -> tuple[list[Trial], Optional[str], Optional[int]]:
    params: dict[str, Any] = {
        "query.cond": condition,
        "filter.overallStatus": "RECRUITING",
        "pageSize": page_size,
        "format": "json",
        # countTotal returns the total recruiting count alongside the
        # paged studies so the wrapper can show "Showing X to Y of Z".
        "countTotal": "true",
    }
    # CT.gov v2 supports demographic refinement via aggFilters when the
    # participant's age and sex are known. Both are optional; condition
    # remains the only minimum required by the tool contract.
    agg_parts: list[str] = []
    if isinstance(age_years, int):
        if age_years < 18:
            agg_parts.append("ages:child")
        elif age_years < 65:
            agg_parts.append("ages:adult")
        else:
            agg_parts.append("ages:older")
    if sex:
        s_lower = sex.strip().lower()
        if s_lower in ("male", "m"):
            agg_parts.append("sex:m")
        elif s_lower in ("female", "f"):
            agg_parts.append("sex:f")
    if agg_parts:
        params["aggFilters"] = ",".join(agg_parts)
    # EPIC-006-F-005-S-001-REQ-B-074: geographic_scope='us' restricts to
    # country=United States via CT.gov's locStr-style filter; 'international'
    # (the default) applies NO geo filter so every record is in scope.
    if (geographic_scope or "").strip().lower() == "us":
        params["query.locn"] = "United States"
    if cursor:
        params["pageToken"] = cursor
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(CT_GOV_URL, params=params)
        r.raise_for_status()
        payload = r.json()
    studies = payload.get("studies") or []
    next_token = payload.get("nextPageToken")
    total_count_raw = payload.get("totalCount")
    total_count = int(total_count_raw) if isinstance(total_count_raw, int) else None
    return [_parse_trial(s) for s in studies], next_token, total_count


class ClinicalTrialsTool(ChatHealthyTool):
    """EPIC-006-F-005 — Find Clinical Trials."""
    TOOL_NAME = "clinical_trials"
    Request = Request
    Response = Response

    async def run(self, deps: AgentDeps, request: "Request") -> "Response":
        try:
            return await self._run_inner(deps, request)
        except ChatHealthyException:
            raise
        except Exception as exc:
            # The failure is named where it happened, so the panel can say
            # what was being attempted rather than show a status code. A
            # request carrying a cursor was extending a list that already
            # exists; one without was producing a list for a condition.
            mode = ("clinical_trials_list_not_extended" if request.cursor
                    else "clinical_trials_list_not_produced")
            raise ChatHealthyException(
                mode=mode,
                component="ClinicalTrialsTool",
                message=(f"clinical trials registry call failed for condition "
                         f"{(request.condition or '').strip()!r}: "
                         f"{type(exc).__name__}: {exc}"),
                exception=exc,
            )

    async def _run_inner(self, deps: AgentDeps, request: "Request") -> "Response":
        # Echo the actual params we'll use so the client can re-issue
        # pagination requests against the same context.
        search_context = SearchContext(
            condition=(request.condition or "").strip(),
            age_years=request.age_years,
            sex=request.sex,
            geographic_scope=(request.geographic_scope or "international"),
        )
        condition = search_context.condition
        if not condition:
            # Mode 2 (REQ-B-008): precondition failure surfaced inline as
            # Response.error. NOT 503; no fatal_error tag.
            log.error("clinical_trials precondition failed: no condition provided",
                      exc=ChatHealthyException(
                          mode="clinical_trials_no_condition",
                          message="clinical_trials precondition failed: no condition provided",
                          component="ClinicalTrialsTool",
                      ), if_not_debug_log=True)
            result = Response(
                trials=[],
                error="No condition provided.",
                search_context=search_context,
            )
            deps.stream({
                "kind": "clinical_trials_chunk",
                "data": {
                    "trials": [],
                    "chunk_index": 0,
            # This chunk begins a set rather than adding to one,
            # so whatever is showing belongs to a search that is
            # over. Said here because the tool knows it; a reader
            # comparing an index to zero is inferring it.
            "starts_new_set": True,
                    "starts_new_set": True,
                    "is_final": True,
                    "total_eligible": 0,
                    "is_partial": False,
                    "search_context": search_context.model_dump(exclude_none=True),
                    "error": "No condition provided.",
                },
            })
            return result

        # How far the registry is paged depends on whether an age was
        # given, and the two cases are different problems.
        #
        # With no age, no trial is dropped after the fetch, so the count
        # the registry returns with countTotal is exact and the tool pages
        # only as far as the person reads: one registry page per request,
        # carrying the registry's own token onward.
        #
        # With an age, the local eligibility test below removes trials the
        # registry counted, so the count must be counted rather than asked
        # for -- and the age bucket has already cut the set, which is what
        # makes paging it to exhaustion a bounded cost.
        target_age = request.age_years if isinstance(request.age_years, int) else None

        if target_age is None:
            trials, next_cursor, ct_total_count = await _fetch_ct_gov(
                condition,
                request.page_size or _CT_GOV_PAGE_SIZE,
                request.cursor,
                age_years=None,
                sex=request.sex,
                geographic_scope=request.geographic_scope,
            )
            established_count = ct_total_count
        else:
            trials = []
            next_cursor = None
            cursor: Optional[str] = None
            while True:
                page, cursor, _ = await _fetch_ct_gov(
                    condition,
                    _CT_GOV_PAGE_SIZE,
                    cursor,
                    age_years=target_age,
                    sex=request.sex,
                    geographic_scope=request.geographic_scope,
                )
                # The registry's buckets are coarser than "open to a person
                # of their age": a trial the bucket admitted and the
                # declared range excludes is dropped. This step is required
                # and not defensive.
                trials.extend(t for t in page
                              if _age_in_range(target_age, t.minimum_age, t.maximum_age))
                if not cursor:
                    break
            established_count = len(trials)

        # The count reported is the count actually established -- the
        # number the person is being shown the remainder of. The
        # registry's own total is never presented as ours where a local
        # test has removed trials it counted.
        data: dict[str, Any] = {
            "trials": [t.model_dump() for t in trials],
            "chunk_index": 0,
            "is_final": True,
            "search_context": search_context.model_dump(exclude_none=True),
            "total_eligible": established_count,
            "cursor": next_cursor,
            # What the person may still narrow by: the three refinements
            # this search offers, less those the parameters already carry
            # (EPIC-006-F-005-S-001-REQ-B-081). FindCare states which
            # exist; the utterance manager puts them into words.
            "refinements_not_used": self._refinements_not_used(request),
        }
        deps.stream({"kind": "clinical_trials_chunk", "data": data})

        return Response(
            trials=trials,
            cursor=next_cursor,
            total_count=established_count,
            page_size=request.page_size or _CT_GOV_PAGE_SIZE,
            search_context=search_context,
            refinements_not_used=data["refinements_not_used"],
        )

    @staticmethod
    def _refinements_not_used(request: "Request") -> list[str]:
        """The ways of narrowing this search the person has not yet used."""
        offered = []
        if request.age_years is None:
            offered.append("age")
        if not request.sex:
            offered.append("sex")
        if (request.geographic_scope or "international").strip().lower() != "us":
            offered.append("united_states_only")
        return offered


TOOL = ClinicalTrialsTool()
