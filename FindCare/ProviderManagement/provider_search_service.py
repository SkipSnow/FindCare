# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# FindCareService — the public interface for all FindCare business capabilities.
#
# This is a Facade (GoF: https://refactoring.guru/design-patterns/facade)
# implemented as a service class. It is the single entry point for FindCare.
# EvaluateCareFacade calls this, never internal services directly.
#
# UAT Features: 1 (Provider Search), 2 (Specialty Identification)
# Design: ARCH-001, business component: FindCare

from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
import os
from typing import Optional

from chathealthy_lib.runtime_data_collections import providers_coll, specialty_meta_coll

log = ChatHealthyLoggingService()


def primary_practice_address(p: dict) -> dict:
    """Return the first entry in practice_addresses[].

    v4 splits the single addresses[] into practice_addresses[] and
    business_address. Every element of practice_addresses IS a practice
    address, so the address_type test the old shape needed has nothing left
    to select on -- carried forward it would match nothing at all."""
    for a in p.get("practice_addresses") or []:
        if isinstance(a, dict):
            return a
    return {}


def primary_county(p: dict) -> dict:
    """Return the county sub-doc on the primary practice address."""
    return primary_practice_address(p).get("county") or {}


DEFAULT_LIMIT = 25  # F-10: raised from 10

# The registry's two kinds of enumerated entity. A page returns one of
# them, and which one is a property of the page rather than a filter a
# caller may pass.
ENTITY_TYPE_INDIVIDUAL = "1"
ENTITY_TYPE_ORGANIZATION = "2"

class FindCareService:
    """FindCare Facade — single entry point for all FindCare capabilities.

    Facade pattern (GoF): simplifies access to provider search, specialty
    identification, and provider location. EvaluateCareFacade calls this
    service, never internal components directly.

    UAT Features: 1 (Provider Search), 2 (Specialty Identification)
    Dependencies: MongoDB (read-only), SpecialtyService.

    Provider search is deterministic. It filters registry facts on indexed
    fields; it does not embed a provider and does not vector-search one.
    """

    def __init__(self, get_db_fn, env_prefix: str, specialty_service=None):
        """
        Args:
            get_db_fn: callable returning MongoDB client or None
            env_prefix: environment prefix (dev/qa/prod)
        """
        self._get_db = get_db_fn
        self._env = env_prefix
        self._specialty = specialty_service
        self._taxonomy_name_cache = {}  # code -> Display Name

    def _searchable(self, base_filter: dict, entity_type: str) -> dict:
        """The filter plus what every result is implicitly constrained to.

        The entity type the page asked for. _facet_query applied it to its
        own copy, so anything computing counts off the raw filter counted
        providers the search would not return -- the panel promised 482
        where the result held 477.

        The entity type is written from what the caller was given rather than
        as a literal, because a facility page and an individual-provider page
        are the same query over the same collection differing in this one
        clause. It is a property of the page, not a filter a caller may omit.

        Deactivation is not filtered on. `active` is a history of
        deactivation and reactivation events, not a flag: a record carries
        no field at all, or an empty history, or events. The clause here
        tested for the field's absence, which hid 153 records with an empty
        history and 18552 whose history ends in a reactivation -- every one
        of them active. Not one record in the collection ends its history
        deactivated, so the clause excluded 18705 live providers and no
        dead ones. Whether a provider is currently active is a fact the
        load can derive once into a boolean and index; asking it here means
        reading the last element of an array, which no index can serve.
        """
        out = dict(base_filter)
        out["entity_type_code"] = entity_type
        return out

    # What NPPES appends to a county name. Louisiana has parishes, Alaska
    # boroughs, municipalities and census areas, Puerto Rico municipios,
    # Virginia and Maryland independent cities. Taken from the 2,104 distinct
    # names in the collection: no name carries any of these words other than
    # as its last, so stripping one can never eat part of a real name. 288
    # carry none at all, which is why the bare form is asked for too.
    COUNTY_TYPE_WORDS = ("County", "Parish", "Municipio", "city", "Borough",
                         "Area", "Region", "Municipality", "District")

    def _county_clause(self, county: str) -> dict:
        """Every stored form of one county name, as equality.

        The parameter is named county, so the kind-word is the field and not
        the value: the place is Los Angeles whether or not the person said
        'county'. The data appends the kind-word and which one depends on the
        state, so the value the model writes can never be the value the data
        holds. Code closes that here rather than asking the model to spell a
        stored value -- which is what made one sentence ask three different
        questions: 2,237 providers one run, the whole of California the next,
        nothing the run after.

        $in is a seek per value on the second key of
        idx_practice_state_county, so asking for every form costs what asking
        for one did.
        """
        bare = (county or "").strip()
        for word in self.COUNTY_TYPE_WORDS:
            suffix = " " + word
            if bare.endswith(suffix):
                bare = bare[:-len(suffix)]
                break
        return {"$in": [bare] + [f"{bare} {w}" for w in self.COUNTY_TYPE_WORDS]}

    def _name_filter(self, last: str = "", first: str = "", middle: str = "") -> dict:
        """Match a provider named outright.

        last is exact. first and middle match the name OR the bare initial,
        in either direction: a record may hold JAMES where the person typed
        J, or hold J where they typed JAMES, and a prefix only ever resolves
        the first of those. $in on the second and third keys of
        idx_provider_name is a seek per value rather than a scan.

        Everything is uppercased because NPPES stores names that way, and
        because idx_provider_name carries no collation: a case-insensitive
        comparison against it cannot use it, and every name search becomes
        a scan of 9.3M documents. That is a property of the index, not of
        case-insensitivity -- the city comparison is case-insensitive and
        indexed, against idx_practice_state_city_ci which was built with
        the same collation the query carries. A name search could have the
        same, at the cost of a second index on three fields.
        """
        clause: dict = {}
        last = (last or "").strip().upper()
        if last:
            clause["provider_last_name_legal_name"] = last
        for field, value in (("provider_first_name", first),
                             ("provider_middle_name", middle)):
            value = (value or "").strip().upper()
            if not value:
                continue
            clause[field] = value if len(value) == 1 else {"$in": [value, value[0]]}
        return clause

    def _facility_name_filter(self, name: str = "") -> dict:
        """A facility named outright.

        A person names an organization with one string and the registry
        holds two names for it -- the legal business name and the other
        organization name -- so the clause is a disjunction over both:
        matching either is matching (EPIC-006-F-006-S-001-REQ-B-002).

        "Exactly those" makes this a filter and not a ranking. The result
        set is defined by the predicate, so no score orders it and nothing
        is admitted for being close. Uppercased because NPPES stores names
        that way; a case-insensitive match would make the index unusable.
        """
        wanted = (name or "").strip().upper()
        if not wanted:
            return {}
        return {"$or": [
            {"provider_organization_name_legal_business_name": wanted},
            {"provider_other_organization_name": wanted},
        ]}

    def _administrator_name_filter(self, last: str = "", first: str = "",
                                   middle: str = "") -> dict:
        """The facility found by naming the person who administers it.

        The record carries the authorized official's last, first and middle
        names as separate fields. The person may give one name or several,
        in either order, and is not required to say which is which: the
        utterance manager decomposes what they said into the same
        structured shape it already produces for a care giver's name, and
        each part matches either the corresponding field or its initial
        (EPIC-006-F-006-S-001-REQ-B-008).

        A structured match on registry fields, not a text search.
        """
        clause: dict = {}
        for field, value in (
                ("authorized_official_last_name", last),
                ("authorized_official_first_name", first),
                ("authorized_official_middle_name", middle)):
            value = (value or "").strip().upper()
            if not value:
                continue
            clause[field] = (value if len(value) == 1
                             else {"$in": [value, value[0]]})
        return clause

    def _preference_filter(self, provider_sex: str = "",
                           sole_proprietor=None, insurance: str = "") -> dict:
        """Narrowings the person asked for, applied to an already-narrow set.

        None of these is indexed and none should be: each has a handful of
        values, so as a leading key it selects nothing. They refine a result
        specialty and geography have already cut to hundreds.

        A stated sex preference excludes everyone who does not match it,
        including X (neither male nor female, an affirmation) and U
        (undisclosed, a refusal to stipulate). Those two are never merged.
        """
        clause: dict = {}
        sex = (provider_sex or "").strip().upper()
        if sex:
            clause["provider_sex_code"] = sex
        if sole_proprietor is not None:
            clause["is_sole_proprietor"] = "Y" if sole_proprietor else "N"
        payer = (insurance or "").strip().upper()
        if payer:
            # Which payers a provider registered identifiers with. NOT
            # network participation -- nothing in NPPES establishes that.
            clause["insurance"] = {"$elemMatch": {"payer_name": payer}}
        return clause

    # The city as registrants typed it: 'LONG BEACH' for most, 'Long Beach'
    # for about one in ten. Matching on the value alone loses whichever
    # casing the search did not guess, so every query that filters on an
    # address carries this, and idx_practice_state_city_ci is built with the
    # same strength so the comparison still uses an index.
    ADDRESS_COLLATION = {"locale": "en", "strength": 2}

    def _practice_address_filter(self, state: str = "", city: str = "",
                                  county: str = "", zip: str = "") -> dict:
        """Build {"addresses": {"$elemMatch": ...}} for the practice address
        using deterministic priority (most-specific wins) with exact-match
        equality only — no regex:
          1. zip alone (most specific)
          2. county + state (per the operator's named ranking)
          3. city + state
          4. state alone
          5. nothing → {} (no location scoping)

        State is uppercased; city is uppercased to match the data shape
        (NPPES practice addresses store city in uppercase). County is
        passed through verbatim — the LLM's prompt instructs Title Case
        with the literal 'County' suffix, matching the data shape."""
        s = (state or "").strip().upper()
        c = (city or "").strip()
        co = (county or "").strip()
        # Practice addresses store zip as 5 digits; truncate the incoming
        # value so a ZIP+4 emission from the LLM still matches.
        z = (zip or "").strip()[:5]
        elem: dict = {}
        if z:
            elem["zip"] = z
        elif s and co:
            elem["state"] = s
            elem["county.name"] = self._county_clause(co)
        elif s and c:
            elem["state"] = s
            elem["city"] = c
        elif s:
            elem["state"] = s
        else:
            return {}
        return {"practice_addresses": {"$elemMatch": elem}}

    def _cache_taxonomy_names(self, options: list) -> None:
        """Hold code -> display name for the codes the filter offered.

        The row's chosen-specialty label is resolved server-side because the
        server holds both the provider's taxonomy list and the chosen set;
        the client holds only one of them.
        """
        for option in options or []:
            code = option.get("code")
            name = option.get("name")
            if code and name:
                self._taxonomy_name_cache[code] = name

    def _matched_specialty(self, p: dict, selected_specialty_codes: list) -> tuple:
        """The chosen code that admitted this provider, and its label.

        A provider holds several taxonomies and the query admitted them on
        the intersection of their taxonomy set with the chosen codes, so the
        specialty shown is the first of the provider's taxonomies present in
        the chosen set -- not another specialty that provider holds
        (EPIC-006-F-001-S-004-REQ-B-001).
        """
        chosen = set(selected_specialty_codes or [])
        if not chosen:
            return "", ""
        for t in p.get("taxonomies") or []:
            code = t.get("code") if isinstance(t, dict) else None
            if code and code in chosen:
                return code, self._taxonomy_name_cache.get(code, "")
        return "", ""

    def _format_provider(self, p: dict, selected_specialty_codes: list = None) -> dict:
        if p.get("entity_type_code") == "1":
            parts = [
                p.get("provider_name_prefix_text"), p.get("provider_first_name"),
                p.get("provider_middle_name"), p.get("provider_last_name_legal_name"),
                p.get("provider_name_suffix_text"),
            ]
            name = " ".join(x for x in parts if x)
            if p.get("provider_credential_text"):
                name += f", {p['provider_credential_text']}"
        else:
            name = p.get("provider_organization_name_legal_business_name") or "Unknown Organization"
        addr = primary_practice_address(p)
        address = ", ".join(x for x in [addr.get("line1"), addr.get("city"), addr.get("state"), addr.get("zip")] if x)
        primary_code = p.get("primary_taxonomy_code")
        primary = next((t for t in p.get("taxonomies", [])
                        if t.get("code") == primary_code), None) if primary_code else None
        county_obj = primary_county(p)
        county_name = county_obj.get("name") or ""
        raw_phone = addr.get("phone", "")
        phone = f"({raw_phone[:3]}) {raw_phone[3:6]}-{raw_phone[6:]}" if len(raw_phone) == 10 else raw_phone
        matched_code, matched_label = self._matched_specialty(p, selected_specialty_codes)
        return {
            "name": name,
            "npi": p.get("npi", ""),
            "taxonomy_code": primary.get("code", "") if primary else "",
            "matched_specialty_code": matched_code,
            "matched_specialty_label": matched_label,
            # Every taxonomy this provider holds, not only the primary.
            # A record that travels answers questions about itself where
            # it lands; without this a receiver had to reopen the
            # collection to recover what it had just been sent (C-26).
            "taxonomy_codes": [t.get("code") for t in (p.get("taxonomies") or [])
                               if isinstance(t, dict) and t.get("code")],
            "address": address,
            "phone": phone,
            "county": county_name,
            "lat": addr.get("lat"),
            "lng": addr.get("lng"),
        }

    def _format_facility(self, p: dict, _unused=None) -> dict:
        """The facility row: exactly five things and nothing else.

        EPIC-006-F-006-S-002-REQ-B-007 fixes the row exactly, so the
        projection is the enforcement point rather than the renderer -- a
        renderer that is sent a sixth field is one edit away from painting
        it.

        The facility is its legal business name. The address count is the
        length of the list the record already carries. The facility type is
        the label of the primary taxonomy, resolved server-side against the
        specialty catalogue for the same reason the chosen specialty is:
        the server holds the catalogue. The link to the detail is the NPI,
        which is what the detail is opened by.
        """
        addresses = [a for a in (p.get("practice_addresses") or [])
                     if isinstance(a, dict)]
        primary = primary_practice_address(p)
        address = ", ".join(x for x in (primary.get("line1"), primary.get("city"),
                                        primary.get("state"), primary.get("zip")) if x)
        code = p.get("primary_taxonomy_code") or ""
        return {
            "facility": p.get("provider_organization_name_legal_business_name") or "",
            "practice_address_count": len(addresses),
            "primary_practice_address": address,
            "facility_type": self._taxonomy_name_cache.get(code, ""),
            "npi": p.get("npi", ""),
            "_primary_taxonomy_code": code,
        }

    @staticmethod
    def _format_selected_facility(p: dict) -> dict:
        """A row of the selected-facility set: the legal business name and
        the NPI, and nothing else (EPIC-006-F-006-S-003-REQ-B-004).

        A second, narrower projection than the result row, because the
        selected row is a reminder of what was chosen rather than a second
        result row.
        """
        return {
            "facility": p.get("provider_organization_name_legal_business_name") or "",
            "npi": p.get("npi", ""),
        }

    def _facet_query(self, collection, base_filter: dict, cursor: str, safe_limit: int,
                     entity_type: str, direction: str,
                     selected_specialty_codes: list = None,
                     row_formatter=None) -> tuple:
        """Single-query count + page using $facet. Returns (providers, total_count).

        The order is the NPI, which is unique, so it is total and the same
        search asked twice returns the same records in the same order
        (EPIC-006-F-008-S-007-REQ-B-003). The position is therefore a key in
        that order rather than an offset.

        Forward takes the rows greater than the key in ascending order. Back
        takes the rows less than it in descending order and reverses the page
        before the row projection runs, so a backward page costs exactly what
        a forward page costs and no history of the cursors walked is needed
        (EPIC-006-F-008-S-007-REQ-B-001). An absent key is the top of the
        list, which is what a page never visited means (REQ-B-004).

        Every returned row is asserted to be the entity type the page asked
        for; a row that is not is a data-integrity bug and we fail hard.
        """
        base_filter = self._searchable(base_filter, entity_type)
        query_filter = dict(base_filter)
        backward = direction == "back"
        if cursor:
            query_filter["npi"] = {"$lt": cursor} if backward else {"$gt": cursor}
        sort_order = -1 if backward else 1
        pipeline = [
            {"$match": query_filter},
            {"$facet": {
                "count": [{"$match": base_filter}, {"$count": "total"}] if not cursor
                          else [{"$count": "total"}],
                "page": [{"$sort": {"npi": sort_order}}] +
                         ([{"$limit": safe_limit}] if safe_limit > 0 else []) +
                         [{"$project": self._PROJECTION}],
            }},
        ]
        # For the count facet, we need the full base_filter count (not cursor
        # filtered). So we run count separately only when paginating.
        if cursor:
            total_count = collection.count_documents(
                base_filter, collation=self.ADDRESS_COLLATION)
            result = list(collection.aggregate([
                {"$match": query_filter},
                {"$sort": {"npi": sort_order}},
            ] + ([{"$limit": safe_limit}] if safe_limit > 0 else []) + [
                {"$project": self._PROJECTION},
            ], collation=self.ADDRESS_COLLATION))
            if backward:
                result.reverse()
            self._assert_entity_type(result, entity_type)
            project = row_formatter or self._format_provider
            return [project(p, selected_specialty_codes)
                    for p in result], total_count

        result = list(collection.aggregate(
            pipeline, collation=self.ADDRESS_COLLATION))
        if not result:
            return [], 0
        total_count = result[0]["count"][0]["total"] if result[0]["count"] else 0
        page = result[0]["page"]
        if backward:
            page = list(reversed(page))
        self._assert_entity_type(page, entity_type)
        project = row_formatter or self._format_provider
        providers = [project(p, selected_specialty_codes) for p in page]
        return providers, total_count

    def _assert_entity_type(self, docs, entity_type: str) -> None:
        """Fail-hard: every returned row MUST be the entity type the page
        asked for. A facility search returning a person is as much a
        violation as a provider search returning an organization, so the
        guard runs in both directions. A row that is not the asked-for type
        indicates a data-integrity bug; the app must abend so the operator
        sees it.
        """
        for p in docs:
            etc = p.get("entity_type_code")
            if etc != entity_type:
                raise ChatHealthyException(
                    mode="compliance_violation",
                    component="provider_search_service",
                    message="entity-type violation: a row of a different entity type was "
                    f"returned by /search (npi={p.get('npi')!r}, "
                    f"entity_type_code={etc!r}, asked for {entity_type!r}). "
                    "Data-integrity bug — fail-hard per spec."
                )

    _PROJECTION = {
        "_id": 0, "npi": 1, "entity_type_code": 1,
        "provider_first_name": 1, "provider_last_name_legal_name": 1,
        "provider_middle_name": 1, "provider_name_prefix_text": 1,
        "provider_name_suffix_text": 1, "provider_credential_text": 1,
        "provider_organization_name_legal_business_name": 1,
        "practice_addresses": 1, "business_address": 1, "taxonomies": 1,
        "primary_taxonomy_code": 1,
    }

    @staticmethod
    def _build_summary_message(has_more: bool, total_count: int, page_count: int,
                               specialty_searched: str = "",
                               specialization_options: list = None,
                               noun: str = "", **kwargs) -> str:
        """Build a system-generated summary message per GOV-011 / FC-RESULT-MSG.

        The system builds this message from structured data — the LLM does not write it.
        """
        if not has_more:
            return ""
        remaining = total_count - page_count
        # The words the person searched with. One construction serves both
        # pages; the page supplies its own noun for the case where the
        # person named nothing to search with
        # (EPIC-006-F-006-S-004-REQ-B-001).
        search_term = specialty_searched or noun or "results"
        spec_count = len(specialization_options) if specialization_options else 0
        # Build location narrowing options — offer exactly those dimensions
        # the geography does not already carry, the postal code included.
        # Appending zipcode unconditionally offered a person who searched by
        # postal code the postal code back
        # (EPIC-006-F-001-S-005-REQ-B-008, EPIC-006-F-006-S-004-REQ-B-004).
        state = kwargs.get("state", "")
        city = kwargs.get("city", "")
        county = kwargs.get("county", "")
        zip = kwargs.get("zip", "")
        narrow_options = []
        if not city:
            narrow_options.append("city")
        if not county:
            narrow_options.append("county")
        if not zip:
            narrow_options.append("zipcode")

        parts = [f"There are {remaining:,} more '{search_term}'"]
        if state:
            parts.append(f" in '{state}'")
        parts.append(". ")
        if spec_count > 0:
            parts.append(f"There are {spec_count} types of providers. ")
        parts.append(f"Shall I show you [more '{search_term}'](#action:next-page)")
        if spec_count > 0:
            parts.append(f" or would you like to [filter '{search_term}' by provider type](#action:filter)?")
        else:
            parts.append("?")
        if narrow_options:
            parts.append(f" You can also search by {', '.join(narrow_options)}.")
        return "".join(parts)

    # What a person may still narrow by, and what each would cost them.
    #
    # Computed over the result specialty and geography have already cut to
    # hundreds, which is why none of these fields is indexed: as a leading
    # key each selects almost nothing, and here they are nearly free.
    #
    # The counts are the point. A sex preference costs 90% of the list for
    # orthopaedic surgery and 10% for nurse practitioners, and a person
    # should see that before choosing rather than after.
    SEX_LABELS = {"F": "Female", "M": "Male",
                  "X": "Neither Male nor Female", "U": "Undisclosed"}

    def _refinements(self, collection, base_filter: dict, already: dict,
                     entity_type: str) -> dict:
        """Per-choice counts for the dimensions not yet chosen."""
        # Every dimension is offered, including one already in force. Hiding
        # a chosen dimension left the person no way to undo it: the toggle
        # existed on the server and nothing on screen could reach it. A
        # filter you cannot remove is a trap.
        facets: dict = {}
        facets["sex"] = [{"$group": {"_id": "$provider_sex_code",
                                     "n": {"$sum": 1}}}]
        facets["sole_proprietor"] = [{"$group": {"_id": "$is_sole_proprietor",
                                                 "n": {"$sum": 1}}}]
        # Count PROVIDERS, not identifier rows. A provider registered with
        # one payer across several states carries several entries, so a
        # plain unwind reported 108 where 97 providers qualify -- an
        # overcount on the one screen whose job is to tell the truth about
        # what a choice costs.
        facets["insurance"] = [
            {"$unwind": "$insurance"},
            {"$group": {"_id": {"payer": "$insurance.payer_name",
                                "npi": "$npi"}}},
            {"$group": {"_id": "$_id.payer", "n": {"$sum": 1}}},
            {"$sort": {"n": -1}}, {"$limit": 8}]
        if not facets:
            return {}
        try:
            raw = next(iter(collection.aggregate(
                [{"$match": self._searchable(base_filter, entity_type)},
                 {"$facet": facets}], allowDiskUse=True,
                collation=self.ADDRESS_COLLATION)))
        except Exception as exc:
            # Mode 1: refinement counts are an aid, not the answer. A
            # failure here leaves the panel without hints and the result
            # itself untouched.
            log.info("refinement counts unavailable: %s", exc,
                     exc=ChatHealthyException(
                         mode="refinement_counts_unavailable",
                         message=f"refinement counts unavailable: {exc}",
                         component="ProviderSearchService", exception=exc))
            return {}

        out: dict = {}
        if "sex" in raw:
            chosen = (already.get("provider_sex") or "").upper()
            rows = [{"value": r["_id"], "label": self.SEX_LABELS.get(r["_id"], r["_id"]),
                     "count": r["n"], "in_force": r["_id"] == chosen}
                    for r in raw["sex"] if r["_id"]]
            if len(rows) > 1 or chosen:
                out["provider_sex"] = sorted(rows, key=lambda r: -r["count"])
        if "sole_proprietor" in raw:
            # X is "Not Answered". Dropping it made the counts fail to sum
            # to the result total, which on a panel of costs reads as an
            # arithmetic error rather than as a provider who said nothing.
            labels = {"Y": "Yes", "N": "No", "X": "Not answered"}
            chosen_sole = already.get("sole_proprietor")
            in_force = None if chosen_sole is None else ("Y" if chosen_sole else "N")
            rows = [{"value": r["_id"], "label": labels.get(r["_id"], r["_id"]),
                     "count": r["n"], "in_force": r["_id"] == in_force}
                    for r in raw["sole_proprietor"] if r["_id"]]
            if len([r for r in rows if r["value"] in ("Y", "N")]) > 1 or in_force:
                out["sole_proprietor"] = sorted(rows, key=lambda r: -r["count"])
        if "insurance" in raw:
            chosen_ins = (already.get("insurance") or "").upper()
            rows = [{"value": r["_id"], "count": r["n"],
                     "in_force": r["_id"] == chosen_ins}
                    for r in raw["insurance"] if r["_id"]]
            if rows:
                out["insurance"] = rows
        return out

    def _paginated_result(self, providers: list, search_mode: str, safe_limit: int,
                          entity_type: str,
                          search_params: dict = None, total_count: int = 0,
                          page_start: int = 1, collection=None,
                          base_filter: dict = None, chosen: dict = None,
                          **extra) -> dict:
        """Build a result dict with pagination metadata and refinements.

        The refinements are computed HERE rather than at each route. Wired
        into one route only, four others returned providers with no way to
        narrow them -- including the county fallback -- and the panel went
        silent on exactly the searches that return the most.
        """
        first_npi = providers[0]["npi"] if providers else ""
        last_npi = providers[-1]["npi"] if providers else ""
        has_more = len(providers) == safe_limit and safe_limit > 0
        page_end = page_start + len(providers) - 1 if providers else 0
        summary_message = self._build_summary_message(
            has_more, total_count, len(providers),
            specialty_searched=extra.get("specialty_searched", ""),
            specialization_options=extra.get("specialization_options"),
            noun=extra.get("noun", ""),
            state=extra.get("state", ""),
            city=(search_params or {}).get("city", ""),
            county=(search_params or {}).get("county", ""),
            zip=(search_params or {}).get("zip", ""),
        )
        result = {
            "supported": True,
            "search_mode": search_mode,
            "refinements": (
                self._refinements(collection, base_filter, chosen or {}, entity_type)
                if collection is not None and base_filter else {}),
            "count": len(providers),
            "total_count": total_count,
            "providers": providers,
            "first_npi": first_npi,
            "last_npi": last_npi,
            "has_more": has_more,
            # Whether a page precedes this one. The widget had no way to
            # know and used first_npi, which is the first row of whatever
            # page it is looking at -- so a Previous control appeared on
            # the first page, offering to go back to nothing.
            "has_previous": page_start > 1,
            "page_start": page_start,
            "page_end": page_end,
            "search_params": search_params or {},
            "summary_message": summary_message,
        }
        result.update(extra)
        return result

    def search_providers(self, entity_type: str,
                         specialty_query: str = "", state: str = "", city: str = "",
                         county: str = "", zip: str = "", limit: int = 25, npi: str = "",
                         npis: list = None, name: str = "",
                         nucc_codes: list[str] = None,
                         cursor: str = "", direction: str = "forward",
                         last_name: str = "", first_name: str = "", middle_name: str = "",
                         provider_sex: str = "", sole_proprietor=None, insurance: str = "",
                         facility_name: str = "",
                         administrator_last_name: str = "",
                         administrator_first_name: str = "",
                         administrator_middle_name: str = "",
                         find_specialty_fn=None) -> dict:
        """Search for providers. Main entry point.

        Facade pattern (GoF) — routes to the right search strategy based on args.

        Args:
            entity_type: the entity type the page returns — a property of the
                page the request was dispatched to, not a filter the caller
                may omit. There is no default to fall through to.
            specialty_query: natural-language specialty intent; resolved by the
                LLM tool to nucc_codes (no fuzzy matching anywhere).
            state: two-letter state code
            city: optional city filter
            county: optional county filter
            zip: optional ZIP code filter (5 digits)
            limit: max results
            npi: exact NPI lookup
            name: provider name search
            nucc_codes: list of exact NUCC taxonomy codes to filter by
            cursor: keyset position — the NPI the page is taken relative to.
                Absent means the first page.
            direction: forward for the rows after the cursor, back for those
                before it.
            facility_name: an organization named outright, matched against
                its legal business name or its other organization name.
            administrator_last_name / _first_name / _middle_name: the person
                who administers the facility, decomposed by the utterance
                manager into the same shape a care giver's name takes.
            find_specialty_fn: callable for taxonomy fallback (injected to avoid circular dep)
        """
        chosen = {"provider_sex": provider_sex, "sole_proprietor": sole_proprietor,
                  "insurance": insurance}

        # Backwards-compat parameter name (the old `specialty_codes` is now `nucc_codes`).
        specialty_codes = nucc_codes
        state_upper = state.upper().strip() if state else ""

        # Build search_params for pagination replay
        _search_params = {}
        if specialty_query: _search_params["specialty_query"] = specialty_query
        if state_upper: _search_params["state"] = state_upper
        if city: _search_params["city"] = city
        if county: _search_params["county"] = county
        if zip: _search_params["zip"] = zip
        if name: _search_params["name"] = name
        if specialty_codes: _search_params["nucc_codes"] = specialty_codes

        db = self._get_db()
        if db is None:
            return {"error": "Database unavailable"}

        safe_limit = int(limit)
        collection = providers_coll()

        # ── Facility route: the page returns organizations ──
        # Reached only when the page dispatched to is the facility page.
        # It shares _searchable, _practice_address_filter and _facet_query
        # with the care-giver routes and diverges only in the clauses a
        # facility is named by and in the row it projects.
        if entity_type == ENTITY_TYPE_ORGANIZATION and not npi:
            base_filter = self._practice_address_filter(
                state=state_upper, city=city, county=county, zip=zip)
            named = self._facility_name_filter(facility_name)
            if named:
                base_filter.update(named)
            administered_by = self._administrator_name_filter(
                administrator_last_name, administrator_first_name,
                administrator_middle_name)
            if administered_by:
                base_filter.update(administered_by)
            if specialty_codes:
                # At least one, not all -- identical in form to the
                # care-giver taxonomy clause.
                base_filter["taxonomies.code"] = {"$in": specialty_codes}

            # The catalogue's non-individual section, which is where a
            # facility type's label lives. Loaded before the query because
            # the row projection reads the name cache this fills.
            facility_types = self._facility_type_options(base_filter, specialty_codes)
            self._cache_taxonomy_names(facility_types)

            facilities, total_count = self._facet_query(
                collection, base_filter, cursor, safe_limit, entity_type,
                direction, specialty_codes,
                row_formatter=self._format_facility)
            # A row shows the label of ITS primary taxonomy
            # (EPIC-006-F-006-S-001... see S-002-REQ-B-007), which is not
            # always one of the codes the search named: a facility carrying
            # several taxonomies matches on one and is primary in another.
            # Loading labels only for the searched codes left those rows
            # showing an empty type, and a different set of rows each run.
            self._label_rows_by_own_taxonomy(facilities)
            log.info("search: facility route returned %d of %d in %s",
                     len(facilities), total_count, state_upper or "all")
            return self._paginated_result(
                facilities, "facility", safe_limit, entity_type,
                search_params=_search_params, total_count=total_count,
                collection=collection, base_filter=base_filter, chosen=chosen,
                state=state_upper,
                specialty_searched=facility_name,
                noun="facilities",
                specialization_options=facility_types)

        # ── Route 1: lookup by identity ──
        # An array is the general shape and a single npi is a case of it,
        # so both arrive here and one query serves them (C-26).
        wanted_npis = [n for n in (list(npis) if npis else []) if n]
        if npi and npi not in wanted_npis:
            wanted_npis.append(npi)
        if len(wanted_npis) > 1:
            found = list(collection.find(
                {"npi": {"$in": wanted_npis}, "entity_type_code": entity_type},
                self._PROJECTION))
            self._assert_entity_type(found, entity_type)
            return {"supported": True, "search_mode": "npis",
                    "count": len(found),
                    "providers": [self._format_provider(f, specialty_codes)
                                  for f in found]}
        if npi:
            # The page's entity type at the query level, so an organization
            # NPI on the provider page returns "no provider found" and the
            # same lookup on the facility page finds the organization.
            # Asserted post-fetch as a fail-hard guard against mislabeled
            # records.
            provider = collection.find_one({"npi": npi, "entity_type_code": entity_type},
                                           self._PROJECTION)
            if provider:
                self._assert_entity_type([provider], entity_type)
                return {"supported": True, "search_mode": "npi", "count": 1,
                        "providers": [self._format_provider(provider, specialty_codes)]}
            return {"supported": True, "providers": [], "message": f"No provider found for NPI {npi}."}

        # ── Route 2: Name search ──
        # A structured name uses idx_provider_name. The legacy free-text
        # `name` argument does not: it matched with an unanchored,
        # case-insensitive regex across three fields, which no index can
        # serve, so every such search scanned the whole collection.
        named = self._name_filter(last_name, first_name, middle_name)
        if named:
            base_filter = self._practice_address_filter(state=state_upper, city=city, county=county, zip=zip)
            base_filter.update(named)
            base_filter.update(self._preference_filter(provider_sex, sole_proprietor, insurance))
            providers, total_count = self._facet_query(
                collection, base_filter, cursor, safe_limit, entity_type, direction,
                specialty_codes)
            return self._paginated_result(providers, "name", safe_limit, entity_type,
                                          search_params=_search_params,
                                          total_count=total_count,
                                          collection=collection, base_filter=base_filter,
                                          chosen=chosen)

        # ── Route 3: Specialty codes direct filter ──
        if specialty_codes:
            base_filter = {"taxonomies.code": {"$in": specialty_codes}}
            base_filter.update(self._practice_address_filter(state=state_upper, city=city, county=county, zip=zip))
            base_filter.update(self._preference_filter(provider_sex, sole_proprietor, insurance))

            # Look up selected specialty names for the summary, and for the
            # per-row chosen-specialty label. Loaded before the query runs
            # because the row projection reads the name cache this fills.
            specialization_options = []
            try:
                meta_coll = specialty_meta_coll()
                for doc in meta_coll.find({"Code": {"$in": specialty_codes}},
                                           {"Code": 1, "Display Name": 1, "can_prescribe": 1, "homeopathic": 1, "_id": 0}):
                    specialization_options.append({
                        "code": doc.get("Code", ""),
                        "name": doc.get("Display Name", ""),
                        "can_prescribe": doc.get("can_prescribe", False),
                        "homeopathic": doc.get("homeopathic", False),
                    })
            except Exception as _exc:
                # Mode 1 (REQ-B-008): specialty Display Names for the
                # summary are nice-to-have decoration; falls back to
                # specialization_options=[] and the summary message
                # degrades gracefully without names.
                log.info("specialty_meta options load failed (ignored): %s", _exc, exc=ChatHealthyException(
                                                                                       mode="specialty_meta_options_load_failed",
                                                                                       message=f"specialty_meta options load failed (ignored): {_exc}",
                                                                                       component="FindCareService",
                                                                                       exception=_exc,
                                                                                   ))
            self._cache_taxonomy_names(specialization_options)

            providers, total_count = self._facet_query(
                collection, base_filter, cursor, safe_limit, entity_type, direction,
                specialty_codes)
            log.info("search: specialty_codes returned %d for %d codes in %s",
                       len(providers), len(specialty_codes), state_upper or "all")

            # EPIC-006-F-003-S-002: the specialty step runs once when the
            # list is offered. Apply Filter MUST be a parameterized DB
            # query only — no further LLM calls.

            # What the person searched for, in the person's own words
            # (EPIC-006-F-001-S-005-REQ-B-001). Naming the resolved
            # taxonomies instead put all nineteen labels in the summary, and
            # the summary names the term three times, so a search for "a
            # shrink" answered with fifty-seven label names and pushed the
            # providers off the screen. The labels are what the panel is
            # for; the summary is about what was asked.
            filtered_term = (specialty_query
                             or f"{len(specialty_codes)} selected specialties")

            return self._paginated_result(providers, "specialty_codes", safe_limit, entity_type,
                                          search_params=_search_params, total_count=total_count,
                                          state=state_upper, specialty_searched=filtered_term,
                                          specialization_options=specialization_options,
                                          collection=collection, base_filter=base_filter,
                                          chosen=chosen,
                                          codes_searched=len(specialty_codes))

        # ── Route 4: Specialty query (vector resolves codes → taxonomy returns data) ──
        if specialty_query:
            codes = []

            # EPIC-006-F-003-S-001: resolve codes via SpecialtyMetaData vector search only.
            # No regex, no classify call. SpecialtyService embeds the query and matches
            # against NUCC specialty embeddings via cosine similarity.
            spec_fn = find_specialty_fn or (self.identify_specialty if self._specialty else None)
            if spec_fn:
                specialty_result = spec_fn(specialty_query)
                if "error" in specialty_result:
                    return specialty_result
                codes = [s["Code"] for s in specialty_result.get("specialties", [])]

            if not codes:
                return {"supported": True, "providers": [],
                        "message": f"No matching specialty found for '{specialty_query}'."}

            # Build specialization_options from vector search results (preserves rank)
            specialization_options = []
            for s in specialty_result.get("specialties", []):
                specialization_options.append({
                    "code": s.get("Code", ""),
                    "name": s.get("Display Name", ""),
                    "can_prescribe": s.get("can_prescribe", False),
                    "homeopathic": s.get("homeopathic", False),
                    "rank": s.get("rank", 0),
                })
            self._cache_taxonomy_names(specialization_options)

            # Step 3: Database answers — deterministic taxonomy query
            base_filter = {"taxonomies.code": {"$in": codes}}
            base_filter.update(self._practice_address_filter(state=state_upper, city=city, county=county, zip=zip))
            base_filter.update(self._preference_filter(provider_sex, sole_proprietor, insurance))

            providers, total_count = self._facet_query(
                collection, base_filter, cursor, safe_limit, entity_type, direction, codes)
            log.info("search: specialty '%s' → %d codes → %d/%d providers in %s",
                       specialty_query, len(codes), len(providers), total_count, state_upper or "all")

            # Log resolved codes to admin DB for debugging (non-prod only)
            if self._env != "prod":
                try:
                    db = self._get_db()
                    if db:
                        db[f"{self._env}_admin"]["specialty_code_log"].insert_one({
                            "query": specialty_query,
                            "resolved_codes": codes,
                            "code_count": len(codes),
                            "total_providers": total_count,
                            "state": state_upper,
                            "timestamp": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
                        })
                except Exception as _exc:
                    # Mode 1 (REQ-B-008): admin debug log write; non-prod
                    # only, no user impact. Failure is silently ignored.
                    log.info("specialty_code_log write failed (ignored): %s", _exc, exc=ChatHealthyException(
                                                                                        mode="specialty_code_log_write_failed",
                                                                                        message=f"specialty_code_log write failed (ignored): {_exc}",
                                                                                        component="FindCareService",
                                                                                        exception=_exc,
                                                                                    ))

            # search_params includes resolved codes — /search replays with codes, no AI
            replay_params = {"state": state_upper, "nucc_codes": codes}
            if city: replay_params["city"] = city
            if county: replay_params["county"] = county
            if zip: replay_params["zip"] = zip
            return self._paginated_result(providers, "taxonomy", safe_limit, entity_type,
                                          search_params=replay_params, total_count=total_count,
                                          collection=collection, base_filter=base_filter,
                                          chosen=chosen,
                                          state=state_upper, specialty_searched=specialty_query,
                                          specialization_options=specialization_options)

        # ── Route 5: County fallback ──
        if county and state_upper:
            base_filter = {
                # v4 promotes the primary taxonomy to the document, so the
                # element match on the primary flag is a test of one scalar.
                # $regex is not permitted in this tree; a prefix range does
                # the same work on an indexable field.
                "primary_taxonomy_code": {"$gte": "2", "$lt": "3"},
            }
            base_filter.update(self._practice_address_filter(state=state_upper, county=county, zip=zip))
            base_filter.update(self._preference_filter(provider_sex, sole_proprietor, insurance))
            providers, total_count = self._facet_query(
                collection, base_filter, cursor, safe_limit, entity_type, direction,
                specialty_codes)
            log.info("search: county fallback returned %d for '%s' in %s", len(providers), county, state_upper)
            return self._paginated_result(providers, "county_physicians", safe_limit, entity_type,
                                          search_params=_search_params, total_count=total_count,
                                          collection=collection, base_filter=base_filter,
                                          chosen=chosen,
                                          state=state_upper, county_searched=county)

        return {"supported": True, "providers": [],
                "message": f"No providers found matching the search criteria."}

    def _label_rows_by_own_taxonomy(self, rows: list) -> None:
        """Fill the facility type on any row the option set did not label.

        The row's type is the label of the row's own primary taxonomy. The
        option cache holds the codes the search named, so a facility that
        matched on one taxonomy and is primary in another arrives unlabelled.
        Reads the catalogue once for exactly the codes still missing.
        """
        missing = {r.get("_primary_taxonomy_code") for r in rows
                   if isinstance(r, dict) and not r.get("facility_type")}
        missing = {c for c in missing if c}
        if not missing:
            return
        # Whatever section the code lives in. REQ-B-007 asks for the label
        # of the row's primary taxonomy and puts no section on it: an
        # organization may be primary in an individual-section taxonomy,
        # and those rows showed no type at all.
        for doc in specialty_meta_coll().find(
                {"Code": {"$in": sorted(missing)}},
                {"_id": 0, "Code": 1, "Display Name": 1}):
            code = doc.get("Code", "")
            name = doc.get("Display Name", "")
            if code and name:
                self._taxonomy_name_cache[code] = name
        for row in rows:
            if not isinstance(row, dict) or row.get("facility_type"):
                continue
            row["facility_type"] = self._taxonomy_name_cache.get(
                row.get("_primary_taxonomy_code") or "", "")
        for row in rows:
            if isinstance(row, dict):
                row.pop("_primary_taxonomy_code", None)

    def _facility_type_options(self, base_filter: dict,
                               specialty_codes: list = None) -> list:
        """The kinds of facility this result can carry, from the catalogue.

        The catalogue holds a non-individual section and the facility page
        reads that one, exactly as the care-giver page reads the individual
        section. This is the only place a facility label is resolved, and
        it is resolved server-side because the server holds the catalogue.
        """
        wanted = list(specialty_codes or [])
        if not wanted:
            # Nothing was named, so the labels needed are the ones the page
            # can show: the primary taxonomy of each row it will return.
            # Read from the catalogue's non-individual section.
            wanted = [c for c in specialty_meta_coll().distinct(
                "Code", {"Section": "Non-Individual"}) if c]
        if not wanted:
            return []
        options = []
        for doc in specialty_meta_coll().find(
                {"Code": {"$in": wanted}, "Section": "Non-Individual"},
                {"_id": 0, "Code": 1, "Display Name": 1, "Classification": 1,
                 "Specialization": 1, "Definition": 1}):
            options.append({
                "code": doc.get("Code", ""),
                "name": doc.get("Display Name", ""),
                "classification": doc.get("Classification", ""),
                "specialization": doc.get("Specialization", ""),
                "definition": doc.get("Definition", ""),
            })
        return options

    def identify_specialty(self, query: str) -> dict:
        """UAT Feature 2: Identify NUCC specialty codes via the two-stage
        AI pipeline (EPIC-006-F-003-S-002)."""
        if not self._specialty:
            return {"error": "SpecialtyFilter not configured"}
        return self._specialty.find_specialties(query)
