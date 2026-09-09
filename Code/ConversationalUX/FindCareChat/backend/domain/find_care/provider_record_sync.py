# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""provider_record_sync — Provider Detail data-management cycle.

Realizes EPIC-006-F-002-S-002. Owns the compare + write-back +
provenance-stamp + embed-trigger logic the Provider Detail flow uses on
every click.

Public surface:
    live_to_comparable(live_nppes_response) -> dict
    stored_to_comparable(stored_record) -> dict
    compare(live_proj, stored_proj) -> dict (per-section divergence)
    regenerate_licenses(taxonomies) -> list[dict]
    regenerate_insurance(other_identifiers) -> list[dict]
    geocode_new_address(address) -> dict address with county or address w/o county
    merge_for_writeback(live, stored) -> new record
    write_back(coll, npi, new_doc) -> bool
    stamp_provenance(record) -> record (mutates in place)

The module does NOT call Mongo directly except via the collection
argument passed in. The pytest can inject a test collection; the
production handler injects the runtime-bound collection.
"""
from __future__ import annotations

import datetime as dt
from chathealthy_lib import ChatHealthyLoggingService
from chathealthy_lib.exceptions import ChatHealthyException
import os
from copy import deepcopy
from typing import Any

import requests


# ── Live -> comparable projection ─────────────────────────────────────

log = ChatHealthyLoggingService()


def stored_addresses(record: dict) -> list[dict]:
    """Every stored address, practice first then business, each saying
    which of the two it is.

    v4 splits the stored addresses[] into practice_addresses[] and a lone
    business_address, so the field an address came out of is what makes it
    a business address or a practice one. Flattening the two into one list
    is what threw that away: business_address does not always carry an
    address_type, and every reader downstream then had to guess. It is
    stamped here, where the answer is still known.

    The LIVE NPPES response is unaffected and keeps its own shape --
    live_to_addresses still reads live["addresses"], because that is
    NPPES's field and not ours.
    """
    out = [dict(a) for a in (record.get("practice_addresses") or [])]
    business = record.get("business_address")
    if business:
        out.append({**business, "address_type": "business"})
    return out


def split_stored_addresses(addresses: list[dict]) -> dict:
    """The inverse: the two fields v4 writes, from one tagged list.

    Which field an address belongs in is the same question _kind_of
    answers, and asking it any other way is how a secondary practice
    address -- which says "secondary_practice", not "practice" -- was
    dropped on write-back instead of being written to practice_addresses.
    """
    practice = [a for a in addresses if _kind_of(a or {}) == "practice"]
    business = next((a for a in addresses
                     if _kind_of(a or {}) == "business"), None)
    written: dict = {"practice_addresses": practice}
    if business:
        written["business_address"] = business
    return written


def live_to_comparable(live: dict) -> dict:
    return {
        "name": live_name(live),
        "addresses": live_to_addresses(live),
        "taxonomies": live_to_taxonomies(live),
        "other_identifiers": live_to_other_identifiers(live),
        "status_active": live_status_active(live),
        "enumeration_date": (live.get("basic") or {}).get("enumeration_date"),
    }


def stored_to_comparable(stored: dict) -> dict:
    return {
        "name": stored_name(stored),
        "addresses": [
            {k: v for k, v in a.items() if k != "county"}
            for a in stored_addresses(stored)
        ],
        "taxonomies": [
            {"code": t.get("code", ""), "primary": bool(t.get("primary"))}
            for t in (stored.get("taxonomies") or [])
        ],
        "other_identifiers": stored.get("other_identifiers") or [],
        "status_active": stored_status_active(stored),
        "enumeration_date": stored.get("provider_enumeration_date"),
    }


def live_name(live: dict) -> dict:
    b = live.get("basic") or {}
    return {
        "first": (b.get("first_name") or "").strip(),
        "middle": (b.get("middle_name") or "").strip(),
        "last": (b.get("last_name") or "").strip(),
        "prefix": (b.get("name_prefix") or "").strip(),
        "credential": (b.get("credential") or "").strip(),
    }


def stored_name(stored: dict) -> dict:
    return {
        "first": (stored.get("provider_first_name") or "").strip(),
        "middle": (stored.get("provider_middle_name") or "").strip(),
        "last": (
            stored.get("provider_last_name_legal_name")
            or stored.get("provider_last_name") or ""
        ).strip(),
        "prefix": (stored.get("provider_name_prefix_text") or "").strip(),
        "credential": (stored.get("provider_credential_text") or "").strip(),
    }


def live_to_addresses(live: dict) -> list[dict]:
    out = []
    for a in live.get("addresses") or []:
        purpose = (a.get("address_purpose") or "").upper()
        address_type = (
            "practice" if purpose == "LOCATION"
            else "business" if purpose == "MAILING"
            else purpose.lower()
        )
        out.append({
            "line1": (a.get("address_1") or "").strip(),
            "line2": (a.get("address_2") or "").strip(),
            "city": (a.get("city") or "").strip(),
            "state": (a.get("state") or "").strip(),
            "zip": ((a.get("postal_code") or "")[:5]),
            "country": (a.get("country_code") or "").strip(),
            "phone": (a.get("telephone_number") or "").strip(),
            "address_type": address_type,
        })
    return out


def live_to_taxonomies(live: dict) -> list[dict]:
    return [
        {"code": t.get("code", ""), "primary": bool(t.get("primary"))}
        for t in (live.get("taxonomies") or [])
    ]


def live_to_other_identifiers(live: dict) -> list[dict]:
    out = []
    for oi in live.get("other_identifiers") or []:
        out.append({
            "identifier": (oi.get("identifier") or "").strip(),
            "type_code": (oi.get("code") or "").strip(),
            "type_description": (oi.get("desc") or "").strip(),
            "state": (oi.get("state") or "").strip(),
            "issuer": (oi.get("issuer") or "").strip(),
        })
    return out


def live_status_active(live: dict) -> bool:
    return (live.get("basic") or {}).get("status") == "A"


def stored_status_active(stored: dict) -> bool:
    active = stored.get("active") or []
    if not active:
        return True  # Records without an active log are presumed active.
    return bool(active[-1].get("is_active"))


# ── Comparison ───────────────────────────────────────────────────────


def compare(live_proj: dict, stored_proj: dict) -> dict:
    """Per-section divergence flags. False == identical."""
    return {
        "name": live_proj.get("name") != stored_proj.get("name"),
        "addresses": live_proj.get("addresses") != stored_proj.get("addresses"),
        "taxonomies": live_proj.get("taxonomies") != stored_proj.get("taxonomies"),
        "other_identifiers": (
            live_proj.get("other_identifiers")
            != stored_proj.get("other_identifiers")
        ),
        "status_active": (
            live_proj.get("status_active") != stored_proj.get("status_active")
        ),
        "enumeration_date": (
            live_proj.get("enumeration_date")
            != stored_proj.get("enumeration_date")
        ),
    }


def has_any_divergence(div: dict) -> bool:
    return any(div.values())


# ── Normalized array regeneration ─────────────────────────────────────


def regenerate_licenses(live_taxonomies: list[dict]) -> list[dict]:
    """Pull (state, number) per taxonomy that has license info."""
    out = []
    for t in live_taxonomies or []:
        state = (t.get("state") or "").strip()
        number = (t.get("license") or "").strip()
        if state or number:
            out.append({"state": state, "number": number})
    return out


INSURANCE_TYPE_MAP = {
    "01": "Other",
    "05": "Medicaid",
    "06": "Medicare",
}


def regenerate_insurance(live_other_identifiers: list[dict]) -> list[dict]:
    out = []
    for oi in live_other_identifiers or []:
        type_code = (oi.get("code") or "").strip()
        type_desc = (oi.get("desc") or "").strip()
        insurance_type = INSURANCE_TYPE_MAP.get(type_code) or type_desc or "Other"
        state = (oi.get("state") or "").strip()
        issuer = (oi.get("issuer") or "").strip()
        brand = issuer if issuer else (
            f"State Medicaid Agency — {state}" if insurance_type == "Medicaid" and state
            else insurance_type
        )
        out.append({
            "insurance_type": insurance_type,
            "payer_name": brand,
            "state": state,
            "issuer_raw": (oi.get("identifier") or "").strip(),
        })
    return out


# ── Google Maps geocoding for new addresses ───────────────────────────


GMAPS_ENDPOINT = "https://maps.googleapis.com/maps/api/geocode/json"


def geocode_new_address(address: dict) -> dict:
    """Return the address with a county object stamped on success, or
    with NO county on failure (the caller's WARNING log fires in either
    case the source label is 'geocoder_pass4_maps')."""
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    out = dict(address)
    if not api_key:
        log.warning(
            "GOOGLE_MAPS_API_KEY absent; new address county lookup "
            "skipped for %s, %s",
            address.get("line1"), address.get("city"),
        )
        return out
    line1 = (address.get("line1") or "").strip()
    city = (address.get("city") or "").strip()
    state = (address.get("state") or "").strip()
    zip5 = (address.get("zip") or "")[:5]
    q = f"{line1}, {city}, {state} {zip5}"
    try:
        resp = requests.get(
            GMAPS_ENDPOINT,
            params={"address": q, "key": api_key},
            timeout=15,
        )
        data = resp.json()
    except Exception as exc:
        # Mode 1 (REQ-B-008): Google Maps lookup is best-effort enrichment.
        log.info("Google Maps lookup failed for %s: %s", q, exc, exc=ChatHealthyException(
                                                                     mode="google_maps_lookup_failed",
                                                                     message=f"Google Maps lookup failed for {q}: {exc}",
                                                                     component="ProviderRecordSync",
                                                                     exception=exc,
                                                                 ))
        return out
    results = (data or {}).get("results") or []
    if not results:
        log.warning(
            "Google Maps returned no authoritative result for %s; "
            "county omitted on this address", q,
        )
        return out
    comps = results[0].get("address_components") or []
    county_name = ""
    for c in comps:
        if "administrative_area_level_2" in (c.get("types") or []):
            county_name = (c.get("long_name") or "").strip()
            break
    if not county_name:
        log.warning(
            "Google Maps returned a result without administrative_area_level_2 "
            "for %s; county omitted", q,
        )
        return out
    out["county"] = {
        "name": county_name,
        "source": "geocoder_pass4_maps",
    }
    return out


# ── Per-enrichment preservation matrix ────────────────────────────────


def _kind_of(address: dict) -> str:
    """Whether this is a place someone practises or a place post is sent.

    NPPES says it as address_purpose, which live_to_addresses records as
    address_type; a stored practice address carries "practice" or
    "secondary_practice", and business_address is the business one whether
    or not it says so. Both reduce to the two kinds, because it is the
    kind that decides which stored address a live one corresponds to.
    """
    kind = str(address.get("address_type") or "").strip().lower()
    return "business" if kind.startswith("business") or kind == "mailing" else "practice"


def _carries_a_county(address: dict) -> bool:
    """Whether this address actually names a county.

    A county the pipeline resolved looks like {"fips": "06037", "source":
    "zip_crosswalk", "name": "Los Angeles County", ...}. An address the
    pipeline never enriched carries {"fips": None} -- a dict with no name,
    which is truthy, and was therefore preserved as though it were an
    answer. Nothing downstream can use it: the row reads county["name"]
    and gets nothing.
    """
    county = address.get("county")
    return bool(isinstance(county, dict) and str(county.get("name") or "").strip())


def addresses_with_preserved_county(
    live_addresses: list[dict], stored_addresses: list[dict],
) -> list[dict]:
    """For each live address: if it matches a stored address by line1 +
    city + state + zip, preserve the stored county verbatim. Otherwise
    call geocode_new_address (urban marker stays absent on new).

    A practice address is matched against a stored PRACTICE address, and a
    business address against the stored business one. They are different
    kinds of address that frequently share a street: a provider whose
    mailing address is the place they practise holds it in
    practice_addresses AND as business_address.

    Keyed on the street alone the two collided, and stored_addresses puts
    the business one last, so it replaced the practice entry. Business
    addresses are never county-enriched, so a write-back on the first
    Provider Detail open replaced a resolved county with an empty one and
    called it preservation. 55.8% of providers have that collision.

    The kind is part of what an address IS, so it is part of the key.
    """
    stored_by_key = {}
    for a in stored_addresses or []:
        key = (
            _kind_of(a),
            (a.get("line1") or "").strip(),
            (a.get("city") or "").strip(),
            (a.get("state") or "").strip(),
            (a.get("zip") or "")[:5],
        )
        stored_by_key[key] = a
    out = []
    for la in live_addresses:
        key = (
            _kind_of(la),
            la.get("line1", ""),
            la.get("city", ""),
            la.get("state", ""),
            la.get("zip", ""),
        )
        sa = stored_by_key.get(key)
        merged = dict(la)
        # A stub is not a county. Carrying {"fips": None} across as though
        # it were an answer is how the enrichment was lost; falling
        # through to the geocoder is how it is recovered.
        if sa is not None and _carries_a_county(sa):
            merged["county"] = deepcopy(sa["county"])
        else:
            merged = geocode_new_address(merged)
        out.append(merged)
    return out


def update_active_log(
    stored_active: list[dict],
    live: dict,
) -> list[dict]:
    basic = live.get("basic") or {}
    deact = (basic.get("npi_deactivation_date") or "").strip()
    react = (basic.get("npi_reactivation_date") or "").strip()
    stored = list(stored_active or [])
    # Detect the latest event represented in stored.
    last_stored_date = stored[-1].get("date") if stored else None
    last_stored_is_active = stored[-1].get("is_active") if stored else True
    # If status mismatch with the live signal, append.
    live_is_active = basic.get("status") == "A"
    if deact and (last_stored_date != deact or last_stored_is_active is True):
        stored.append({
            "event": "deactivated",
            "date": deact,
            "is_active": False,
            "source": "nppes_deactivation_date",
        })
    if react and (last_stored_date != react or last_stored_is_active is False):
        stored.append({
            "event": "reactivated",
            "date": react,
            "is_active": True,
            "source": "nppes_reactivation_date",
        })
    if not stored and not live_is_active:
        # Only deactivation date is missing but status says inactive.
        stored.append({
            "event": "deactivated",
            "date": "",
            "is_active": False,
            "source": "nppes_basic_status",
        })
    return stored


def recompute_quality_flags(record: dict) -> dict:
    addresses = stored_addresses(record)
    active = record.get("active") or []
    out = dict(record)
    if not addresses:
        out["bad_data"] = {"flagged": True, "reason": "no_address"}
    else:
        out.pop("bad_data", None)
    practice = next(
        (a for a in addresses if _kind_of(a) == "practice"), None)
    if practice and (practice.get("country") or "US") != "US":
        out["out_of_scope"] = {
            "flagged": True, "reason": "foreign_provider",
        }
    elif active and not active[-1].get("is_active"):
        out["out_of_scope"] = {
            "flagged": True, "reason": "deactivated",
        }
    else:
        out.pop("out_of_scope", None)
    return out


# ── Provenance ─────────────────────────────────────────────────────────


def stamp_provenance(record: dict) -> dict:
    out = dict(record)
    prov = dict(out.get("provenance") or {})
    prov["origin"] = "real_time_sync"
    prov["last_touched_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    prov["real_time_sync_count"] = int(prov.get("real_time_sync_count", 0)) + 1
    out["provenance"] = prov
    return out


# ── Merge for write-back ───────────────────────────────────────────────


def merge_for_writeback(live: dict, stored: dict) -> dict:
    """Build the new record from live NPPES + stored enrichments.

    Per the design's preservation matrix:
        - addresses: live shape, county preserved on unchanged addresses
        - taxonomies: live verbatim
        - other_identifiers: live (normalized) — exact pipeline shape
        - licenses[]: regenerated from live
        - insurance[]: regenerated from live
        - can_prescribe/is_homeopathic/is_disqualified: preserved when
          taxonomies unchanged; recompute deferred (catalog can be
          unreachable) — when changed, WARN + preserve as-is
        - active[]: append on status change
        - bad_data / out_of_scope: recomputed
        - load_id / loaded_at / chunk_id / row_index_in_chunk: preserved
        - embedding / embedding_model / embedding_version: preserved
          (re-embed runs as a BackgroundTask after the response returns)
        - provenance: stamped real_time_sync
    """
    new = deepcopy(stored)

    # ── NPPES-sourced fields overwritten ──────────────────────────────
    basic = live.get("basic") or {}
    new["provider_first_name"] = basic.get("first_name", "")
    new["provider_middle_name"] = basic.get("middle_name", "")
    # v03 used provider_last_name_legal_name for individuals
    new["provider_last_name_legal_name"] = basic.get("last_name", "")
    if basic.get("name_prefix"):
        new["provider_name_prefix_text"] = basic.get("name_prefix")
    if basic.get("credential"):
        new["provider_credential_text"] = basic.get("credential")
    if basic.get("enumeration_date"):
        new["provider_enumeration_date"] = basic.get("enumeration_date")

    # Addresses — preserve county on unchanged, geocode on new.
    live_addrs = live_to_addresses(live)
    stored_addrs = stored_addresses(stored)
    # Written back into the two fields v4 holds, not the one v03 held.
    new.update(split_stored_addresses(
        addresses_with_preserved_county(live_addrs, stored_addrs)))
    new.pop("addresses", None)

    # Taxonomies + flags
    live_tax = live_to_taxonomies(live)
    new["taxonomies"] = live_tax
    stored_tax = [
        {"code": t.get("code", ""), "primary": bool(t.get("primary"))}
        for t in (stored.get("taxonomies") or [])
    ]
    if live_tax != stored_tax:
        log.warning(
            "taxonomies changed for NPI %s; flag recompute deferred — "
            "catalog may be unreachable, preserving stored flags",
            stored.get("npi"),
        )
        # Preserve existing can_prescribe / is_homeopathic / is_disqualified

    # licenses[] regenerated
    new["licenses"] = regenerate_licenses(live.get("taxonomies") or [])

    # other_identifiers (raw) + insurance[] regenerated
    new["other_identifiers"] = live_to_other_identifiers(live)
    new["insurance"] = regenerate_insurance(live.get("other_identifiers") or [])

    # active[] appended on status change
    new["active"] = update_active_log(stored.get("active") or [], live)

    # quality flags recomputed
    new = recompute_quality_flags(new)

    # provenance stamped
    new = stamp_provenance(new)

    return new


# ── Write-back ────────────────────────────────────────────────────────


def write_back(coll, npi: str, new_doc: dict) -> bool:
    """Single atomic replace_one. Returns True if a doc was replaced."""
    # Remove _id from new_doc so replace_one keeps the existing _id.
    doc = {k: v for k, v in new_doc.items() if k != "_id"}
    result = coll.replace_one({"npi": npi}, doc, upsert=False)
    return bool(result.modified_count or result.matched_count)


# ── Embedding background task ─────────────────────────────────────────


def build_embedding_text(record: dict) -> str:
    """The embedding input. Single source of truth for both the embed call and
    the diff check that gates whether re-embedding is needed.

    Delegates to chathealthy_lib.provider_embedding so the front-end
    re-embed produces byte-identical text to what the pipeline produced when
    the record was first embedded — same record yields same vector on both
    sides."""
    from chathealthy_lib.provider_embedding import project, render
    return render(project(record))


def embed_after_response(coll, npi: str) -> None:
    """Called from FastAPI BackgroundTasks after the handler returns.

    Reads the current record, generates a new embedding via the canonical
    global embedding model, updates the record's embedding fields. On
    failure, prior embedding fields are preserved and a WARNING is
    logged (the response has already returned).
    """
    try:
        from infrastructure.embeddings.embedding_client import EmbeddingClient
    except ImportError as _imp:
        # Mode 1 (REQ-B-008): EmbeddingClient not importable; skip re-embed.
        log.info(
            "EmbeddingClient unavailable; skipping re-embed for NPI %s",
            npi,
            exc=ChatHealthyException(
             mode="embedding_client_unavailable",
             message=f"EmbeddingClient unavailable; skipping re-embed for NPI {npi}: {_imp}",
             component="ProviderRecordSync",
             exception=_imp,
         ),
        )
        return
    try:
        doc = coll.find_one({"npi": npi})
        if doc is None:
            return
        text = build_embedding_text(doc)
        if not text:
            return
        client = EmbeddingClient()
        vec = client.embed(text)
        coll.update_one(
            {"npi": npi},
            {"$set": {
                "embedding": vec,
                "embedding_model": client.model_name,
                "embedding_version": client.model_version,
            }},
        )
    except Exception as exc:
        # Mode 1 (REQ-B-008): main record write succeeded; only the
        # embedding refresh failed — will retry on next sync.
        log.info(
            "Embedding call failed after write-back for NPI %s: %s",
            npi, exc,
            exc=ChatHealthyException(
             mode="embedding_call_failed_after_writeback",
             message=f"Embedding call failed after write-back for NPI {npi}: {exc}",
             component="ProviderRecordSync",
             exception=exc,
         ),
        )
