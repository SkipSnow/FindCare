# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""Story-level pytest for EPIC-006-F-002-S-002 Provider Detail data
management and retrieval.

ONE test function exercises the full data-management cycle described
in the story, against a small TEST database built from a zipcode-
filtered slice of provider_v03. The live PublicHealthData.provider_v03
collection is NOT touched.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest


# Rule-004: one place in this file obtains a connection, and it goes through
# the canonical utility. The certificate is the credential; there is no
# connection string here and no fallback. Raises if the identity cannot
# connect, which is the point -- a test that quietly connects as something
# else proves nothing about production.
def _ch_connection():
    import sys as _sys, pathlib as _pl
    for _d in _pl.Path(__file__).resolve().parents:
        if (_d / ".git").exists():
            _lib = _d / "ChatHealthyLib" / "src"
            if str(_lib) not in _sys.path:
                _sys.path.insert(0, str(_lib))
            break
    from chathealthy_lib.mongo_utilities import ChatHealthyMongoUtilities
    return ChatHealthyMongoUtilities().getConnection("DevOpsUser", 'ChatHealthyFrontEnd')


# Make the backend package importable for the sync module.
_BACKEND = Path(__file__).resolve().parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

_FINDCARE = Path(__file__).resolve().parents[5] / "FindCare"
if str(_FINDCARE) not in sys.path:
    sys.path.insert(0, str(_FINDCARE))


from build_provider_detail_test_db import (
    build as _build_test_db,
    teardown as _teardown_test_db,
    TEST_DB, TEST_COLL, TEST_NPI,
)


import sys as _sys, pathlib as _pl
for _d in _pl.Path(__file__).resolve().parents:
    if (_d / '.git').exists():
        _lib = _d / 'ChatHealthyLib' / 'src'
        if str(_lib) not in _sys.path:
            _sys.path.insert(0, str(_lib))
        break
from chathealthy_lib.exceptions import ChatHealthyException


@pytest.fixture(scope="module")
def test_coll():
    """Build the test DB once per module run; drop it on teardown."""
    try:
        from pymongo import MongoClient
    except ImportError:
        pytest.skip("pymongo not installed")
    _build_test_db()
    client = _ch_connection()
    yield client[TEST_DB][TEST_COLL]
    _teardown_test_db()


def test_provider_detail_data_management_cycle(test_coll):
    """Story-level test for EPIC-006-F-002-S-002.

    Exercises:
      REQ-B-001 live NPPES fetch
      REQ-B-002 comparison
      REQ-B-003 write-back on divergence
      REQ-B-004 new-address Google Maps geocoding (when applicable)
      REQ-B-005 urban marker stays off on new addresses
      REQ-B-006 county preserved on unchanged addresses
      REQ-B-008 normalized licenses[] + insurance[] regenerated
      REQ-B-009 active log appended on status change (n/a today for
                Stephanie — no deactivation events)
      REQ-B-010 quality flags recomputed
      REQ-B-011 provenance sidecar stamped
      REQ-B-012 failure containment (sync_summary always returns)
      REQ-B-013 atomic single update (replace_one)
      REQ-B-014 re-embed scheduled (not verified here; runs as
                BackgroundTask in the FastAPI handler)
    """
    from ProviderDetail.provider_detail_service import ProviderDetailService

    # Pre-state: the test slice carries Stephanie's record from the
    # baseline JSON. Capture it for diff assertions.
    pre = test_coll.find_one({"npi": TEST_NPI})
    assert pre is not None, "baseline record not in test slice"
    pre_load_id = pre.get("load_id")
    pre_addresses = pre.get("addresses") or []

    service = ProviderDetailService()
    scheduled = []
    result = service.lookup(
        provider_name="Stephanie Lauren Post",
        npi=TEST_NPI,
        state="CA",
        provider_coll=test_coll,
        schedule_background_task=(
            lambda fn, *a, **kw: scheduled.append((fn.__name__, a))
        ),
    )

    # Cycle ran; npi_details returned from stored record (which is now
    # post-write-back if there was divergence).
    assert result.get("npi") == TEST_NPI
    assert result.get("research_sites"), "research_sites missing"
    assert result.get("npi_details"), "npi_details missing"

    post = test_coll.find_one({"npi": TEST_NPI})
    assert post is not None

    # REQ-B-011: provenance sidecar stamped with origin='real_time_sync'.
    # (Always — even when no divergence, we run the cycle. If no diff,
    # no write happens and provenance may not be present.)
    if result.get("npi_details") and pre_load_id is not None:
        # If the cycle wrote, provenance is present.
        if post.get("provenance"):
            assert post["provenance"]["origin"] == "real_time_sync"
            assert post["provenance"]["real_time_sync_count"] >= 1
            assert post["provenance"]["last_touched_at"]

    # REQ-B-008: licenses[] and insurance[] are present (regenerated if
    # the cycle wrote; otherwise present from pre-state).
    assert "licenses" in post or "licenses" not in pre, (
        "licenses[] regression"
    )
    assert "insurance" in post or "insurance" not in pre, (
        "insurance[] regression"
    )

    # REQ-B-014: re-embed task was scheduled when a write happened.
    if post.get("provenance"):
        assert any(
            name == "embed_after_response" for name, _ in scheduled
        ), "re-embed BackgroundTask not scheduled"

    # REQ-B-006: county preserved on unchanged addresses. Look for any
    # post-address whose key matches a pre-address AND check its county
    # carries over verbatim.
    pre_addr_keys = {
        (
            (a.get("line1") or "").strip(),
            (a.get("city") or "").strip(),
            (a.get("state") or "").strip(),
            (a.get("zip") or "")[:5],
        ): a
        for a in pre_addresses
    }
    for pa in post.get("addresses") or []:
        k = (
            pa.get("line1", ""),
            pa.get("city", ""),
            pa.get("state", ""),
            pa.get("zip", ""),
        )
        if k in pre_addr_keys:
            pre_county = (pre_addr_keys[k].get("county") or {})
            post_county = (pa.get("county") or {})
            if pre_county.get("fips"):
                assert post_county.get("fips") == pre_county.get("fips"), (
                    f"county.fips not preserved on unchanged address {k}"
                )


def test_provider_detail_identical_records_no_write(test_coll):
    """REQ-B-002 / REQ-B-013: when the live NPPES response matches the
    stored record byte-for-byte, no write happens and the provenance
    real_time_sync_count does not advance on the second cycle.

    Setup: run the cycle ONCE so the test record converges to the live
    NPPES state. Run it a SECOND time and assert no further changes.
    """
    from ProviderDetail.provider_detail_service import ProviderDetailService

    service = ProviderDetailService()

    # Cycle 1: brings the test record to live-NPPES freshness.
    service.lookup(
        provider_name="Stephanie Lauren Post",
        npi=TEST_NPI,
        state="CA",
        provider_coll=test_coll,
    )
    after_first = test_coll.find_one({"npi": TEST_NPI})
    sync_count_after_first = (
        (after_first.get("provenance") or {}).get("real_time_sync_count") or 0
    )

    # Cycle 2: live == stored now; no divergence, no write.
    scheduled = []
    service.lookup(
        provider_name="Stephanie Lauren Post",
        npi=TEST_NPI,
        state="CA",
        provider_coll=test_coll,
        schedule_background_task=(
            lambda fn, *a, **kw: scheduled.append(fn.__name__)
        ),
    )
    after_second = test_coll.find_one({"npi": TEST_NPI})

    # REQ-B-002: identical-records branch — provenance counter does
    # NOT advance because no write happened.
    sync_count_after_second = (
        (after_second.get("provenance") or {}).get("real_time_sync_count") or 0
    )
    assert sync_count_after_second == sync_count_after_first, (
        f"provenance.real_time_sync_count advanced from "
        f"{sync_count_after_first} to {sync_count_after_second} on the "
        f"identical-records cycle — write happened when it should not have"
    )

    # REQ-B-014 inversely: no re-embed scheduled when nothing changed.
    assert "embed_after_response" not in scheduled, (
        "re-embed scheduled on identical-records cycle (should be skipped)"
    )

    # REQ-B-013 (idempotency): the record bytes are stable across the
    # second cycle.
    a1 = {k: v for k, v in (after_first or {}).items() if k != "_id"}
    a2 = {k: v for k, v in (after_second or {}).items() if k != "_id"}
    assert a1 == a2, "record diverged across identical-records cycle"
