# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
"""Build the test database slice for the Provider Detail data-management
pytest (EPIC-006-F-002-S-002).

Per design §14.3: copy every provider record from PublicHealthData.
provider_v03 whose primary practice address ZIP equals the test
provider's ZIP, into a separate test database on the front-end Mongo
cluster. The slice is small but representative; the pytest exercises
the compare-and-write-back cycle against it without touching the live
collection.

Usage (programmatic — called by the pytest fixture):
    from build_provider_detail_test_db import (
        build, teardown, TEST_DB, TEST_COLL,
    )
    build()
    ...
    teardown()
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys as _sys, pathlib as _pl

import sys as _sys, pathlib as _pl
for _d in _pl.Path(__file__).resolve().parents:
    if (_d / ".git").exists():
        _lib = _d / "ChatHealthyLib" / "src"
        if str(_lib) not in _sys.path:
            _sys.path.insert(0, str(_lib))
        break
from chathealthy_lib.logging_service import ChatHealthyLoggingService

_CH_LOG = ChatHealthyLoggingService()


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
for _d in _pl.Path(__file__).resolve().parents:
    if (_d / '.git').exists():
        _lib = _d / 'ChatHealthyLib' / 'src'
        if str(_lib) not in _sys.path:
            _sys.path.insert(0, str(_lib))
        break
from chathealthy_lib.exceptions import ChatHealthyException
import sys as _ch_sys, pathlib as _ch_pl  # noqa: E402
for _ch_d in _ch_pl.Path(__file__).resolve().parents:
    if (_ch_d / '.git').exists():
        _ch_lib = _ch_d / 'ChatHealthyLib' / 'src'
        if str(_ch_lib) not in _ch_sys.path:
            _ch_sys.path.insert(0, str(_ch_lib))
        break
from chathealthy_lib.exceptions import ChatHealthyException  # noqa: E402
import sys


TEST_DB = "PublicHealthData_test"
TEST_COLL = "provider_test_slice"
TEST_NPI = "1003199654"  # Stephanie Lauren Post — the divergence reference

# Stephanie's stored record carries two SF addresses, ZIPs 94115 + 94123.
# The slice is built from her primary practice ZIP — the first address
# entry whose address_type=='practice'.
_BASELINE_PATH = (
    Path(__file__).resolve().parents[5]
    / "FindCare" / "architectureAndDesign"
    / "EPIC-006-F-002-baseline-record-NPI-1003199654.json"
)


def _primary_practice_zip(record: dict) -> str:
    for a in record.get("addresses", []):
        if a.get("address_type") == "practice" and a.get("zip"):
            return a["zip"]
    raise ChatHealthyException(
        mode="value_error",
        component="build_provider_detail_test_db",
        message=f"baseline record {record.get('npi')} has no practice address")


def build() -> int:
    """Build the test slice. Returns the count of records copied."""
    from pymongo import MongoClient
    c = _ch_connection()
    live = c["PublicHealthData"]["provider_v03"]
    test = c[TEST_DB][TEST_COLL]

    test.drop()

    baseline = json.loads(_BASELINE_PATH.read_text(encoding="utf-8"))
    target_zip = _primary_practice_zip(baseline)
    docs = list(live.find({"addresses.zip": target_zip}))
    if not docs:
        # Fall back to inserting just the baseline so the pytest can run
        # against at least one record even if the live ZIP slice is
        # empty.
        docs = [baseline]
    else:
        # Ensure the baseline (NPI 1003199654) is present even if the
        # live record carries a different ZIP than the stored baseline.
        if not any(d.get("npi") == TEST_NPI for d in docs):
            docs.append(baseline)

    test.insert_many(docs)
    test.create_index("npi")
    return test.count_documents({})


def teardown() -> None:
    """Drop the test collection. (FrontEndUser does not have
    dropDatabase rights; per-collection drop works.)"""
    from pymongo import MongoClient
    c = _ch_connection()
    c[TEST_DB][TEST_COLL].drop()


def restore_baseline_record_to_v03() -> None:
    """Restore Stephanie's record in the LIVE provider_v03 collection
    to the captured baseline (per Skip's instruction: leave v03
    untouched after testing). Call only from the pytest teardown OR
    from the operator-run restore script — never from production
    code."""
    from pymongo import MongoClient
    c = _ch_connection()
    baseline = json.loads(_BASELINE_PATH.read_text(encoding="utf-8"))
    coll = c["PublicHealthData"]["provider_v03"]
    coll.replace_one({"npi": TEST_NPI}, baseline, upsert=True)


def main() -> int:
    """Drive the program and report its status.

    The exit lives here because this is the function the guard
    calls, and a process reports its outcome by exit code.
    """
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    if cmd == "build":
        n = build()
        _CH_LOG.info(f"test slice built at {TEST_DB}.{TEST_COLL}: {n} records")
    elif cmd == "teardown":
        teardown()
        _CH_LOG.info(f"test database {TEST_DB} dropped")
    elif cmd == "restore":
        restore_baseline_record_to_v03()
        _CH_LOG.info(f"NPI {TEST_NPI} restored to baseline in provider_v03")
    else:
        _CH_LOG.info(f"unknown command: {cmd}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
