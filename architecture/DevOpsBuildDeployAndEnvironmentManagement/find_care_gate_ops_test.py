# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# find_care_gate_ops_test.py
# Direct /gate-op contract tests for the ops I touched in the
# HTML-out-of-Python cleanup. No browser — keeps these fast and isolates
# them from the Playwright fixture (pymongo's transitive imports were
# leaving an asyncio loop running and breaking sync_playwright in the
# paired panels_and_auth smoke).
#
# Covers:
#   - claim_oauth_result (empty → null)
#   - claim_oauth_result (seeded → returns + clears)
#   - evalcare-splash (structured data, no html anywhere)

import os
import json
import ssl
import urllib.request


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

SHARED_URL = "https://localhost:8002"


def _ndjson_post(url, body):
    ctx = ssl._create_unverified_context()
    req = urllib.request.Request(
        url, method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/x-ndjson"},
        data=json.dumps(body).encode(),
    )
    with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
        raw = r.read().decode()
    return [json.loads(ln) for ln in raw.split("\n") if ln.strip()]


def test_claim_oauth_result_returns_null_when_empty():
    events = _ndjson_post(f"{SHARED_URL}/gate",
                          {"op": "claim_oauth_result", "payload": {}})
    kinds = [ev.get("kind") for ev in events]
    assert "oauth_result" in kinds, f"missing oauth_result event: {kinds}"
    result_event = next(ev for ev in events if ev.get("kind") == "oauth_result")
    assert result_event["data"]["result"] is None, (
        f"empty session should return result=null, got {result_event}"
    )


def test_claim_oauth_result_pop_and_clear():
    from dotenv import load_dotenv
    from pymongo import MongoClient
    load_dotenv("Code/.env")
    client = _ch_connection()
    coll = client["Users"]["sessions"]
    try:
        # Seed a session, then plant a pending_oauth_result. A session is
        # established by /auth/issue and by nothing else: it mints the
        # token and creates the session in one call.
        import urllib.request as _rq
        req = _rq.Request(f"{SHARED_URL}/auth/issue", method="POST",
                          data=b"{}",
                          headers={"Content-Type": "application/json"})
        token = json.loads(_rq.urlopen(req, timeout=30).read())
        guid = (token.get("token") or "")[-32:]
        assert guid, "/auth/issue did not return a session guid"

        seeded = {"outcome": "success", "message": "Welcome test",
                  "email": "test@example.com", "user_id": "u-TEST123"}
        res = coll.update_one({"_id": guid},
                              {"$set": {"pending_oauth_result": seeded}})
        assert res.matched_count == 1, "could not seed pending_oauth_result"

        first = _ndjson_post(f"{SHARED_URL}/gate",
                             {"op": "claim_oauth_result", "payload": {},
                              "prior_guid": guid})
        first_result = next(ev for ev in first
                            if ev.get("kind") == "oauth_result"
                           )["data"]["result"]
        assert first_result == seeded, (
            f"first claim should return seeded data, got {first_result}"
        )

        second = _ndjson_post(f"{SHARED_URL}/gate",
                              {"op": "claim_oauth_result", "payload": {},
                               "prior_guid": guid})
        second_result = next(ev for ev in second
                             if ev.get("kind") == "oauth_result"
                            )["data"]["result"]
        assert second_result is None, (
            f"second claim should return null after clear, got {second_result}"
        )
    finally:
        client.close()


def test_evalcare_splash_returns_structured_data_no_html():
    events = _ndjson_post(f"{SHARED_URL}/gate",
                          {"op": "evalcare-splash", "payload": {}})
    ev = next(e for e in events if e.get("kind") == "evalcare-splash")
    full = json.dumps(ev)
    assert "<html" not in full.lower() and "<div" not in full, (
        f"evalcare-splash must NOT carry HTML strings: {full[:300]!r}"
    )
    data = ev["data"].get("data") or {}
    assert data.get("title"), f"missing title: {ev}"
    assert "subtitle" in data, f"missing subtitle: {ev}"
