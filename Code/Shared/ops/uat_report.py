# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# uat_report.py — DevOps tool: generates clean UAT welcome report.
#
# Starts blank every session. Reads features from brain/uat config.
# Build number comes from /health endpoint at runtime.
# Done, Bugs Fixed, New Features columns are ALWAYS blank at start.
#
# Usage: imported by main.py, called to build the welcome message.
# Location: Code/Shared/ops/ (DevOps tool, not business logic)

import json

import os
from pathlib import Path

import sys as _ch_sys, pathlib as _ch_pl
for _ch_d in _ch_pl.Path(__file__).resolve().parents:
    if (_ch_d / ".git").exists():
        _ch_lib = _ch_d / "ChatHealthyLib" / "src"
        if str(_ch_lib) not in _ch_sys.path:
            _ch_sys.path.insert(0, str(_ch_lib))
        break
# The chain materialises the application .env, which sets
# CH_LOG_DESTINATION=mongo and CH_LOG_DB=pipelineAdmin. Those are the
# deployed application's facts, not this tool's: devops tooling runs on
# a workstation and its log is the operator's terminal. Inheriting them
# made a build depend on a Mongo write it has no grant for.
import os as _ch_os
_ch_os.environ["CH_LOG_DESTINATION"] = "stderr"
from chathealthy_lib.logging_service import ChatHealthyLoggingService

_log = ChatHealthyLoggingService()

# UAT feature definitions — the WHAT, not the results
# Results are filled in by human during testing
UAT_FEATURES = [
    {"id": 1,  "feature": "Provider Search (DE + MS + VA, vector + regex)"},
    {"id": 2,  "feature": "Specialty Identification (NUCC + AI expansion)"},
    {"id": 3,  "feature": "Clinical Trials Search (+ travel time)"},
    {"id": 4,  "feature": "About ChatHealthy / Skip Snow"},
    {"id": 5,  "feature": "Safety Filter (dual-trigger, IP lock, audit)"},
    {"id": 6,  "feature": "Lead Capture (follow-up offer)"},
    {"id": 7,  "feature": "Consent Framework (two-stream)"},
    {"id": 8,  "feature": "Provider Detail (NPI lookup + external links)"},
    {"id": 9,  "feature": "URL Guardian (validate + defang broken links)"},
    {"id": 10, "feature": "Chat UX (timer, stop, markdown, emergency)"},
    {"id": 11, "feature": "Blob Storage Infrastructure"},
    {"id": 12, "feature": "Unanswerable Question Handling (3-path)"},
    {"id": 13, "feature": "Markdown Table Rendering (GFM tables in chat)"},
    {"id": 14, "feature": "UAT Report (clean start, build number, environment banner)"},
    {"id": 15, "feature": "Session history preserved through safety unlock"},
    {"id": 16, "feature": "Long-running request timeout modal (30s threshold)"},
    {"id": 17, "feature": "CI/CD: Website auto-deploy on git push (roadmap propagation)"},
    {"id": 18, "feature": "Fit in iFrame (chathealthy.ai embed)"},
]


BUG_CLASSIFICATIONS = {
    "PE": "Prompt Engineering",
    "DB": "Database Fix",
    "LG": "Logic Fix",
    "MS": "Model Swap",
    "UX": "UX / Frontend Fix",
    "CF": "Configuration Fix",
    "IF": "Infrastructure Fix",
}

ENHANCEMENT_CLASSIFICATIONS = {
    "FT": "New Feature",
    "RF": "Refactor",
    "PF": "Performance",
    "DV": "DevOps",
    "SC": "Security",
}


def _get_uat_status(db, env_prefix: str) -> dict:
    """Read UAT status from MongoDB. Returns {feature_id: {done, bugs_fixed, new_features, notes}}.

    Simple: reads what's there. Claude updates it on commits. Human clears it when needed.
    """
    if db is None:
        return {}
    try:
        coll = db[f"{env_prefix}_System"]["uat_status"]
        doc = coll.find_one({"_id": "current"})
        if not doc or "features" not in doc:
            return {}
        return {f["id"]: f for f in doc["features"]}
    except Exception:
        pass
    return {}

def build_uat_welcome(get_db_fn=None) -> str:
    """Build UAT welcome message. Reads all config from environment and MongoDB.

    Simple: HUMAN_TESTING=true shows report. Claude maintains the data.
    """
    import os
    env_prefix = os.getenv("ENV_PREFIX", "dev")
    version = os.getenv("APP_VERSION", "unknown")
    env = env_prefix if os.getenv("SPACE_ID") else "local"

    db = get_db_fn() if get_db_fn else None
    build = "?"
    if db:
        # Read from canonical
        # frontEndAdmin.BuildVersions (latest record), not the legacy per-env build_counter.
        try:
            record = db["frontEndAdmin"]["BuildVersions"].find_one(sort=[("from", -1)])
            build = str(record["build"]) if record else "0"
        except Exception:
            pass

    total = len(UAT_FEATURES)
    status = _get_uat_status(db, env_prefix)

    total_done = 0
    total_bugs = 0
    total_features = 0

    lines = [
        f"**UAT Session**\n\n"
        f"| Environment | Release | Build | Scenarios |\n"
        f"|:-----------:|:-------:|:-----:|:---------:|\n"
        f"| {env.upper()} | {version} | {build} | {total} |\n\n",
        "| # | Feature | Done | Bugs Fixed | New Features |",
        "|:---:|---------|:----:|:----------:|:------------:|",
    ]

    for f in UAT_FEATURES:
        s = status.get(f["id"], {})
        done = s.get("done", "")
        bugs = s.get("bugs_fixed", 0)
        features = s.get("new_features", 0)
        if done:
            total_done += 1
        total_bugs += bugs
        total_features += features
        lines.append(
            f"| {f['id']:>3} | {f['feature']} | {done} | {bugs or ''} | {features or ''} |"
        )

    lines.append(
        f"| | **TOTALS** | **{total_done}/{total}** | **{total_bugs}** | **{total_features}** |"
    )

    # Notes from DB
    has_notes = [(f, status.get(f["id"], {})) for f in UAT_FEATURES if status.get(f["id"], {}).get("notes")]
    if has_notes:
        lines.append("\n**Notes:**\n")
        for f, s in has_notes:
            lines.append(f"- **#{f['id']} {f['feature']}**: {s['notes']}")

    lines.append("\n**Bug Classifications:** " + " | ".join(f"**{k}**: {v}" for k, v in BUG_CLASSIFICATIONS.items()))
    lines.append("\n**Enhancement Classifications:** " + " | ".join(f"**{k}**: {v}" for k, v in ENHANCEMENT_CLASSIFICATIONS.items()))
    lines.append(
        "\n*human: mark Done (Y/DEF/FAIL/OOS), tally bugs and features as you test. Note type codes in chat.*"
    "\n*Y = passed | DEF = deferred to future release | FAIL = failed | OOS = out of scope for this release*"
    "\n*Every requirement must carry exactly one label. No requirement may be left blank.*"
    )

    return "\n".join(lines)
