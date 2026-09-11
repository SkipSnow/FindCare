# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# One-shot migration: scalar `build` -> per-env `builds` array on frontEndAdmin.BuildVersions.
#
# Reads the latest frontEndAdmin.BuildVersions record in the front-end cluster's admin DB
# (frontEndAdmin.BuildVersions). If the record already has a `builds` array, exits 0 with
# a notice (idempotent). Otherwise:
#   * carries the scalar `build: int` value into all three env slots
#     [{"env": "dev", "build": N}, {"env": "qa", "build": N}, {"env": "prod", "build": N}]
#   * inserts a NEW record (the collection grows by record; we do not modify
#     the old document in place)
#   * preserves version, framework, and stamps a fresh `from` timestamp.
#
# Run:
#   python Code/Shared/ops/migrate_versions_to_per_env.py
#
# Exit codes:
#   0 = migrated (or already migrated, idempotent no-op)
#   1 = environment / preconditions wrong
#   2 = unexpected data shape in the latest record


import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient

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

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))


log = ChatHealthyLoggingService()

ENV_ORDER = ("local", "dev", "qa", "prod")



def _devops_connection():
    """The DevOps identity, by certificate. Rule-004: no MongoClient here.

    Operator tooling authenticates as DevOpsUser like everything else in the
    devops chain. It used to open a MongoClient on a SCRAM connection string,
    which is a fourth credential outside the three-identity model and outside
    Rule-004's scan scope, so nothing caught it.
    """
    import sys as _sys, pathlib as _pl
    _src = _pl.Path(__file__).resolve()
    for _p in _src.parents:
        if (_p / ".git").exists():
            _lib = _p / "ChatHealthyLib" / "src"
            if str(_lib) not in _sys.path:
                _sys.path.insert(0, str(_lib))
            from dotenv import load_dotenv as _ld
            _ld(_p / ".env", override=False)
            break
    from chathealthy_lib.mongo_utilities import ChatHealthyMongoUtilities
    return ChatHealthyMongoUtilities().getConnection("DevOpsUser", "ChatHealthyFrontEnd")


def _looks_like_builds_array(value) -> bool:
    if not isinstance(value, list) or not value:
        return False
    for entry in value:
        if not isinstance(entry, dict):
            return False
        if "env" not in entry or "build" not in entry:
            return False
    return True


def _migrate(client):
    """Perform the migration and report it."""
    coll = client["frontEndAdmin"]["BuildVersions"]

    latest = coll.find_one(sort=[("from", -1)])
    if latest is None:
        log.error("frontEndAdmin.BuildVersions has no records; run seed_versions_collection.py first")
        return 1

    if _looks_like_builds_array(latest.get("builds")):
        present = {entry["env"] for entry in latest["builds"]}
        missing = [e for e in ENV_ORDER if e not in present]
        if not missing:
            log.info("frontEndAdmin.BuildVersions latest record already has all required slots; "
                     "nothing to migrate. _id=%s builds=%s",
                     latest.get("_id"), latest.get("builds"))
            return 0
        # Backfill: existing builds preserved; missing envs seeded from dev's
        # current value (so local mirrors dev at seed time, per the per-env
        # counter design).
        existing = {entry["env"]: int(entry["build"]) for entry in latest["builds"]}
        dev_build = existing.get("dev", min(existing.values()) if existing else 0)
        for e in missing:
            existing[e] = dev_build
        new_builds = [{"env": e, "build": existing[e]} for e in ENV_ORDER]
        record = {
            "builds": new_builds,
            "version": latest["version"],
            "framework": latest["framework"],
            "from": datetime.now(timezone.utc).isoformat(),
        }
        result = coll.insert_one(record)
        log.info("Backfilled missing slots %s. New record _id=%s builds=%s",
                 missing, result.inserted_id, new_builds)
        return 0

    scalar_build = latest.get("build")
    if not isinstance(scalar_build, int):
        log.error("latest record has neither a per-env `builds` array nor a "
                  "scalar int `build`; aborting. _id=%s keys=%s",
                  latest.get("_id"), list(latest.keys()))
        return 2

    version = latest.get("version")
    framework = latest.get("framework")
    if version is None or framework is None:
        log.error("latest record is missing version or framework; aborting. "
                  "_id=%s", latest.get("_id"))
        return 2

    new_builds = [{"env": e, "build": int(scalar_build)} for e in ENV_ORDER]
    record = {
        "builds": new_builds,
        "version": version,
        "framework": framework,
        "from": datetime.now(timezone.utc).isoformat(),
    }
    result = coll.insert_one(record)
    log.info("Migrated. New record _id=%s builds=%s version=%s framework=%s "
             "(prior scalar build was %d, _id=%s)",
             result.inserted_id, new_builds, version, framework,
             scalar_build, latest.get("_id"))
    return 0


def main() -> int:
    client = _devops_connection()
    return _migrate(client)


if __name__ == "__main__":
    sys.exit(main())
