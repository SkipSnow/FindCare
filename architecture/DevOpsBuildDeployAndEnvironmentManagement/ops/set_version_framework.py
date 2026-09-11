# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""Human-driven update of version and/or framework in frontEndAdmin.BuildVersions.

The build number is global, in frontEndAdmin.BuildVersions, and is bumped at build time
(see DeploymentArchitectureDesignAndMigrationPlanPhase_v14.docx section 2.1 build_chathealthy.py).
version and framework are set ONLY by humans, only at
prod UAT sign-off. Claude invokes this routine when the operator authorizes
a version or framework update.

Pattern matches the build-time bump in build_chathealthy.py:
    1. Read latest frontEndAdmin.BuildVersions record.
    2. Insert a new record with the supplied version and/or framework, the
       previous `builds` array copied forward unchanged (per-env build slots
       are not touched by this routine), and a fresh `from` timestamp.

Usage:
    python set_version_framework.py --version 0.5.0
    python set_version_framework.py --framework 0.2.0
    python set_version_framework.py --version 0.5.0 --framework 0.2.0

At least one of --version or --framework MUST be supplied.
"""

import argparse

import os
import sys
from datetime import datetime, timezone

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
from chathealthy_lib.exceptions import ChatHealthyException
from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))


log = ChatHealthyLoggingService()

PUSH_TIMEOUT_SECONDS = 5


VALID_ENVS = ("local", "dev", "qa", "prod")



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


def _carry_forward_builds(builds_array):
    """Return the input builds array filtered + ordered. No values change."""
    by_env = {}
    for entry in builds_array or []:
        env = entry.get("env")
        if env in VALID_ENVS:
            by_env[env] = int(entry["build"])
    return [{"env": e, "build": by_env[e]} for e in VALID_ENVS if e in by_env]


def set_version_framework(version: str | None, framework: str | None) -> dict:
    """Insert a new frontEndAdmin.BuildVersions record with the supplied changes.

    The `builds` array is copied forward unchanged from the latest record;
    this routine only updates version and/or framework.

    Returns the inserted record dict (builds, version, framework, from).
    """
    if version is None and framework is None:
        raise ChatHealthyException(
            mode="value_error",
            component="SetVersionFramework",
            message="must supply at least one of version, framework")

    client = _devops_connection()
    coll = client["frontEndAdmin"]["BuildVersions"]
    latest = coll.find_one(sort=[("from", -1)])
    if latest is None:
        raise ChatHealthyException(
            mode="versions_collection_empty",
            component="SetVersionFramework",
            message="frontEndAdmin.BuildVersions has no records. Run "
                    "seed_versions_collection.py first.")

    carried_builds = _carry_forward_builds(latest.get("builds", []))
    if not carried_builds:
        raise ChatHealthyException(
            mode="versions_record_missing_builds",
            component="SetVersionFramework",
            message="latest frontEndAdmin.BuildVersions record has no per-env builds "
                    "array; run migrate_versions_to_per_env.py first.")

    record = {
        "builds": carried_builds,  # preserved
        "version": version if version is not None else latest["version"],
        "framework": framework if framework is not None else latest["framework"],
        "from": datetime.now(timezone.utc).isoformat(),
    }
    coll.insert_one(record)
    return record




def main():
    parser = argparse.ArgumentParser(
        description="Update version and/or framework in frontEndAdmin.BuildVersions"
    )
    parser.add_argument("--version", help="New version ID (e.g., 0.5.0)")
    parser.add_argument("--framework", help="New framework ID (e.g., 0.2.0)")
    args = parser.parse_args()

    if not args.version and not args.framework:
        parser.error("must supply at least one of --version, --framework")

    record = set_version_framework(args.version, args.framework)
    log.info("Inserted: builds=%s version=%s framework=%s",
             record["builds"], record["version"], record["framework"])


if __name__ == "__main__":
    main()
