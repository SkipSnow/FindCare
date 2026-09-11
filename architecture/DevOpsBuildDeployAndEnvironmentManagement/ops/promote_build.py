# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# Promote a build from one environment to another.
# Copies the build number (does NOT increment) and merges the branch.
# Usage:
#   python promote_build.py --from dev --to qa
#   python promote_build.py --from qa --to prod --confirm-prod
#
# Prod promotion requires --confirm-prod flag (GOV-007).
#
# Per the per-env builds shape:
#   dev -> qa : the qa slot is set to whatever dev's slot is right now.
#   qa  -> prod: the prod slot is set to whatever DEV's slot is right now
#                (NOT qa's). If qa is lagging dev, prod still tracks dev at
#                promotion time — qa continues to lag.

import argparse
import datetime

import os
import sys

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
import sys as _ch_sys, pathlib as _ch_pl  # noqa: E402
for _ch_d in _ch_pl.Path(__file__).resolve().parents:
    if (_ch_d / '.git').exists():
        _ch_lib = _ch_d / 'ChatHealthyLib' / 'src'
        if str(_ch_lib) not in _ch_sys.path:
            _ch_sys.path.insert(0, str(_ch_lib))
        break
from chathealthy_lib.exceptions import ChatHealthyException  # noqa: E402

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))


log = ChatHealthyLoggingService()

VALID_PROMOTIONS = {
    ("dev", "qa"),
    ("qa", "prod"),
}

BRANCH_MAP = {
    "dev": "dev",
    "qa": "qa",
    "prod": "main",
}

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


def _builds_to_map(builds_array):
    out = {}
    for entry in builds_array or []:
        env = entry.get("env")
        if env in ENV_ORDER:
            out[env] = int(entry["build"])
    return out


def promote(from_env: str, to_env: str, confirm_prod: bool = False, dry_run: bool = False):
    if (from_env, to_env) not in VALID_PROMOTIONS:
        raise ChatHealthyException(
            mode="aborted",
            component="promote_build",
            message=("Invalid promotion: %s -> %s. Valid: dev->qa, qa->prod" % (from_env, to_env,)),
            exit_code=1)

    if to_env == "prod" and not confirm_prod:
        raise ChatHealthyException(
            mode="aborted",
            component="promote_build",
            message="REFUSED: promoting to prod requires --confirm-prod flag (GOV-007)",
            exit_code=1)

    client = _devops_connection()
    coll = client["frontEndAdmin"]["BuildVersions"]

    latest = coll.find_one(sort=[("from", -1)])
    if latest is None:
        raise ChatHealthyException(
            mode="aborted",
            component="promote_build",
            message="frontEndAdmin.BuildVersions has no records — seed first",
            exit_code=1)

    builds_map = _builds_to_map(latest.get("builds", []))
    for required in ENV_ORDER:
        if required not in builds_map:
            raise ChatHealthyException(
                mode="aborted",
                component="promote_build",
                message=("latest frontEndAdmin.BuildVersions record is missing the %r slot; "
                      "run migrate_versions_to_per_env.py first" % (required,)),
            exit_code=1)

    # Promotion sources are explicit and asymmetric:
    #   dev -> qa  : qa <- dev
    #   qa  -> prod: prod <- dev (intentional; qa is not the source)
    source_env_for_value = "dev"
    source_build = builds_map[source_env_for_value]
    target_build_before = builds_map[to_env]

    log.info("Promoting %s -> %s. Source value comes from %r slot = %d "
             "(target %r was %d).",
             from_env, to_env, source_env_for_value, source_build,
             to_env, target_build_before)

    if dry_run:
        log.info("DRY RUN — no changes made")
        log.info("  Would merge %s -> %s", BRANCH_MAP[from_env], BRANCH_MAP[to_env])
        log.info("  Would set frontEndAdmin.BuildVersions %r slot to %d (was %d)",
                 to_env, source_build, target_build_before)
        return

    # Write a new record (collection grows by record; no in-place update).
    new_builds_map = dict(builds_map)
    new_builds_map[to_env] = source_build
    new_builds = [{"env": e, "build": new_builds_map[e]} for e in ENV_ORDER]
    record = {
        "builds": new_builds,
        "version": latest["version"],
        "framework": latest["framework"],
        "from": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    coll.insert_one(record)
    log.info("Wrote new frontEndAdmin.BuildVersions record: builds=%s", new_builds)

    # Branch merge instructions
    src_branch = BRANCH_MAP[from_env]
    dst_branch = BRANCH_MAP[to_env]
    log.info("NEXT STEP: merge %s -> %s", src_branch, dst_branch)
    log.info("  git checkout %s && git merge %s && git push origin %s", dst_branch, src_branch, dst_branch)
    log.info("Promotion complete. %r build %d shipped to %s.",
             source_env_for_value, source_build, to_env)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Promote a build between environments")
    parser.add_argument("--from", dest="from_env", required=True, choices=["dev", "qa"],
                        help="Source environment")
    parser.add_argument("--to", dest="to_env", required=True, choices=["qa", "prod"],
                        help="Target environment")
    parser.add_argument("--confirm-prod", action="store_true",
                        help="Required for prod promotion (GOV-007)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without making changes")
    args = parser.parse_args()

    promote(args.from_env, args.to_env, args.confirm_prod, args.dry_run)
