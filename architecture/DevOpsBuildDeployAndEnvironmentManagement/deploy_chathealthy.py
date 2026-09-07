"""deploy_chathealthy.py — the unified deploy entry point.

Replaces the legacy local_deploy.py / remote_deploy.py pair. One script,
one CLI surface, internally dispatches by GITHUB_ACTIONS context and --env.

CLI:
  python deploy_chathealthy.py --env {local|dev|qa|prod} [--target <list>] [--tests <test_list>]

Behaviour by --env:
  local        — runs LocalDeploy (today's local stack stand-up: Docker
                 containers + host-OS Website wrapper).
  dev|qa|prod  — ships per-target packages from localBuild/<target_id>/
                 to cloud destinations (Cloudflare Pages, HuggingFace Spaces,
                 Azure Function App, etc.).

Three-check staleness gate (per build_deploy_promote_plan v3 §C.4) runs
BEFORE any handler is dispatched. Any failed check rejects the deploy
with a fix-it message naming the stale fact:
  (a) git_head_sha  — manifest's git_head_sha must equal current HEAD short SHA
                      [non-local envs only; local builds source from working
                      tree, not HEAD, so SHA drift does not invalidate]
  (b) env           — manifest.env must equal --env
  (c) build counter — manifest.build must equal frontEndAdmin.BuildVersions.latest.build
                      [non-local envs only; --env local does not bump and
                      the counter relationship is not load-bearing]

Reference: build_deploy_promote_plan v3 §C.2 (deploy responsibilities),
§C.4 (staleness gate), §C.7 (push-outcome reporting), §C.8 (smoke policy),
§INV-4 (env-mismatch rejection).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _deploy_chain import (  # noqa: E402
    LocalStandUp,
    run_cloud_deploy,
    BUILD_ROOT_REL,
)
from devops_identity import establish_azure_identity  # noqa: E402
from cert_placement import bake_ca_chain_into_images  # noqa: E402

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
_CH_LOG = ChatHealthyLoggingService()


VALID_ENVS = ("local", "dev", "qa", "prod")
_ENV_BRANCH = {"local": "dev", "dev": "dev", "qa": "qa", "prod": "main"}


def _chathealthy_exception():
    """ChatHealthyException, imported without assuming the library is on the
    path. These scripts run as bare entry points before any package setup."""
    import sys as _sys, pathlib as _pl
    for _p in _pl.Path(__file__).resolve().parents:
        if (_p / ".git").exists():
            _lib = _p / "ChatHealthyLib" / "src"
            if str(_lib) not in _sys.path:
                _sys.path.insert(0, str(_lib))
            break
    from chathealthy_lib.exceptions import ChatHealthyException
    return ChatHealthyException


def _repo_root() -> Path:
    import chain_provenance as _cp  # noqa: PLC0415
    return _cp.repository_root(Path(__file__))


def _current_branch(repo_root: Path) -> str:
    return subprocess.run(
        ["git", "symbolic-ref", "--short", "HEAD"],
        cwd=str(repo_root), capture_output=True, text=True, check=True,
    ).stdout.strip()


def _enforce_env_branch_check(repo_root: Path, env: str) -> None:
    """No-op (retained for callsite compatibility).

    Operator directive 2026-08-04: deploy for env X depends only on
    X's build output at <repo>/build/<target_id>/. It does not read
    the working tree, does not care about the current branch, and
    MUST NOT force a git checkout. The build script has already
    materialised the correct source (from origin/<branch> for cloud
    envs; from the working tree for local) and produced the artifacts
    the deploy will ship."""
    return


def _current_head_sha(repo_root: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=str(repo_root), capture_output=True, text=True, check=True,
    ).stdout.strip()


def _latest_admin_build() -> int | None:
    from dotenv import load_dotenv
    from pymongo import MongoClient

    load_dotenv(_repo_root() / ".env")
    try:
        from chathealthy_lib.mongo_utilities import ChatHealthyMongoUtilities
        from cluster_host import host_for as _cluster_host
        latest = (ChatHealthyMongoUtilities()
                  .getConnection("DevOpsUser", "ChatHealthyFrontEnd",
                                 host=_cluster_host("ChatHealthyFrontEnd"))
                  ["frontEndAdmin"]["BuildVersions"].find_one(sort=[("from", -1)]))
    except Exception:
        return None
    if latest is None:
        return None
    return latest.get("build")


def _staleness_gate(repo_root: Path, env: str, target_ids: list[str],
                    packages: set[str] | None = None) -> None:
    """Three-check staleness gate per plan v3 SS C.4. Fails fast on any
    manifest mismatch in a package being deployed. For --env local, skip
    checks (a) and (c) per INV-1 (working-tree source).

    The gate used to read pkgs[0] -- whichever package the manifest happened
    to list first -- and judge the whole target by it. That is wrong in both
    directions: a current build of the selected package was refused because an
    unrelated package was stale, and a stale selected package would have
    shipped whenever the first one happened to be current. Every package being
    deployed is checked now, and only those.
    """
    head_sha = _current_head_sha(repo_root) if env != "local" else None
    latest_build = _latest_admin_build() if env != "local" else None

    for target_id in target_ids:
        from _deploy_chain import _target_packages, package_build_facts
        pkgs = _target_packages(repo_root, target_id)
        if not pkgs:
            continue  # provisioned target: no bytes, nothing to stale-check
        if packages:
            pkgs = [p for p in pkgs if p in packages]
            if not pkgs:
                continue  # nothing from this target is being deployed

        for pkg in pkgs:
            data = package_build_facts(repo_root, target_id, pkg)

            manifest_env = data.get("env")
            if manifest_env != env:
                raise ChatHealthyException(
                    mode="aborted",
                    component="deploy_chathealthy",
                    message=f"ERROR: build was for env {manifest_env!r}, deploy "
                    f"requested {env!r} (target={target_id}, package={pkg}); "
                    f"rebuild for {env}")

            if env == "local":
                continue

            manifest_sha = data.get("git_head_sha")
            if manifest_sha and manifest_sha != head_sha:
                raise ChatHealthyException(
                    mode="aborted",
                    component="deploy_chathealthy",
                    message=f"ERROR: build at {manifest_sha} does not match current "
                    f"checkout {head_sha} (target={target_id}, package={pkg}); "
                    f"rebuild with build_chathealthy.py --env {env}")

            if latest_build is not None:
                manifest_build = data.get("build")
                if (manifest_build is not None
                        and int(manifest_build) != int(latest_build)):
                    raise ChatHealthyException(
                        mode="aborted",
                        component="deploy_chathealthy",
                        message=f"ERROR: build_number {manifest_build} is older than "
                        f"frontEndAdmin.BuildVersions latest {latest_build} for "
                        f"env {env} (target={target_id}, package={pkg}); rebuild")


def _collect_target_ids_for_env(repo_root: Path, env: str, target_arg: str) -> list[str]:
    """Mirror local_deploy._select_target_ids logic minus the env filter.
    For staleness-gate purposes we need the set of target_ids the deploy
    is going to touch."""
    from target_record import DeploymentCollection
    from record_loader import RecordLoader
    brain_path = repo_root / "brain" / "machine_artifacts" / "content" / "deployment_architecture.json"
    load_filter = (target_arg
                   if target_arg.startswith("target_")
                   and "," not in target_arg else None)
    coll: DeploymentCollection = RecordLoader().load_collection(
        brain_path, target_id_filter=load_filter,
    )
    from _deploy_chain import select_target_ids
    selected = select_target_ids(coll, target_arg)
    out = []
    for tid, _kind in selected:
        target = coll.by_target_id(tid)
        if env in target.env_binding_set():
            out.append(tid)
    return out


def _provision_pipeline_certs(
    repo_root: Path, env: str, target_ids: list[str]
) -> None:
    """F-012 §7.1 pipeline-cert provisioning bridge. Runs BEFORE the
    substrate-specific deploy so ACR builds see the CA chain baked into
    the image and ACA Jobs find their per-node cert at KV boot time.

    No-op if no Azure pipeline target is in the current deploy set.
    Idempotent: skips certs already at the KV node-scoped path and not
    near expiry (rotation window handled inside cert_placement)."""
    if not any(t.startswith("target_azure_") for t in target_ids):
        return
    from record_loader import RecordLoader
    brain_path = (
        repo_root / "brain" / "machine_artifacts" / "content"
        / "deployment_architecture.json"
    )
    coll = RecordLoader().load_collection(brain_path)
    kv_target = coll.by_target_id("target_azure_key_vault_pipeline")
    acr_target = coll.by_target_id("target_azure_container_registry_pipeline")
    if kv_target is None or acr_target is None:
        raise ChatHealthyException(
            mode="aborted",
            component="deploy_chathealthy",
            message="ERROR: manifest missing target_azure_key_vault_pipeline or "
            "target_azure_container_registry_pipeline. F-012 §7 cannot "
            "run without both.")
    # Certificate issuance and the vault grant/revoke that follows it are
    # entitlement work, not deploy work, and belong to claudeCodeAgent. The
    # deploy reads the two public CA certs and bakes the trust chain into
    # the images; it mints nothing and grants nothing.
    bake_ca_chain_into_images(env=env, acr_target=acr_target)


def _run_tests(env: str, tests: list[str]) -> int:
    """Run the named tests after the deploy completes. SMOKE_TEST_ENV is
    set so the test modules pick up the right URL set."""
    if not tests:
        return 0
    test_map = {
        "ur_um_regression": "architecture/DevOpsBuildDeployAndEnvironmentManagement/findcare_ur_um_regression_test.py",
        "fire_provider_pipeline": "architecture/DevOpsBuildDeployAndEnvironmentManagement/fire_provider_pipeline_test.py",
    }
    test_paths = []
    for name in tests:
        path = test_map.get(name)
        if not path:
            _CH_LOG.info(f"WARN: unknown test {name!r}; skipping")
            continue
        test_paths.append(path)
    if not test_paths:
        return 0
    cmd = ["python", "-m", "pytest"] + test_paths + ["-v"]
    env_dict = dict(os.environ)
    env_dict["SMOKE_TEST_ENV"] = env
    _CH_LOG.info(f"[deploy] running tests: {tests} against env={env}")
    return subprocess.run(cmd, env=env_dict).returncode


_LOCAL_STACK_KINDS = ("hf_space", "cloudflare_pages_project")


def _is_local_manifest_target(repo_root: Path, target_arg: str) -> bool:
    """True when --target names a target that env local installs directly.

    The application services and the website are stood up as containers by
    LocalStandUp; a host_os_process target is installed from its manifest.

    This used to mean "names a target that binds to local", which was
    right while only host_os_process targets bound to local. Merging the
    docker_local twins into the real targets gave the application targets
    a local binding too, so naming one of them sent it down the cloud
    deploy path and it failed looking for a git branch.
    """
    if "," in target_arg:
        return False
    import json
    brain = (repo_root / "brain" / "machine_artifacts" / "content"
             / "deployment_architecture.json")
    if not brain.is_file():
        return False
    doc = json.loads(brain.read_text(encoding="utf-8"))

    def walk(o):
        if isinstance(o, dict):
            if "target_id" in o and "environments" in o:
                yield o
            for v in o.values():
                yield from walk(v)
        elif isinstance(o, list):
            for v in o:
                yield from walk(v)

    for rec in walk(doc):
        if rec.get("target_id") != target_arg:
            continue
        if rec.get("target_kind") in _LOCAL_STACK_KINDS:
            return False        # LocalStandUp owns these
        return any(e.get("env_binding") == "local"
                   for e in rec.get("environments") or [])
    return False


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    """The command line this program accepts."""
    parser = argparse.ArgumentParser(
        description="Deploy per-target packages: --env local stands up the local "
                    "host stack; --env dev|qa|prod ships cloud targets."
    )
    parser.add_argument("--env", required=True, choices=VALID_ENVS)
    parser.add_argument(
        "--target", required=True,
        help="'pipeline' | 'cloudflare' | 'hf' | 'azure' | 'aca' | 'all' "
             "(front-end stack) | a specific target_id. When --target names "
             "a GROUP ('pipeline'/'cloudflare'/'hf'/'azure'/'aca'/'all'), "
             "--package MUST be explicitly enumerated — no shortcut for "
             "'all packages' exists on purpose. If you want to deploy "
             "every runbook under a group, name every package. "
             "EPIC-008-F-012: 'pipeline' MUST NOT be "
             "combined with other target values.",
    )
    parser.add_argument(
        "--package", default="",
        help="Comma-separated list of package_id values. MANDATORY when "
             "--target is a group name; forbidden shortcut: no way to say "
             "'all packages under this target' — you must name each one. "
             "Optional when --target names a specific target_id. Example: "
             "--target pipeline --package provider_pipeline,reservation_reaper",
    )
    parser.add_argument(
        "--tests", default="",
        help="Comma-separated list of test names to run after deploy "
             "(e.g. 'ur_um_regression'). Empty by default.",
    )
    return parser.parse_args(argv)


def _establish_context() -> Path:
    """Act as DevOpsUser and tell hf_helpers which tree holds the manifest.

    The deploy acts as the declared identity rather than whoever is
    logged in, and hf_helpers will not resolve the manifest from
    __file__ because during a cloud build that reads the
    workstation's uncommitted deployment facts.
    """

    # The deploy acts as DevOpsUser, not as whoever is logged in at the
    # terminal. Establishing it here governs every `az` subprocess the chain
    # spawns downstream, at every call site.
    establish_azure_identity("DevOpsUser")

    repo_root = _repo_root()
    # hf_helpers refuses to load the manifest until it is told which tree to
    # read it from -- it will not resolve that from __file__, because during a
    # cloud BUILD that would read the workstation's uncommitted deployment
    # facts. Only the build ever set it, so every HF deploy raised. A deploy
    # runs on the workstation against build output, and the manifest it needs
    # is this repository's.
    import hf_helpers as _hf
    # The manifest comes from the tree the running code came from. On the
    # workstation that is the workstation; in a re-executed run it is the
    # branch checkout, which is what the deploy installs. repo_root stays the
    # workstation because .env and the build output live there.
    _hf.set_build_source(Path(__file__).resolve().parents[2])

    return repo_root


def _refuse_bad_selection(args) -> None:
    """Refuse a selection the operator did not actually make."""
    # EPIC-008-F-012: reject any attempt to combine the
    # 'pipeline' selector with another target value. Comma-separated
    # multi-target strings are the only shape this rule needs to block —
    # a single specific target_id (like 'target_atlas_pipeline') is
    # legal because it names one target, not a combination.
    tokens = [t.strip() for t in args.target.split(",") if t.strip()]
    if "pipeline" in tokens and len(tokens) > 1:
        raise ChatHealthyException(
            mode="aborted",
            component="deploy_chathealthy",
            message="ERROR: --target=pipeline MUST be the sole target. Pipeline "
            "deploys are independent of front-end deploys "
            "(EPIC-008-F-012).")

    # Force explicit --package enumeration when --target is a group name.
    # Design intent: the operator must think through and TYPE every
    # package being deployed. No blanket 'all packages under this group'
    # shortcut exists — the friction is the feature. Prevents accidental
    # 10-runbook shotgun deploys when a single package was intended.
    # 'all' is gone from both build and deploy. Every deploy names the
    # destinations and the capabilities inside them; a selector meaning
    # "everything" is what turned every change into a whole-estate deploy.
    if args.target.strip() == "all":
        raise ChatHealthyException(
            mode="aborted",
            component="deploy_chathealthy",
            message="ERROR: --target='all' no longer exists. Name the target_id(s) "
            "(comma-separated) and the package(s) you intend to deploy.")
    _GROUP_TARGETS = frozenset({"pipeline", "cloudflare", "hf", "azure", "aca"})


def _declared_package_set(repo_root: Path, target_ids: list[str]) -> set[str]:
    """Every package the named targets declare, read from the record.

    The deploy used to require the operator to type this list, which is a
    second declaration of a fact deployment_architecture.json already holds.
    Two declarations of one fact drift, and the one that drifts silently is
    the one nothing validates -- a package could be declared on a target and
    simply never deployed, because the list the operator typed did not
    mention it and nothing compared the two.
    """
    from record_loader import RecordLoader  # noqa: PLC0415
    from _build_chain import _declared_packages  # noqa: PLC0415
    brain = (repo_root / "brain" / "machine_artifacts" / "content"
             / "deployment_architecture.json")
    coll = RecordLoader().load_collection(brain)
    wanted: set[str] = set()
    for t in coll.records:
        if t.target_id in target_ids:
            wanted.update(_declared_packages(t))
    return wanted


def _ordered_packages(repo_root: Path, target_ids: list[str],
                      selection: set[str]) -> list[str]:
    """The selected packages, in the order their dependencies require.

    Derived from the declarations, never written into this program. A deploy
    that installs a package before the one it requires has installed
    something that cannot work, and finding that out at runtime is the whole
    cost this avoids.
    """
    from record_loader import RecordLoader  # noqa: PLC0415
    from _build_chain import package_install_order  # noqa: PLC0415
    brain = (repo_root / "brain" / "machine_artifacts" / "content"
             / "deployment_architecture.json")
    coll = RecordLoader().load_collection(brain)
    order: list[str] = []
    for t in coll.records:
        if t.target_id not in target_ids:
            continue
        for pid in package_install_order(t):
            if pid in selection and pid not in order:
                order.append(pid)
    for pid in sorted(selection):
        if pid not in order:
            order.append(pid)
    return order


def _packages_for(args, repo_root: Path, target_ids: list[str]) -> set[str]:
    """What this deploy installs.

    The record states which packages a target CAN carry. Which of them this
    deployment installs is the operator's decision and cannot be derived --
    a schema change has no business reshipping the pages beside it.

    So a subset is legitimate and narrows the deploy. What is not legitimate
    is naming a package the target does not declare: that is a second, and
    contradictory, statement of what the target carries.
    """
    declared = _declared_package_set(repo_root, target_ids)
    supplied = {p.strip() for p in (args.package or "").split(",") if p.strip()}
    undeclared = supplied - declared if declared else set()
    if undeclared:
        raise ChatHealthyException(
            mode="illegal_state",
            component="deploy_chathealthy",
            message=f"ERROR: --package={sorted(undeclared)} names package(s) "
            f"these targets do not declare. They carry {sorted(declared)}. "
            f"The record states which packages a target carries; a command "
            f"line that states it differently is a second declaration of one "
            f"fact. Name only packages the target declares, or drop --package "
            f"and the deploy installs every one of them.")
    return supplied or declared


def _filter_to_selected_packages(target_ids: list[str],
                                 pkg_selection: set[str],
                                 target: str, package: str) -> list[str]:
    """Drop per-package targets the operator did not name.

    Host targets -- vault, storage, network, identities, registry, the
    container-app environment, the automation account, the resource group --
    pass through so their shells stay verified. Only the synthetic
    per-package targets are filtered, because those are the capabilities the
    operator enumerated.
    """
    prefixes = ("target_azure_automation_runbook_",
                "target_azure_container_app_job_")
    kept: list[str] = []
    for tid in target_ids:
        prefix = next((x for x in prefixes if tid.startswith(x)), None)
        if prefix is None:
            kept.append(tid)
        elif tid[len(prefix):] in pkg_selection:
            kept.append(tid)
    if not kept:
        raise ChatHealthyException(
            mode="aborted",
            component="deploy_chathealthy",
            message=f"--package={package!r} matched no packages under "
                    f"--target={target!r}")
    _CH_LOG.info(f"[local_deploy] --package filter: {len(kept)} targets "
                 f"remain (packages selected: {sorted(pkg_selection)})")
    return kept


def _confirm_secrets_agree(repo_root: Path, env: str, target_ids: list[str],
                           pkg_selection: set[str] | None) -> None:
    """The vault's copy and the workstation's fair copy hold the same bytes.

    The build already confirmed these exist. A deploy is about to install
    them, so it asks the stricter question. Two copies of one credential that
    nobody compares will drift, and the first sign of it is a service
    authenticating with a value its peer has already retired.

    Before anything is pushed. The earlier shape found a secret problem after
    the image was built and after the target's variables had been written,
    which left the target half-changed.
    """
    import secret_preflight as _secret_preflight  # noqa: PLC0415
    from record_loader import RecordLoader as _RL2  # noqa: PLC0415
    _secret_preflight.confirm_secrets_match_local(
        _RL2().load_collection(
            repo_root / "brain" / "machine_artifacts" / "content"
            / "deployment_architecture.json"),
        env, repo_root,
        target_ids=list(target_ids) or None,
        packages=pkg_selection or None)


def _run_from_branch_checkout(env: str, argv):
    """Hand a cloud deploy over to the same program checked out of its branch.

    Returns the child's exit code, or None when this process carries on.
    """
    import chain_provenance as _cp  # noqa: PLC0415
    return _cp.reexec_from_branch(
        env,
        "architecture/DevOpsBuildDeployAndEnvironmentManagement/deploy_chathealthy.py",
        list(argv if argv is not None else sys.argv[1:]))


def _authorize_deployment(repo_root: Path, args):
    """EPIC-008-F-012-S-004-REQ-B-006. Ask a person, before anything is deployed.

    Returns (worker, record id), or (None, None) for a local deploy, or
    (worker, None) when the operator refused.

    Local is exempt, and the requirement says why: a local deploy must be
    accomplishable by code bots without human intervention. What the approval
    governs is code leaving this machine for somewhere that serves people. A
    page in front of the inner loop would also teach the operator to dismiss
    the same page that guards dev, qa and prod.
    """
    if args.env == "local":
        return None, None
    import sys as _sys  # noqa: PLC0415
    _sys.path.insert(
        0, str(repo_root / "architecture" / "EngineeringRuleEnforcement" / "code"))
    from deploy_authorization_worker import (  # noqa: PLC0415
        DeployAuthorizationWorker, DeploymentFacts)
    targets = [t.strip() for t in args.target.split(",") if t.strip()]
    # The page the operator approves names what the record says is being
    # installed, not what was typed. Approving a list the deploy does not
    # use is an approval of nothing.
    selection = _packages_for(args, repo_root, targets)
    packages = _ordered_packages(repo_root, targets, selection)
    build_number, commit = _build_identity(repo_root, targets, packages)
    # Each target names only what IT is receiving. The union across targets
    # told the operator a target was getting packages it does not carry, and
    # an approval of a description the deploy does not follow approves nothing.
    per_target = {
        t: [p for p in _ordered_packages(repo_root, [t],
                                         selection & _declared_package_set(repo_root, [t]))]
        for t in targets}
    worker = DeployAuthorizationWorker(DeploymentFacts(
        environment=args.env, targets=targets,
        packages=per_target,
        build_number=build_number, commit=commit))
    return worker, worker.authorize()


def _build_identity(repo_root: Path, targets: list[str],
                    packages: list[str]) -> tuple[int, str]:
    """The build and commit the operator is being asked to approve.

    Read off the staged package rather than off git, because what is about to
    be installed is the build output, and a working tree can have moved since.
    """
    from _deploy_chain import package_build_facts  # noqa: PLC0415
    for target_id in targets:
        for package in packages:
            try:
                facts = package_build_facts(repo_root, target_id, package)
            except Exception:  # noqa: BLE001 - a target may not carry it
                continue
            if facts:
                return int(facts.get("build") or 0), str(facts.get("commit") or "")
    return 0, ""


def _deploy_to_cloud(args, repo_root: Path) -> int:
    """Select the targets the operator named and drive them to running state."""
    target_ids = _collect_target_ids_for_env(repo_root, args.env, args.target)
    if not target_ids:
        raise ChatHealthyException(
            mode="aborted",
            component="deploy_chathealthy",
            message=f"ERROR: no targets matched --env={args.env} --target={args.target!r}")
    # --package filter: drop synth per-package targets (runbook/job) whose
    # package_id is not in the selected set. Host targets (KV, Storage, VNet,
    # MIs, ACR, ACA env, AA, RG) pass through so their shells stay verified.
    pkg_selection = _packages_for(args, repo_root, target_ids)
    if pkg_selection:
        target_ids = _filter_to_selected_packages(
            target_ids, pkg_selection, args.target, args.package)
    _staleness_gate(repo_root, args.env, target_ids, pkg_selection)
    _confirm_secrets_agree(repo_root, args.env, target_ids, pkg_selection)
    # Cert placement runs inside run_cloud_deploy after CA runbooks and before
    # ACA Jobs (dependency order); not called here, CaEndpointRunbook must
    # already be published. The filtered target_ids and pkg_selection are
    # passed so --package is honoured end to end rather than re-enumerated.
    return run_cloud_deploy(args.env, args.target,
                            explicit_target_ids=target_ids,
                            package_selection=pkg_selection or None)


def _record_what_landed(args, repo_root: Path) -> None:
    """Record which build of which package now sits on which target.

    version_counter has carried record_package_deploy since the counter was
    written, and nothing has ever called it: every row in PackageBuilds says
    'built' and none says 'deployed'. So the question the docstring there
    promises to answer -- which build of this package is live in prod --
    could only be answered by asking prod, which is the thing a record
    exists to avoid.

    Recorded after the deploy returned success, never before: a row saying a
    package landed when it did not is worse than no row.
    """
    from _deploy_chain import package_build_facts  # noqa: PLC0415
    from version_counter import record_package_deploy  # noqa: PLC0415
    targets = [t.strip() for t in args.target.split(",") if t.strip()]
    packages = _ordered_packages(repo_root, targets,
                                 _packages_for(args, repo_root, targets))
    for target_id in targets:
        for package in packages:
            try:
                facts = package_build_facts(repo_root, target_id, package)
            except Exception:  # noqa: BLE001 - target may not carry this package
                continue
            if not facts:
                continue
            record_package_deploy(target_id, package,
                                  int(facts.get("build") or 0), args.env)
            _CH_LOG.info(f"[deploy] recorded deployed {target_id}/{package} "
                         f"build={facts.get('build')} env={args.env}")


def _execute_deploy(args, repo_root: Path, worker, approval) -> int:
    """Run the deploy and record what happened to it.

    The outcome is written in a finally, so a deployment that raises still
    records that it ran and failed. An authorization with no outcome beside
    it says a deployment was approved and nothing about whether it happened.
    """
    rc = 1
    try:
        if args.env == "local" and not _is_local_manifest_target(repo_root, args.target):
            # LocalStandUp owns the local stack lifecycle and consumes no
            # per-target manifest, so it runs only when --target names none.
            rc = LocalStandUp().run()
        else:
            rc = _deploy_to_cloud(args, repo_root)
    finally:
        if worker is not None:
            worker.record_outcome(approval, "succeeded" if rc == 0 else "failed")
    if rc == 0:
        _record_what_landed(args, repo_root)
    return rc


def main(argv: list[str] | None = None) -> int:
    """Drive the deploy and report its status."""
    # The workstation's .env carries the facts a connection needs, and every
    # env reaches Mongo. It was loaded inside the build-number lookup, which
    # only cloud deploys call, so a local deploy reached the cluster with
    # nothing telling it where the vault is.
    from dotenv import load_dotenv
    load_dotenv(_repo_root() / ".env")
    args = _arguments(argv)
    from_git = _run_from_branch_checkout(args.env, argv)
    if from_git is not None:
        return from_git
    repo_root = _establish_context()
    _refuse_bad_selection(args)
    # Nothing is deployed from a manifest that does not satisfy the published
    # schema. The deploy validated only inside one selection helper, so
    # whether it happened depended on which path ran.
    #
    # Validate the manifest being DEPLOYED, which for dev, qa and prod is the
    # branch checkout this process is running from -- not the workstation.
    # repository_root() answers with the workstation on purpose, because that
    # is where .env and the build output live, and validation borrowed that
    # answer. The effect was that an uncommitted edit on the workstation could
    # fail a deploy of committed code it would never ship, which is the same
    # confusion the build resolved with _get_build_source.
    from record_loader import RecordLoader as _RL
    _RL.validate_architecture(Path(__file__).resolve().parents[2])
    _enforce_env_branch_check(repo_root, args.env)

    worker, approval = _authorize_deployment(repo_root, args)
    if worker is not None and approval is None:
        _CH_LOG.info(f"[deploy] {worker.last_verdict}: nothing deployed, "
                     f"recorded as a rejected deployment")
        return 3

    rc = _execute_deploy(args, repo_root, worker, approval)
    if rc != 0:
        return rc
    return _run_tests(args.env,
                      [t.strip() for t in args.tests.split(",") if t.strip()])


if __name__ == "__main__":
    sys.exit(main())
