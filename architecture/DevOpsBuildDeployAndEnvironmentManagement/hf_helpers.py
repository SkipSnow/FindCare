"""hf_helpers.py - HF Space helper library imported by the deploy chain.

Library-only module (no main(), no CLI). Used by build_chathealthy.py and
deploy_chathealthy.py for HF Space target_kind handling. Exposes:

  - HF Space naming + peer URL helpers (_hf_space_name, _hf_peer_url)
  - HF API write helpers (_hf_set_variable, _hf_set_secret)
  - Source-set definitions for each HF Space target (which dirs get
    staged into each Space's docker build context)
  - Filesystem copy helper (_copy_tree) using Builder's exclusion rules
  - React frontend build for FindCare (_build_react_frontend)
  - build_info.json writer for each HF Space (_write_hf_build_info)
"""
from __future__ import annotations

import base64
import functools
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from builder import _EXCLUDE_DIRS, _EXCLUDE_FILE_NAMES, _EXCLUDE_FILE_SUFFIXES
from target_record import TargetRecord
import ch_fonts_inliner

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
from chathealthy_lib.exceptions import ChatHealthyException  # noqa: E402
_CH_LOG = ChatHealthyLoggingService()
def _ch_exc():
    """ChatHealthyException without assuming the library is installed.
    These modules run as bare scripts in the devops chain."""
    import sys as _s, pathlib as _p
    for _d in _p.Path(__file__).resolve().parents:
        if (_d / ".git").exists():
            _l = _d / "ChatHealthyLib" / "src"
            if str(_l) not in _s.path:
                _s.path.insert(0, str(_l))
            break
    from chathealthy_lib.exceptions import ChatHealthyException
    return ChatHealthyException



# ── HF Space identifiers from the manifest ─────────────────────────────
# Each HF target's environments[<env>].huggingface_space.space carries the
# fully-qualified 'org/name' Space identifier for that env_binding. The
# manifest is the source of truth (EPIC-008-F-012/B-009);
# no org/base/prefix rule lives in code anymore.

# Set by the build to the directory it is building FROM. For --env
# dev|qa|prod that is a temp worktree of origin/<branch>; for --env local
# it is the working tree. Never resolved from __file__: this module lives
# on the workstation, so __file__ always points at the local tree, and a
# cloud build would read local, uncommitted deployment facts while
# packaging committed code. The local system is a sandbox for local only.
_BUILD_SOURCE: Path | None = None


def set_build_source(path: Path) -> None:
    _load_manifest.cache_clear()
    global _BUILD_SOURCE
    _BUILD_SOURCE = Path(path)


@functools.lru_cache(maxsize=1)
def _load_manifest() -> dict:
    if _BUILD_SOURCE is None:
        raise ChatHealthyException(
            mode="runtime_error",
            component="hf_helpers",
            message="build source not set; call set_build_source() with the "
                    "directory the build is reading from before loading the "
                    "manifest. Resolving it from __file__ would read the "
                    "workstation's manifest during a cloud build.")
    manifest = _BUILD_SOURCE / "brain" / "machine_artifacts" / "content" / "deployment_architecture.json"
    return json.loads(manifest.read_text(encoding="utf-8"))


def _hf_space_qualified(target_id: str, env: str) -> str:
    """Return the fully-qualified 'org/name' HF Space identifier for the
    given target_id + env_binding, read from the manifest. Fails loud if
    the target / env_binding / huggingface_space block is missing.
    """
    data = _load_manifest()
    for rec in data["DeploymentTargetRecord"]:
        if rec.get("target_id") != target_id:
            continue
        for env_entry in rec.get("environments", []):
            if env_entry.get("env_binding") != env:
                continue
            hs = env_entry.get("huggingface_space")
            if not hs or "space" not in hs:
                raise ChatHealthyException(
            mode="runtime_error",
            component="hf_helpers",
            message=f"manifest target {target_id!r} env_binding {env!r} "
                    f"has no huggingface_space.space — populate it before deploy.")
            return hs["space"]
    raise ChatHealthyException(
            mode="runtime_error",
            component="hf_helpers",
            message=f"manifest has no target {target_id!r} with env_binding {env!r}.")


def _hf_org(target_id: str, env: str) -> str:
    return _hf_space_qualified(target_id, env).split("/", 1)[0]


def _hf_space_name(target_id: str, env: str) -> str:
    return _hf_space_qualified(target_id, env).split("/", 1)[1]


@functools.lru_cache(maxsize=1)
def _hf_default_org() -> str:
    """The operator's HF org. Read from any HF target's huggingface_space
    entry — all HF Spaces in this project sit under the same org. Used by
    the HF API helpers below where the caller has the bare space name but
    needs to assemble the API URL.
    """
    data = _load_manifest()
    for rec in data["DeploymentTargetRecord"]:
        if rec.get("target_kind") != "hf_space":
            continue
        for env_entry in rec.get("environments", []):
            hs = env_entry.get("huggingface_space")
            if hs and "space" in hs:
                return hs["space"].split("/", 1)[0]
    raise ChatHealthyException(
            mode="runtime_error",
            component="hf_helpers",
            message="no hf_space target with a populated huggingface_space.space "
        "in the manifest — cannot infer operator HF org.")


def _hf_peer_url(target_id: str, env: str) -> str:
    """Stable-base-name URL. Retained for callers that still want the
    manifest-stored Space (no build_n suffix). New per-build code should
    use _hf_peer_url_for_build."""
    org, space = _hf_space_qualified(target_id, env).split("/", 1)
    return f"https://{org.lower()}-{space.replace('_', '-').lower()}.hf.space"


def _hf_peer_url_for_build(target_id: str, env: str, build_n: int) -> str:
    """Reverted 2026-06-22: returns the stable-base-name URL. The
    per-build (`_<n>`) Space-naming scheme is retired — HF rate-limits
    Space CREATE calls, and re-deploying to the same Space is the
    standard pattern. `build_n` is accepted for caller compatibility
    but unused."""
    return _hf_peer_url(target_id, env)


# ── Step notice helper ─────────────────────────────────────────────────
def _step(msg: str) -> None:
    _CH_LOG.info(f"[hf_helpers] {msg}")


# ── build_info.json baked into each HF Space ──────────────────────────
def _write_hf_build_info(workspace: Path, target_id: str, env: str) -> None:
    """Write build_info.json at the workspace root so the Dockerfile's
    `COPY build_info.json /app/build_info.json` resolves. /health on each
    backend prefers this file over an admin.Versions Mongo read."""
    service_map = {
        "target_hf_space_findcare_backend":     "ch-findcare",
        "target_hf_space_evaluatecare_backend": "ch-evalcare",
        "target_hf_space_shared_services":      "ch-sharedsvc",
    }
    service = service_map.get(target_id, target_id)
    # Build = the canonical global counter in admin.Versions on the
    # front-end cluster (one int, not per-env per build_deploy_promote_plan
    # v3 §3). Same source build_chathealthy.py uses for image tagging;
    # baking it here keeps image tag, build_info.json, /health, and banner
    # all in lock-step by construction.
    from version_counter import VERSIONS_COLLECTION, VERSIONS_DB, latest_record
    latest = latest_record()
    build_n = latest.get("build")
    if build_n is None:
        raise ChatHealthyException(
            mode="aborted",
            component="hf_helpers",
            message=f"ERROR: {VERSIONS_DB}.{VERSIONS_COLLECTION} latest record has "
            f"no 'build' field.")
    build_n = int(build_n)
    version_str = latest.get("version")
    framework_str = latest.get("framework")
    if not version_str:
        raise ChatHealthyException(
            mode="aborted",
            component="hf_helpers",
            message=f"ERROR: {VERSIONS_DB}.{VERSIONS_COLLECTION} latest record has "
            f"no 'version' field.")
    commit = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    # Version + framework now come from admin.Versions (single-source). Skip
    # edits the latest record directly when a release ships; build chain
    # reads from there and bakes into the image.
    info = {
        "build": build_n,
        "commit": commit,
        "env": env,
        "target_id": target_id,
        "service": service,
        "version": version_str,
        "framework": framework_str,
        "built_at": datetime.now(timezone.utc).isoformat(),
    }
    (workspace / "build_info.json").write_text(
        json.dumps(info, indent=2), encoding="utf-8",
    )


# ── HF API: variables + secrets ────────────────────────────────────────
def _hf_curl_delete(token: str, space: str, kind: str, key: str) -> None:
    """Delete a variable or secret on an HF Space (idempotent — 404 fine)."""
    import urllib.error
    import urllib.request
    url = f"https://huggingface.co/api/spaces/{_hf_default_org()}/{space}/{kind}"
    body = b'{"key":"' + key.encode() + b'"}'
    req = urllib.request.Request(
        url, data=body, method="DELETE",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        urllib.request.urlopen(req, timeout=15).read()
    except urllib.error.HTTPError:
        pass
    except urllib.error.URLError:
        pass


def _hf_set_variable(token: str, space: str, key: str, value: str) -> None:
    import json as _json
    import urllib.request
    # Delete any same-named secret first to avoid HF's var/secret collision.
    _hf_curl_delete(token, space, "secrets", key)
    url = f"https://huggingface.co/api/spaces/{_hf_default_org()}/{space}/variables"
    payload = _json.dumps({
        "key": key, "value": value, "description": "Set by local_publish",
    }).encode()
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    urllib.request.urlopen(req, timeout=30).read()


def _hf_set_secret(token: str, space: str, key: str, value: str) -> None:
    import json as _json
    import urllib.request
    _hf_curl_delete(token, space, "variables", key)
    _hf_curl_delete(token, space, "secrets", key)
    url = f"https://huggingface.co/api/spaces/{_hf_default_org()}/{space}/secrets"
    payload = _json.dumps({
        "key": key, "value": value, "description": "Set by local_publish",
    }).encode()
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    urllib.request.urlopen(req, timeout=30).read()


# ── HF API: Space lifecycle (create / delete / list / wait) ─────────────
def _hf_space_per_build_qualified(target_id: str, env: str, build_n: int) -> str:
    """Reverted 2026-06-22: returns the STABLE BASE qualified name. The
    per-build (`_<n>`) Space-naming scheme is retired — HF rate-limits
    Space CREATE calls and orphan Spaces accumulate. `build_n` is
    accepted for caller compatibility but unused."""
    return _hf_space_qualified(target_id, env)


def _hf_space_per_build_name(target_id: str, env: str, build_n: int) -> str:
    return _hf_space_per_build_qualified(target_id, env, build_n).split("/", 1)[1]


def _hf_wake_space(token: str, qualified: str, timeout_s: int = 300) -> str:
    """Bring a sleeping Space up, and wait until it is running.

    qa and prod Spaces sleep when nobody is using them, which is what they
    are meant to do. A deploy that pushes to a sleeping Space watches for a
    build that will never converge, because nothing is running to converge.
    Waking is therefore part of deploying, not something an operator does by
    hand beforehand.

    Returns the stage the Space reached. A Space already RUNNING is left
    alone rather than restarted, so this costs nothing on dev.
    """
    import json as _json
    import time as _time
    import urllib.request

    def _stage() -> str:
        url = f"https://huggingface.co/api/spaces/{qualified}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return ((_json.load(resp).get("runtime") or {}).get("stage") or "")

    stage = _stage()
    if stage == "RUNNING":
        _step(f"  {qualified} already running")
        return stage
    _step(f"  {qualified} is {stage or 'unknown'}; waking it")
    req = urllib.request.Request(
        f"https://huggingface.co/api/spaces/{qualified}/restart",
        data=b"", method="POST",
        headers={"Authorization": f"Bearer {token}"})
    try:
        urllib.request.urlopen(req, timeout=60).read()
    except Exception as exc:  # noqa: BLE001 - reported, then the wait decides
        _step(f"  restart call returned {type(exc).__name__}: {exc}")

    waited = 0
    while waited < timeout_s:
        stage = _stage()
        if stage in ("RUNNING", "BUILDING", "RUNNING_BUILDING", "APP_STARTING"):
            _step(f"  {qualified} is {stage}")
            return stage
        _time.sleep(10)
        waited += 10
    raise ChatHealthyException(
        mode="runtime_error",
        component="hf_helpers",
        message=f"{qualified} did not wake within {timeout_s}s; last stage "
                f"{stage!r}. A deploy cannot converge against a Space that is "
                f"not running.")


def _hf_space_exists(token: str, qualified: str) -> bool:
    import urllib.error
    import urllib.request
    url = f"https://huggingface.co/api/spaces/{qualified}"
    req = urllib.request.Request(
        url, method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        urllib.request.urlopen(req, timeout=15).read()
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def _hf_names_present(token: str, qualified: str) -> dict[str, list[str]]:
    """The secret and variable NAMES a Space currently holds.

    Names only. A value is never read, so nothing can be logged or returned
    that would put a credential in an audit trail.
    """
    import json as _json
    import urllib.request
    out: dict[str, list[str]] = {"secrets": [], "variables": []}
    for kind in ("secrets", "variables"):
        url = (f"https://huggingface.co/api/spaces/"
               f"{_hf_default_org()}/{qualified}/{kind}")
        req = urllib.request.Request(
            url, method="GET", headers={"Authorization": f"Bearer {token}"})
        body = _json.loads(urllib.request.urlopen(req, timeout=20).read())
        # The endpoint answers with an object keyed by name. Values are
        # present for variables and masked for secrets; neither is read.
        out[kind] = sorted(body.keys()) if isinstance(body, dict) else []
    return out


def _hf_remove_undeclared(token: str, qualified: str,
                          declared: set[str]) -> list[str]:
    """Remove every secret and variable the record does not declare.

    Realizes EPIC-008-F-012-S-004-REQ-B-014: after a deploy, a host carries
    nothing that deployment_architecture.json does not declare. A name set
    by hand, or declared once and later withdrawn, is removed here rather
    than living on unrecorded. Names are logged; values are never read.
    """
    present = _hf_names_present(token, qualified)
    removed: list[str] = []
    for kind in ("secrets", "variables"):
        for key in present[kind]:
            if key in declared:
                continue
            _hf_curl_delete(token, qualified, kind, key)
            removed.append(f"{kind}/{key}")
            _step(f"  removed undeclared {kind[:-1]} {key} from {qualified}")
    if not removed:
        _step(f"  no undeclared secret or variable on {qualified}")
    return removed


def _hf_create_space(token: str, qualified: str, sdk: str = "docker") -> None:
    """Create a new HF Space. Idempotent — if it already exists, no-op."""
    if _hf_space_exists(token, qualified):
        _step(f"  hf-create skip {qualified} (already exists)")
        return
    import json as _json
    import urllib.request
    org, name = qualified.split("/", 1)
    url = "https://huggingface.co/api/repos/create"
    payload = _json.dumps({
        "type": "space",
        "name": name,
        "organization": org,
        "private": False,
        "sdk": sdk,
    }).encode()
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    urllib.request.urlopen(req, timeout=30).read()
    _step(f"  hf-create OK {qualified} (sdk={sdk})")


def _hf_delete_space(token: str, qualified: str) -> None:
    """Delete an HF Space. Idempotent — 404 fine."""
    import json as _json
    import urllib.error
    import urllib.request
    org, name = qualified.split("/", 1)
    url = "https://huggingface.co/api/repos/delete"
    payload = _json.dumps({
        "type": "space",
        "name": name,
        "organization": org,
    }).encode()
    req = urllib.request.Request(
        url, data=payload, method="DELETE",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        urllib.request.urlopen(req, timeout=30).read()
        _step(f"  hf-delete OK {qualified}")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            _step(f"  hf-delete skip {qualified} (already gone)")
            return
        raise


def _hf_list_per_build_spaces(token: str, target_id: str, env: str) -> list[tuple[str, int]]:
    """Return [(qualified_name, build_n)] for every existing HF Space that
    matches the per-build naming pattern <base>_<int> for this target/env.
    Used to find the previous build's Space so we can delete it after the
    new one converges."""
    import urllib.error
    import urllib.request
    qualified_base = _hf_space_qualified(target_id, env)
    org, base = qualified_base.split("/", 1)
    out: list[tuple[str, int]] = []
    # HF /api/spaces?author=<org> lists all Spaces under the org; filter client-side.
    url = f"https://huggingface.co/api/spaces?author={org}&limit=1000"
    req = urllib.request.Request(
        url, method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        raw = urllib.request.urlopen(req, timeout=30).read()
    except urllib.error.HTTPError:
        return out
    import json as _json
    spaces = _json.loads(raw)
    prefix = f"{base}_"
    for s in spaces:
        sid = s.get("id") or ""
        # sid is "org/name". strip org/.
        if "/" not in sid:
            continue
        name = sid.split("/", 1)[1]
        if not name.startswith(prefix):
            continue
        suffix = name[len(prefix):]
        if not suffix.isdigit():
            continue
        out.append((sid, int(suffix)))
    return out


def _hf_wait_for_build_convergence(qualified: str, build_n: int,
                                    timeout_s: int = 600,
                                    poll_interval_s: int = 10) -> bool:
    """Poll the Space's public /health endpoint until it reports the expected
    build_n, or until timeout. Returns True on convergence, False on timeout."""
    import time
    import urllib.error
    import urllib.request
    org, name = qualified.split("/", 1)
    health_url = (
        f"https://{org.lower()}-{name.replace('_', '-').lower()}.hf.space/health"
    )
    deadline = time.monotonic() + timeout_s
    last_build = None
    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(health_url, method="POST")
            raw = urllib.request.urlopen(req, timeout=15).read()
            import json as _json
            data = _json.loads(raw)
            last_build = data.get("build")
            if last_build == build_n:
                _step(f"  hf-wait OK {qualified}: build={build_n}")
                return True
        except (urllib.error.URLError, TimeoutError, OSError, ValueError, KeyError):
            pass
        time.sleep(poll_interval_s)
    _step(f"  hf-wait TIMEOUT {qualified}: last build={last_build} expected={build_n}")
    return False


# ── Source-set conventions per target_id ───────────────────────────────
# These are the directories `local_publish.py` ships into each HF
# Space's docker build context.
def _source_set_for(target_id: str) -> list[tuple[str, str | None]]:
    """Return the (src_rel, dst_rel|None) subtree list for an HF target,
    read from its huggingface_source_set field in the manifest. dst=None
    means same as src. Replaces the per-target hardcoded functions
    (_findcare_source_set, _evaluatecare_source_set,
    _sharedservices_source_set).
    """
    data = _load_manifest()
    for rec in data["DeploymentTargetRecord"]:
        if rec.get("target_id") != target_id:
            continue
        raw = rec.get("huggingface_source_set")
        if not raw:
            raise ChatHealthyException(
            mode="runtime_error",
            component="hf_helpers",
            message=f"manifest target {target_id!r} has no huggingface_source_set "
                f"declared — populate it before deploy.")
        out: list[tuple[str, str | None]] = []
        for entry in raw:
            src = entry["src"]
            dst = entry.get("dst")
            out.append((src, dst))
        return out
    raise ChatHealthyException(
            mode="runtime_error",
            component="hf_helpers",
            message=f"manifest has no target {target_id!r}.")


def _staged_destination(src_rel: str,
                        source_set: list[tuple[str, str | None]]) -> str:
    """Where one declared file lands inside the build context.

    The source set no longer decides WHAT ships -- the declared file list
    does -- but it still decides WHERE, because a Space expects some
    subtrees remapped (sharedServices/Code arrives at the context root,
    its authentication package under `authentication`). The most specific
    prefix wins, so a file under a remapped subtree is not also matched by
    a shorter one.
    """
    src_posix = src_rel.replace("\\", "/")
    best_src = ""
    best_dst: str | None = None
    for entry_src, entry_dst in source_set:
        entry_posix = entry_src.replace("\\", "/")
        if src_posix == entry_posix or src_posix.startswith(entry_posix + "/"):
            if len(entry_posix) > len(best_src):
                best_src, best_dst = entry_posix, entry_dst
    if not best_src:
        return ""
    if best_dst is None:
        return src_posix
    remainder = src_posix[len(best_src):].lstrip("/")
    if best_dst == ".":
        return remainder or Path(src_posix).name
    return f"{best_dst}/{remainder}" if remainder else best_dst


def _stage_declared_files(repo_root: Path, build_dir: Path,
                          declared: list[str],
                          source_set: list[tuple[str, str | None]],
                          ) -> tuple[int, list[str]]:
    """Stage exactly the files the target record declares.

    The manifest is the instruction, not an audit trail taken after a
    subtree walk. The two can therefore no longer disagree without
    failing: a declared file that is not on disk stops the build instead
    of producing a package without it, and a file nothing declares does
    not ship.

    Returns (staged, outside_context). A declared file lying under none
    of the target's context subtrees is a build-time input rather than a
    shipped byte -- the canonical vite config is the one instance -- and
    it is named in the return so the caller reports it rather than
    letting it pass unremarked.
    """
    staged = 0
    outside: list[str] = []
    for src_rel in declared:
        src = repo_root / src_rel
        if not src.is_file():
            raise ChatHealthyException(
                mode="file_missing",
                component="hf_helpers",
                message=f"the target record declares {src_rel!r} and it is not "
                        "on disk. A build does not produce a package without a "
                        "file the record says it carries.")
        dst_rel = _staged_destination(src_rel, source_set)
        if not dst_rel:
            outside.append(src_rel)
            continue
        out_path = build_dir / dst_rel
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, out_path)
        staged += 1
    return staged, outside


def _copy_tree(
    src_root: Path, dst_root: Path,
    src_rel: str, dst_rel: str,
) -> None:
    """Copy a subtree, or one named file, into staging using the SAME
    exclusion rules Builder uses to enumerate source_locations.

    A src naming a file stages that file and nothing beside it. Without
    it the smallest thing the record could say was "this directory", and
    a directory holding two files the runtime reads and twenty-eight it
    does not shipped all thirty -- which is how the firm's brain came to
    sit inside a publicly pullable image.
    """
    src = src_root / src_rel
    if src.is_file():
        out_path = (dst_root if dst_rel == "." else dst_root / dst_rel)
        if dst_rel == ".":
            out_path = dst_root / src_rel
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, out_path)
        return
    if not src.is_dir():
        raise ChatHealthyException(
            mode="file_missing",
            component="hf_helpers",
            message=f"source path missing: {src}")
    dst = dst_root if dst_rel == "." else dst_root / dst_rel
    dst.mkdir(parents=True, exist_ok=True)
    for path in src.rglob("*"):
        if not path.is_file():
            continue
        parts = set(path.parts)
        if parts & _EXCLUDE_DIRS:
            continue
        if path.name in _EXCLUDE_FILE_NAMES:
            continue
        if path.suffix.lower() in _EXCLUDE_FILE_SUFFIXES:
            continue
        rel = path.relative_to(src)
        out_path = dst / rel
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, out_path)


# ── React frontend build (FindCare only) ──────────────────────────────
def _build_react_frontend(repo_root: Path, evalcare_peer: str,
                          sharedservices_peer: str) -> None:
    """Compile the React application against the peer addresses it is given.

    The peers are RESOLVED BY THE CALLER, not looked up here. Building a
    bundle is not a HuggingFace concern: local runs four docker
    containers and has no Space at all, so asking this function for a
    Space identity made a local build depend on a fact that is not true
    of local. The caller knows the environment it is building and reads
    the addressable location the record states for it.
    """
    # The node project is the repository, not one directory inside it. A
    # widget lives with the feature that owns it, so the project that
    # compiles it has to span every feature -- node and TypeScript both
    # resolve by walking UP from a file, and a project rooted in
    # frontend/ can only ever see what is beneath frontend/.
    frontend = repo_root
    if not (frontend / "package.json").is_file():
        raise ChatHealthyException(
            mode="file_missing",
            component="hf_helpers",
            message=f"package.json missing at {frontend}")
    canonical_vite = (repo_root / "architecture"
                      / "DevOpsBuildDeployAndEnvironmentManagement"
                      / "vite.config.ts")
    if not canonical_vite.is_file():
        raise ChatHealthyException(
            mode="file_missing",
            component="hf_helpers",
            message=f"canonical vite config missing at {canonical_vite}")
    vite_copy = repo_root / "vite.config.ts"
    shutil.copy2(canonical_vite, vite_copy)
    env_for_build = dict(os.environ)
    env_for_build["VITE_API_URL"] = ""
    env_for_build["VITE_EVALCARE_URL"] = evalcare_peer
    env_for_build["VITE_SHAREDSERVICES_URL"] = sharedservices_peer
    try:
        _step(f"npm ci in {frontend}")
        subprocess.run(
            ["npm", "ci"], cwd=str(frontend), env=env_for_build,
            check=True, shell=(sys.platform == "win32"),
        )
        _step(f"npm run build (VITE_EVALCARE_URL={evalcare_peer} VITE_SHAREDSERVICES_URL={sharedservices_peer})")
        subprocess.run(
            ["npm", "run", "build"], cwd=str(frontend), env=env_for_build,
            check=True, shell=(sys.platform == "win32"),
        )
        dist_index = (repo_root / "Code" / "ConversationalUX"
                      / "FindCareChat" / "frontend" / "dist"
                      / "index.html")
        if not dist_index.is_file():
            raise ChatHealthyException(
            mode="file_missing",
            component="hf_helpers",
            message=f"vite produced no {dist_index}")
        if not ch_fonts_inliner.inline_into(dist_index):
            raise ChatHealthyException(
            mode="runtime_error",
            component="hf_helpers",
            message=f"CH_FONTS marker not found in {dist_index}")
    finally:
        if vite_copy.is_file():
            vite_copy.unlink()
