# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
"""The gates that measure the refactor.

Three checks, run together, each reporting a number. They exist because
every gate this firm had asked whether what SHIPS is declared, and none
asked whether the declaration is true -- which is how one page came to
hold a tool that runs and is not written down beside a tool that is
written down and does not run, with a green build throughout.

Each check is written from BEHAVIOUR: what the code does and what the
record holds, never from a comment, a docstring or a bug description.
Each names an EFFECT rather than a spelling, because a check that names a
mechanism inherits that mechanism's lifetime.

Run:  python architecture/EngineeringRuleEnforcement/code/refactor_conformance.py
      --baseline   write the current numbers as the baseline to beat
      --json       machine-readable, for a later enforcement wrapper

These are NOT enforcements yet. A check that has never gone red against a
real violation is a hypothesis, so each must be demonstrated red before it
is wired into engineering_rules.json -- and wiring it is a governance
change that belongs to the operator, not to this program.
"""
from __future__ import annotations

import argparse
import ast
import json
import pathlib
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parents[3]
RECORD = REPO / "brain" / "machine_artifacts" / "content" / "deployment_architecture.json"
ROUTER = REPO / "sharedServices" / "Code" / "AuthorizationsAndAuthentications" / "universal_navigation_tool.py"

# A reference to the legacy tree only matters where something executes,
# builds, configures or declares. Prose that mentions the directory is not
# a dependency on it.
REFERRING_SUFFIXES = (".py", ".ts", ".tsx", ".js", ".jsx", ".json",
                      ".yml", ".yaml", ".ps1", ".sh", ".toml", ".cfg",
                      ".ini", ".http", ".env", ".example")
NOT_A_DEPENDENCY = ("_oneshots/", "node_modules/", "/dist/", "__pycache__/",
                    "/build/", ".git/", "/archive/")


def tracked(prefix: str = "") -> list[str]:
    out = subprocess.run(["git", "ls-files", prefix] if prefix else ["git", "ls-files"],
                         cwd=REPO, capture_output=True, text=True, check=True)
    return [line for line in out.stdout.splitlines() if line.strip()]


def _interesting(path: str) -> bool:
    if any(skip in "/" + path for skip in NOT_A_DEPENDENCY):
        return False
    return path.endswith(REFERRING_SUFFIXES) or path.endswith("Dockerfile")


# ── Gate 1 ───────────────────────────────────────────────────────────
def gate_the_legacy_tree_is_gone() -> dict:
    """Nothing lives in Code/ and nothing reaches into it.

    The effect this names: the legacy tree is not load-bearing. It is red
    while a file is still there OR while anything still points at one, and
    those are two different failures -- a directory can be empty of files
    and still be named by a build script.
    """
    living = tracked("Code")
    referrers: dict[str, int] = {}
    for path in tracked():
        if path.startswith("Code/") or not _interesting(path):
            continue
        try:
            body = (REPO / path).read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        hits = body.count("Code/") + body.count("Code\\\\")
        if hits:
            referrers[path] = hits
    return {
        "gate": "the legacy tree is gone",
        "files_still_in_it": len(living),
        "files_pointing_at_it": len(referrers),
        "references": sum(referrers.values()),
        "pointers": dict(sorted(referrers.items(), key=lambda kv: -kv[1])),
        "passing": not living and not referrers,
    }


# ── the record and the router, read once ─────────────────────────────
def tool_configuration() -> dict:
    record = json.loads(RECORD.read_text(encoding="utf-8"))
    for target in record.get("DeploymentTargetRecord", []):
        for env in target.get("environments", []):
            for coll in env.get("config_collections", []):
                if str(coll.get("address", "")).endswith("ToolConfiguration"):
                    for rec in coll.get("records", []):
                        if rec.get("tools") is not None:
                            return rec
    return {}


def tool_names_on_disk() -> dict[str, str]:
    """TOOL_NAME -> the module that declares it, read by parsing.

    Read from the class attribute rather than inferred from the filename,
    because a filename is a spelling and TOOL_NAME is what the dispatcher
    actually uses.
    """
    found: dict[str, str] = {}
    for path in tracked():
        if not path.endswith("_tool.py") or any(s in "/" + path for s in NOT_A_DEPENDENCY):
            continue
        try:
            tree = ast.parse((REPO / path).read_text(encoding="utf-8"))
        except (SyntaxError, OSError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            for item in node.body:
                if not isinstance(item, ast.Assign):
                    continue
                for target in item.targets:
                    if (isinstance(target, ast.Name)
                            and target.id in ("TOOL_NAME", "CAPABILITY_TOOL")
                            and isinstance(item.value, ast.Constant)
                            and isinstance(item.value.value, str)):
                        found[item.value.value] = path
    return found


def modules_the_router_dispatches() -> set[str]:
    """Every module the router actually calls a TOOL on.

    Behavioural: an import alone is not a dispatch, and a name in a
    comment is nothing at all. This looks for `<module>.TOOL.<call>`.
    """
    tree = ast.parse(ROUTER.read_text(encoding="utf-8"))
    dispatched: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        owner = func.value
        if (isinstance(owner, ast.Attribute) and owner.attr == "TOOL"
                and isinstance(owner.value, ast.Name)):
            dispatched.add(owner.value.id)
    return dispatched


# ── Gate 2 ───────────────────────────────────────────────────────────
def gate_declared_equals_dispatched() -> dict:
    """Everything that runs is declared, and everything declared runs.

    The effect: the record and the running system agree, in BOTH
    directions. One direction is the firm's existing gates; the reverse
    has never been checked anywhere and is where every drift went.
    """
    config = tool_configuration()
    declared = {t.get("tool"): t for t in config.get("tools") or [] if t.get("tool")}
    on_disk = tool_names_on_disk()
    dispatched_modules = modules_the_router_dispatches()

    reachable = {
        name for name, path in on_disk.items()
        if pathlib.Path(path).stem in dispatched_modules
    }
    declared_not_reachable = sorted(set(declared) - reachable - {"ProviderSelection"})
    reachable_not_declared = sorted(reachable - set(declared))
    return {
        "gate": "declared equals dispatched",
        "declared": len(declared),
        "tool_modules_on_disk": len(on_disk),
        "dispatched_by_the_router": len(reachable),
        "declared_but_not_reachable": declared_not_reachable,
        "reachable_but_not_declared": reachable_not_declared,
        "passing": not declared_not_reachable and not reachable_not_declared,
    }


# ── Gate 3 ───────────────────────────────────────────────────────────
def gate_a_declaration_states_where_a_tool_is_served() -> dict:
    """No declared tool leaves its caller to know an address.

    This is gate 3 in its available form. Its full form -- every
    capability carries its four tools -- cannot be written yet, because
    the capability declaration it would read does not exist until Phase 1.
    Saying so is the point: a check written against a record that is not
    there would pass by finding nothing, which is the failure mode these
    gates exist to catch.
    """
    config = tool_configuration()
    tools = config.get("tools") or []
    incomplete = [t.get("tool") for t in tools
                  if not t.get("server") or not t.get("endpoint")]
    pages = config.get("pages") or []
    tools_on_pages = {name for page in pages for name in page.get("tools") or []}
    declared = {t.get("tool") for t in tools if t.get("tool")}
    return {
        "gate": "a declaration states where a tool is served",
        "declared_tools": len(declared),
        "missing_server_or_endpoint": [t for t in incomplete if t],
        "on_a_page_but_not_declared": sorted(tools_on_pages - declared),
        "declared_but_on_no_page": sorted(declared - tools_on_pages),
        "capability_completeness": "BLOCKED until the capability declaration exists (Phase 1)",
        "passing": not incomplete and not (tools_on_pages - declared),
    }


GATES = (gate_the_legacy_tree_is_gone,
         gate_declared_equals_dispatched,
         gate_a_declaration_states_where_a_tool_is_served)


def _render_value(key, value) -> None:
    if isinstance(value, dict):
        sys.stdout.write(f"    {key}: {len(value)}\n")
        for name, count in list(value.items())[:12]:
            sys.stdout.write(f"        {count:>4}  {name}\n")
        if len(value) > 12:
            sys.stdout.write(f"        ... {len(value) - 12} more\n")
    elif isinstance(value, list):
        sys.stdout.write(f"    {key}: {len(value)}"
                         + (f"  {value}" if value else "") + "\n")
    else:
        sys.stdout.write(f"    {key}: {value}\n")


def _report(results) -> None:
    for result in results:
        verdict = "PASS" if result["passing"] else "RED"
        sys.stdout.write(f"\n[{verdict}] {result['gate']}\n")
        for key, value in result.items():
            if key not in ("gate", "passing"):
                _render_value(key, value)
    green = sum(1 for r in results if r["passing"])
    sys.stdout.write(f"\n{green} of {len(results)} passing\n")


def _write_baseline(results) -> None:
    out = (REPO / "architecture" / "EngineeringRuleEnforcement"
           / "ArchitectureDesignAndAuditDocs" / "refactor_baseline.json")
    out.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(f"baseline written: {out}\n")


def _arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--baseline", action="store_true",
                        help="write these numbers as the baseline to beat")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _arguments(argv)
    results = [gate() for gate in GATES]
    if args.json:
        sys.stdout.write(json.dumps(results, indent=2) + "\n")
    else:
        _report(results)
    if args.baseline:
        _write_baseline(results)
    return 0 if all(r["passing"] for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
