"""Runtime data collection bindings driven by ChatHealthyConfig.DBVersions.

Each front-end runtime that consumes versioned data calls
`bind_from_manifest()` at FastAPI startup. The function:

  1. Reads target_id and env from build_info.json baked into the build.
  2. Reads ChatHealthyConfig.DBVersions.find_one({"env": env}) on the
     ChatHealthyFrontEnd cluster, reached through ChatHealthyMongoUtilities.
  3. Locates the targets[] entry whose deployment_target == target_id.
  4. Binds module-level statics for each collection_environment_name to
     its runtime_collection_name.

Accessors providers_coll() and specialty_meta_coll() return the bound
pymongo Collection object. They raise if the binding is missing — no
silent fallback to env-prefixed paths.

The router exposes POST /admin/swap to rebind the statics in place
(activation runbook calls this), and GET /debug/active_collections to
report the in-memory bindings vs the current doc-resolved bindings.

Per EPIC-010-F-101-S-005 (Data version management).
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Header
from pymongo import MongoClient
from pymongo.collection import Collection

from .exceptions import ChatHealthyException
from .mongo_utilities import ChatHealthyMongoUtilities
from .logging_service import set_mongo_log_collection, set_mongo_log_identity

# The front-end services act as frontendUser, including when they write
# their own logs. The Mongo log handler refuses to build without this, and
# this module is imported by every service that resolves a data collection,
# which makes it the front-end equivalent of pipeline_db.
set_mongo_log_identity("frontendUser")
# The whole front-end application is one log stream. The collection was a
# constant in the logging service, which made the front end and the pipeline
# the same stream; each caller now declares its own beside its identity.
set_mongo_log_collection("CGFrontEndLogs")


# The one spelling of a versioned collection. Shared with mongo_utilities,
# which refuses any name that carries a version the binding did not give.
_VERSION_SEPARATOR = "_v_"

_CONFIG_DB = "ChatHealthyConfig"
_CONFIG_COLL = "DBVersions"

_PROVIDER_SLOT = "PROVIDER_COLLECTION"
_SPECIALTY_META_SLOT = "SPECIALTY_META_COLLECTION"

# The parameter declaration: which attributes each page declares, and which
# parameters carry over between which pages. A second document in the same
# database, keyed by env exactly as DBVersions is, read by the same
# connection under the same identity.
#
# It is data rather than code because a change to which parameters carry
# over must take effect without a software release
# (EPIC-006-F-008-S-002-REQ-B-002). The runtime already reads its own
# configuration from a place an operator can change, so this needs no new
# component.
_PARAMETER_DECLARATION_COLL = "ParameterDeclaration"


class _State:
    target_id: str | None = None
    env: str | None = None
    # slot -> composed 'Database.Collection_v_N'. What every consumer of a
    # bound collection reads.
    bindings: dict[str, str] = {}
    # (database, base) -> composed name, taken from the base the RECORD
    # states. Derived from nothing: the previous form rebuilt this by
    # splitting the composed name back apart, which is the same parsing the
    # record was reshaped to eliminate, moved one layer down where it was
    # harder to see. A base is a fact the binding carries, not a fact
    # recovered from a string.
    bases: dict[tuple[str, str], str] = {}
    # The parameter declaration for this env: pages[] and carry_over[].
    parameter_declaration: dict = {}


_state = _State()


def _read_build_info() -> dict:
    candidates = [
        Path("build_info.json"),
        Path("/app/build_info.json"),
        Path(__file__).resolve().parents[3] / "build_info.json",
    ]
    for path in candidates:
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
    raise ChatHealthyException(
        mode="config_error",
        component="runtime_data_collections",
        message=
        "runtime_data_collections: build_info.json not found in "
        f"{[str(p) for p in candidates]}. The build must write it so "
        "the runtime can identify its target_id and env."
    )


def _mongo_client() -> MongoClient:
    # Config lives in ChatHealthyConfig on the admin target.
    return ChatHealthyMongoUtilities().getConnection("frontendUser", "ChatHealthyFrontEnd")


def _read_env_doc(env: str) -> dict:
    coll = _mongo_client()[_CONFIG_DB][_CONFIG_COLL]
    doc = coll.find_one({"env": env})
    if doc is None:
        raise ChatHealthyException(
            mode="config_error",
            component="runtime_data_collections",
            message=
            f"runtime_data_collections: ChatHealthyConfig.DBVersions has no document "
            f"for env={env!r}. Seed the document before starting the runtime "
            "(EPIC-010-F-101-S-005-REQ-B-003)."
        )
    return doc


def compose_collection(base: str, version) -> str:
    """'PublicHealthData.Provider' + 4 -> 'PublicHealthData.Provider_v_4'.

    The physical name is composed and never written down. A name that is
    typed can be typed two ways -- provider_v03 and Provider_v_4 are two
    spellings of one idea, and only the second follows the convention -- and
    the binding that carries a typed name is where that divergence lives.
    Composing it means the convention cannot be broken by a record.
    """
    if not base or "." not in base:
        raise ChatHealthyException(
            mode="config_error",
            component="runtime_data_collections",
            message=f"collection base must be 'Database.Collection', got {base!r}",
        )
    if _VERSION_SEPARATOR in base:
        raise ChatHealthyException(
            mode="config_error",
            component="runtime_data_collections",
            message=(
                f"collection base {base!r} already carries a version. The base "
                f"names the collection; the version is a separate field."),
        )
    try:
        generation = int(version)
    except (TypeError, ValueError):
        raise ChatHealthyException(
            mode="config_error",
            component="runtime_data_collections",
            message=f"collection version must be an integer, got {version!r}",
        ) from None
    return f"{base}{_VERSION_SEPARATOR}{generation}"


def _binding_target(entry: dict) -> str:
    """The collection one binding entry names, composed from base+version.

    An entry carrying neither is a binding to an unversioned collection and
    is taken as written -- Users and DBVersions are not versioned and never
    will be.
    """
    base = entry.get("collection_base")
    version = entry.get("version")
    if base is not None or version is not None:
        return compose_collection(base, version)
    unversioned = entry.get("runtime_collection_name")
    if not unversioned:
        raise ChatHealthyException(
            mode="config_error",
            component="runtime_data_collections",
            message=(
                f"binding for {entry.get('collection_environment_name')!r} names "
                f"no collection: give collection_base + version for a versioned "
                f"collection, or runtime_collection_name for one that is not."),
        )
    return unversioned


def _bases_from_doc(doc: dict, target_id: str) -> dict[tuple[str, str], str]:
    """{(database, base): composed name} straight from what the record says.

    The base is read, never recovered. A caller naming SpecialtyMetaData is
    looked up here and the composed name is swapped in.
    """
    out: dict[tuple[str, str], str] = {}
    for entry in doc.get("targets", []):
        if entry.get("deployment_target") != target_id:
            continue
        for c in entry.get("collections", []):
            base = c.get("collection_base")
            if not base:
                continue          # not versioned; nothing to swap
            db_name, _, base_name = base.partition(".")
            out[(db_name, base_name)] = _binding_target(c)
    return out


def _bindings_from_doc(doc: dict, target_id: str) -> dict[str, str]:
    for entry in doc.get("targets", []):
        if entry.get("deployment_target") != target_id:
            continue
        return {
            c["collection_environment_name"]: _binding_target(c)
            for c in entry.get("collections", [])
        }
    raise ChatHealthyException(
        mode="config_error",
        component="runtime_data_collections",
        message=
        f"runtime_data_collections: ChatHealthyConfig.DBVersions env={doc.get('env')!r} "
        f"has no targets[] entry for deployment_target={target_id!r}. "
        "Update the env doc to include this runtime."
    )


def bind_from_manifest() -> None:
    """Read target_id (from build_info.json) + env (from ENV_PREFIX env
    var) and bind module-level collection statics from
    ChatHealthyConfig.DBVersions. Call from the runtime's startup hook.
    Raises on any missing piece — no silent fallbacks.

    target_id is build-time-stable (baked into the image); env is
    deploy-time-stable (set per HF Space deploy via ENV_PREFIX), so the
    two come from different sources by design.
    """
    info = _read_build_info()
    target_id = info.get("target_id")
    env = os.environ.get("ENV_PREFIX")
    if not target_id:
        raise ChatHealthyException(
            mode="config_error",
            component="runtime_data_collections",
            message=
            f"runtime_data_collections: build_info.json missing target_id "
            f"({target_id!r}). Build step must emit it."
        )
    if not env:
        raise ChatHealthyException(
            mode="config_error",
            component="runtime_data_collections",
            message=
            "runtime_data_collections: ENV_PREFIX env var not set. The HF "
            "deploy must set it per target environment binding."
        )
    doc = _read_env_doc(env)
    bindings = _bindings_from_doc(doc, target_id)
    _state.target_id = target_id
    _state.env = env
    _state.bindings = bindings
    _state.bases = _bases_from_doc(doc, target_id)


def _coll_for(slot: str) -> Collection:
    fqn = _state.bindings.get(slot)
    if not fqn:
        raise ChatHealthyException(
            mode="config_error",
            component="runtime_data_collections",
            message=
            f"runtime_data_collections: slot {slot!r} is not bound. Call "
            "bind_from_manifest() at startup."
        )
    db_name, coll_name = fqn.split(".", 1)
    return _mongo_client()[db_name][coll_name]


def _read_parameter_declaration(env: str) -> dict:
    """The env's parameter declaration, validated against the closed page set.

    The four page names are not in the document. They are fixed in this
    code because EPIC-006-F-008-S-001-REQ-B-002 closes the set, and a
    closed set held as data is a set an edit can open.
    """
    from .authentication.user_parameters import PAGES
    coll = _mongo_client()[_CONFIG_DB][_PARAMETER_DECLARATION_COLL]
    doc = coll.find_one({"env": env})
    if doc is None:
        raise ChatHealthyException(
            mode="config_error",
            component="runtime_data_collections",
            message=(
                f"runtime_data_collections: {_CONFIG_DB}."
                f"{_PARAMETER_DECLARATION_COLL} has no document for env={env!r}. "
                "Seed the declaration before starting the runtime "
                "(EPIC-006-F-008-S-001)."),
        )
    for page in doc.get("pages") or []:
        name = page.get("page")
        if name not in PAGES:
            raise ChatHealthyException(
                mode="config_error",
                component="runtime_data_collections",
                message=(
                    f"parameter declaration for env={env!r} names page {name!r}, "
                    f"which is not one of the four: {', '.join(PAGES)}."),
            )
    for triple in doc.get("carry_over") or []:
        for key in ("from", "to"):
            if triple.get(key) not in PAGES:
                raise ChatHealthyException(
                    mode="config_error",
                    component="runtime_data_collections",
                    message=(
                        f"parameter declaration for env={env!r} states a "
                        f"carry-over whose {key} page is {triple.get(key)!r}, "
                        f"which is not one of the four: {', '.join(PAGES)}."),
                )
    return doc


def parameter_declaration() -> dict:
    """The declaration, read where it is used and held after the first read.

    Read on first use rather than at startup. bind_from_manifest binds
    collection slots, and services that bind slots are not the same set as
    services that read a parameter: the provider backend binds slots and
    never reads one, so reading the declaration there made it depend on a
    collection it has no business touching. The parameters tool is the
    only reader, and this is read where that tool runs.

    Re-read on the bearer-gated rebind, so a change to which parameters
    carry over takes effect on the next turn without a build
    (EPIC-006-F-008-S-002-REQ-B-002).
    """
    if not _state.parameter_declaration:
        env = _state.env or os.environ.get("ENV_PREFIX", "").strip()
        if not env:
            raise ChatHealthyException(
                mode="config_error",
                component="runtime_data_collections",
                message=("runtime_data_collections: ENV_PREFIX is not set, so "
                         "there is no environment whose parameter declaration "
                         "could be read."),
            )
        _state.env = env
        _state.parameter_declaration = _read_parameter_declaration(env)
    return _state.parameter_declaration


def declared_attributes(page: str) -> dict[str, str]:
    """attribute -> value_type for one page, from the declaration.

    The declaration is the allowlist: a write naming an attribute the page
    does not declare is refused, exactly as one naming a fifth page is.
    """
    for entry in parameter_declaration().get("pages") or []:
        if entry.get("page") == page:
            return {a["name"]: a.get("value_type", "string")
                    for a in (entry.get("attributes") or []) if a.get("name")}
    return {}


def required_attributes(page: str) -> list[str]:
    """The attributes this page cannot run without, from the declaration.

    A page states which of its attributes are required, and code asks here
    rather than deciding for itself. Making a parameter required or optional
    is then an edit to the declaration and a deploy -- no code changes, and
    nothing can enforce a requirement the record does not state.

    An attribute that does not say is not required: the declaration states
    what it demands, and silence demands nothing.
    """
    for entry in parameter_declaration().get("pages") or []:
        if entry.get("page") == page:
            return [a["name"] for a in (entry.get("attributes") or [])
                    if a.get("name") and a.get("required") is True]
    return []


def optional_attributes(page: str) -> list[str]:
    """The attributes this page will use but can run without.

    Asked for by the question a page puts to a person when it cannot run:
    what is optional is what must not be asked for, and that is a fact about
    the declaration rather than about any page.
    """
    for entry in parameter_declaration().get("pages") or []:
        if entry.get("page") == page:
            return [a["name"] for a in (entry.get("attributes") or [])
                    if a.get("name") and a.get("required") is not True]
    return []


def carry_over_triples(destination: str) -> list[dict]:
    """The stated triples whose destination is this page.

    Carry-over is a set of stated triples and nothing else produces it.
    The absence of a triple is the refusal (S-002-REQ-B-003): there is no
    rule that a parameter of the same name carries between pages, because
    that is precisely the system-wide rule S-002-REQ-B-001 forbids.
    """
    return [t for t in (parameter_declaration().get("carry_over") or [])
            if t.get("to") == destination]


def providers_coll() -> Collection:
    return _coll_for(_PROVIDER_SLOT)


def specialty_meta_coll() -> Collection:
    return _coll_for(_SPECIALTY_META_SLOT)


# ── Admin / debug endpoints ────────────────────────────────────────────

router = APIRouter()


def _require_bearer(authorization: str | None = Header(default=None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise ChatHealthyException(
            mode="http_error",
            component="runtime_data_collections",
            message="Bearer token required.",
            status_code=401)
    token = authorization[7:].strip()
    if not token:
        raise ChatHealthyException(
            mode="http_error",
            component="runtime_data_collections",
            message="Empty bearer token.",
            status_code=401)
    token_map = json.loads(os.environ.get("API_TOKEN_MAP", "{}"))
    user = token_map.get(token)
    if not user:
        raise ChatHealthyException(
            mode="http_error",
            component="runtime_data_collections",
            message="Invalid bearer token.",
            status_code=401)
    return user


@router.post("/admin/swap", status_code=202)
def admin_swap(
    payload: dict[str, Any],
    _user: str = Depends(_require_bearer),
) -> dict[str, Any]:
    """Rebind module-level statics from the posted collections map.

    Expected payload shape:
        { "collections": [
            { "collection_environment_name": "...", "runtime_collection_name": "..." },
            ...
        ] }

    The activation runbook (ChangeDBVersion) reads each env doc and POSTs
    to this endpoint per target. The previous bindings are discarded; the
    new map is the in-memory truth until the next swap or restart.
    """
    items = payload.get("collections")
    if not isinstance(items, list):
        raise ChatHealthyException(
            mode="http_error",
            component="runtime_data_collections",
            message="collections[] required.",
            status_code=400)
    new_bindings: dict[str, str] = {}
    for entry in items:
        slot = entry.get("collection_environment_name")
        if not slot:
            raise ChatHealthyException(
                mode="http_error",
                component="runtime_data_collections",
                message="each collections[] entry needs collection_environment_name.",
                status_code=400)
        # Composed here too, so a swap cannot introduce a spelling the
        # startup binding would have refused.
        new_bindings[slot] = _binding_target(entry)
    _state.bindings = new_bindings
    # The parameter declaration is dropped on the same rebind, so the next
    # read picks up a change to which parameters carry over and it takes
    # effect on the next turn without a build
    # (EPIC-006-F-008-S-002-REQ-B-002).
    _state.parameter_declaration = {}
    return {"status": "ok", "target_id": _state.target_id, "env": _state.env, "bindings": new_bindings}


@router.get("/debug/active_collections")
def debug_active_collections(
    _user: str = Depends(_require_bearer),
) -> dict[str, Any]:
    """Report the in-memory bindings vs the bindings currently in the
    env's ChatHealthyConfig.DBVersions document. state is 'stable' when they match,
    'drift' when they do not — usually because an activation push failed
    and the runtime is still on the old map.
    """
    in_memory = dict(_state.bindings)
    doc_resolved: dict[str, str] = {}
    state = "stable"
    try:
        doc = _read_env_doc(_state.env) if _state.env else None
        if doc and _state.target_id:
            doc_resolved = _bindings_from_doc(doc, _state.target_id)
    except Exception as exc:
        return {
            "target_id": _state.target_id,
            "env": _state.env,
            "in_memory": in_memory,
            "doc_resolved": None,
            "state": "doc_unreachable",
            "error": str(exc),
        }
    if doc_resolved != in_memory:
        state = "drift"
    return {
        "target_id": _state.target_id,
        "env": _state.env,
        "in_memory": in_memory,
        "doc_resolved": doc_resolved,
        "state": state,
    }
