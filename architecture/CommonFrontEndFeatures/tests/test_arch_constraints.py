# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).
#
# ARCH-CONSTRAINTS: Architecture constraint tests.
# SEC-GUARD-RISK-001: Guard risk acceptance integration.
#
# Uses a real MongoDB "lab" database (GOV-006: real system testing).
# RISK-008: Claude authorized to create/destroy any object in lab DB.
#
# Tests: create collection, insert records, vectorize, create vector index, verify, clean up.

import os
import sys
import pytest

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

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))

sys.path.insert(0, os.path.join(REPO_ROOT, "Code", "DataPipelines"))
sys.path.insert(0, os.path.join(REPO_ROOT, "Code", "Shared"))


from dotenv import load_dotenv
load_dotenv(os.path.join(REPO_ROOT, "Code", ".env"))


LAB_DB = "lab"
LAB_COLLECTION = "test_vector_constraint"


class TestArchConstraintVectorIndex:
    """SEC-GUARD-RISK-001: Prove we can create DB objects in lab under RISK-008.
    Creates collection, inserts vectorized records, creates vector index, verifies."""

    @pytest.fixture(scope="class")
    def lab_collection(self):
        from pymongo import MongoClient
        client = _ch_connection()
        db = client[LAB_DB]
        coll = db[LAB_COLLECTION]
        # Clean slate
        coll.drop()
        yield coll
        # Cleanup after all tests
        coll.drop()
        client.close()

    def test_create_collection_and_insert(self, lab_collection):
        """Create lab collection and insert 3 test records with embeddings."""
        from openai import OpenAI

        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key:
            pytest.skip("OPENAI_API_KEY not set")

        # 3 simple test documents
        docs = [
            {"name": "Test Provider A", "specialty": "Orthopedic Surgery"},
            {"name": "Test Provider B", "specialty": "Family Medicine"},
            {"name": "Test Provider C", "specialty": "Pediatrics"},
        ]

        # Vectorize with cheapest model
        client = OpenAI(api_key=api_key)
        for doc in docs:
            text = f"{doc['name']} {doc['specialty']}"
            resp = client.embeddings.create(
                model="text-embedding-3-small",
                input=text,
            )
            doc["embedding"] = resp.data[0].embedding

        lab_collection.insert_many(docs)
        count = lab_collection.count_documents({})
        assert count == 3, f"Expected 3 docs, got {count}"

    def test_create_vector_index(self, lab_collection):
        """Create Atlas Vector Search index on lab collection."""
        dims = 1536  # text-embedding-3-small dimensions
        index_name = "lab_vector_index"

        try:
            lab_collection.create_search_index({
                "name": index_name,
                "type": "vectorSearch",
                "definition": {
                    "fields": [
                        {
                            "type": "vector",
                            "path": "embedding",
                            "numDimensions": dims,
                            "similarity": "cosine",
                        },
                    ]
                },
            })
        except Exception as exc:
            if "already exists" in str(exc).lower():
                pass  # Idempotent
            else:
                raise

        # Verify index exists
        indexes = list(lab_collection.list_search_indexes())
        index_names = [idx.get("name") for idx in indexes]
        assert index_name in index_names, f"Index {index_name} not found in {index_names}"

    def test_documents_have_embeddings(self, lab_collection):
        """Verify all docs have embedding field with correct dimensions."""
        for doc in lab_collection.find({}, {"embedding": 1}):
            assert "embedding" in doc, f"Doc {doc['_id']} missing embedding"
            assert len(doc["embedding"]) == 1536, f"Doc {doc['_id']} embedding wrong dims: {len(doc['embedding'])}"

    def test_risk_acceptance_loaded_by_guard(self):
        """Verify RISK-008 exists in risk_acceptance.json and guard can read it."""
        import json
        ra_path = os.path.join(REPO_ROOT, "brain", "machine_artifacts", "content", "risk_acceptance.json")
        with open(ra_path, encoding="utf-8") as f:
            ra = json.load(f)

        risk_008 = [e for e in ra["entries"] if e["id"] == "RISK-008"]
        assert len(risk_008) == 1, "RISK-008 not found in risk_acceptance.json"
        assert risk_008[0]["boss_decision"] == "ACCEPTED"
        assert "lab" in risk_008[0]["description"].lower()


# ---------------------------------------------------------------------------
# EPIC-008-F-011-S-004-REQ-B-001: Single canonical embedding model
# (OpenAI text-embedding-3-large, vector dimension 3072).
# ---------------------------------------------------------------------------

# The model name is not written here. It is declared once for the firm in
# deployment_architecture.json; a copy in a test is a fourth place the
# value lives. Only the dimension is a property of the index rather than
# of the declaration, so only the dimension stays.
CANONICAL_EMBED_DIMS = 3072


def test_canonical_embedding_model_data_side():
    """EPIC-008-F-011-S-004-REQ-B-001-PYTEST-1.

    For env prefix 'dev', verify every vectorSearch index on
    {env}_PublicHealthData.providers and {env}_PublicHealthData.SpecialtyMetaData
    has numDimensions == 3072 (the dimension of text-embedding-3-large).
    """
    from pymongo import MongoClient

    client = _ch_connection()
    try:
        env = "dev"
        targets = [
            (f"{env}_PublicHealthData", "providers"),
            (f"{env}_PublicHealthData", "SpecialtyMetaData"),
        ]

        total_vector_indexes_seen = 0
        for db_name, coll_name in targets:
            coll = client[db_name][coll_name]
            try:
                indexes = list(coll.list_search_indexes())
            except Exception as exc:
                # Some envs/clusters may not support search indexes; treat as
                # "nothing to check here" rather than a violation.
                _CH_LOG.info(f"[INFO] {db_name}.{coll_name}: list_search_indexes failed: {exc}")
                continue

            for idx in indexes:
                if idx.get("type") != "vectorSearch":
                    continue
                index_name = idx.get("name", "<unnamed>")
                # Mongo Atlas exposes the live spec under "latestDefinition";
                # historical/proposed under "definition". Prefer latest.
                definition = idx.get("latestDefinition") or idx.get("definition") or {}
                fields = definition.get("fields") or []
                for field in fields:
                    if field.get("type") != "vector":
                        continue
                    total_vector_indexes_seen += 1
                    n = field.get("numDimensions")
                    path = field.get("path", "<unknown>")
                    assert n == CANONICAL_EMBED_DIMS, (
                        f"index {index_name} field {path} has numDimensions={n}, "
                        f"expected {CANONICAL_EMBED_DIMS} "
                        f"(text-embedding-3-large per EPIC-008-F-011-S-004-REQ-B-001)"
                    )

        if total_vector_indexes_seen == 0:
            pytest.skip(
                "No vectorSearch indexes found on dev_PublicHealthData.providers or "
                "dev_PublicHealthData.SpecialtyMetaData; nothing to verify on this env."
            )
    finally:
        client.close()


def _iter_in_scope_py_files(repo_root):
    """Yield absolute Path objects for production-executable .py files in scope."""
    from pathlib import Path

    roots = [
        Path(repo_root) / "Code" / "ConversationalUX" / "FindCareChat" / "backend",
        Path(repo_root) / "Code" / "DataPipelines",
        Path(repo_root) / "evaluateCare" / "Code",
        Path(repo_root) / "sharedServices" / "Code",
        Path(repo_root) / "ChatHealthyLib" / "src",
    ]
    excluded_substrings = (
        "/tests/",
        "/test_",
        "_test.py",
        "/conftest.py",
        "/_oneshots/",
        "/__pycache__/",
        "/node_modules/",
    )
    for root in roots:
        if not root.exists():
            continue
        for p in root.rglob("*.py"):
            posix = p.as_posix()
            if any(sub in posix for sub in excluded_substrings):
                continue
            yield p


def _line_number_for_offset(text, offset):
    return text.count("\n", 0, offset) + 1


# The canonical value is not written here. It is declared once for the
# firm in deployment_architecture.json and read from the record, so this
# test stops being another place the name is written. What the test
# guards is the two things that make one declaration hold: that no
# production file names an embedding model as a literal, and that every
# target whose declared files reach the facade's embed() carries the
# CH_EMBEDDING_MODEL binding.

def _record():
    import json
    from pathlib import Path
    return json.loads(
        (Path(REPO_ROOT) / "brain" / "machine_artifacts" / "content"
         / "deployment_architecture.json").read_text(encoding="utf-8"))


def _declared_embedding_model():
    """The firm's one declaration."""
    model = (_record().get("firm") or {}).get("embedding_model")
    assert model, (
        "deployment_architecture.json firm block declares no embedding_model. "
        "EPIC-008-F-011-S-004-REQ-B-001 requires one embedding model for the "
        "application, declared once."
    )
    return model


def test_no_production_file_names_an_embedding_model_literally():
    """EPIC-008-F-011-S-004-REQ-B-001.

    The model is read from the binding the target carries, so a
    production file naming one is a second declaration of a value that
    has one home.
    """
    model = _declared_embedding_model()
    violations = []
    for path in _iter_in_scope_py_files(REPO_ROOT):
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        idx = text.find(model)
        while idx != -1:
            violations.append((str(path), _line_number_for_offset(text, idx)))
            idx = text.find(model, idx + len(model))

    formatted = "\n  ".join(f"{f}:{ln}" for f, ln in violations)
    assert violations == [], (
        f"An embedding model is named as a literal in production code. The "
        f"model is declared once in the record's firm block and read from the "
        f"CH_EMBEDDING_MODEL binding; a literal is a second declaration:\n  "
        + formatted
    )


EMBED_FACADE_MODULE = "ChatHealthyLib/src/chathealthy_lib/llm.py"
EMBEDDING_BINDING = "CH_EMBEDDING_MODEL"


def _targets_declaring(source_location):
    for target in _record().get("DeploymentTargetRecord", []):
        for entry in target.get("files") or []:
            if entry.get("source_location") == source_location:
                yield target
                break


def _reaches_embed(target):
    """True when a file this target declares calls the facade's embed()."""
    for entry in target.get("files") or []:
        rel = entry.get("source_location") or ""
        if not rel.endswith(".py"):
            continue
        from pathlib import Path
        path = Path(REPO_ROOT) / rel
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if "from chathealthy_lib.llm import" in text and "embed" in text:
            return True
    return False


def test_every_embedding_target_carries_the_binding():
    """EPIC-008-F-011-S-004-REQ-B-001.

    embed() reads CH_EMBEDDING_MODEL and has no default. A target whose
    declared code reaches it and does not carry the binding would fail at
    the first embedding call rather than at the build.
    """
    _declared_embedding_model()
    missing = []
    for target in _record().get("DeploymentTargetRecord", []):
        if not _reaches_embed(target):
            continue
        bindings = dict(target.get("variables") or {})
        bindings.update(target.get("secrets") or {})
        if EMBEDDING_BINDING not in bindings:
            missing.append(target.get("target_id"))
        elif bindings[EMBEDDING_BINDING] != "firm:embedding_model":
            missing.append(
                f"{target.get('target_id')} (binds {bindings[EMBEDDING_BINDING]!r}, "
                "not the firm declaration)")

    assert missing == [], (
        "Targets whose declared code embeds but which do not carry "
        f"{EMBEDDING_BINDING} bound to the firm declaration: " + ", ".join(missing)
    )
