# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).

"""The reconciler brings each governed collection to what the manifest says.

The declarations under test are the real ones, read out of
deployment_architecture.json: the real addresses, the real identity keys and
the real record bodies. Nothing here invents a record or a key, so what is
proved is that the configuration we actually ship reconciles -- and the only
difference from a production run is the collection name the manifest
currently carries.

Every case a deploy can meet is one test: the collection is absent; a stored
record differs; a stored record is declared by nobody; a stored record is
declared by another binding; and a declaration names no records at all.

The collection is real, on the real cluster, reached with the identity the
manifest declares. A double would prove that the code calls the methods it
calls, which is not the question.
"""

import copy
import json
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(REPO_ROOT / ".env")
sys.path.insert(0, str(REPO_ROOT / "ChatHealthyLib" / "src"))
sys.path.insert(0, str(REPO_ROOT / "architecture" / "DevOpsBuildDeployAndEnvironmentManagement"))

from chathealthy_lib.exceptions import ChatHealthyException  # noqa: E402
from chathealthy_lib.mongo_utilities import ChatHealthyMongoUtilities  # noqa: E402

import _deploy_chain  # noqa: E402
from target_record import TargetRecord  # noqa: E402

MANIFEST = REPO_ROOT / "brain" / "machine_artifacts" / "content" / "deployment_architecture.json"
GOVERNED_TARGET = "target_atlas_frontend"
WRITER_TARGET = "target_identity_devops_user"
CLUSTER = "ChatHealthyFrontEnd"
ENVIRONMENTS = ("local", "dev")


def _target_dict(target_id):
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for record in manifest["DeploymentTargetRecord"]:
        if record.get("target_id") == target_id:
            return copy.deepcopy(record)
    raise ChatHealthyException(
        mode="value_error",
        component="test_config_collection_reconciler",
        message=f"{target_id} is not in the manifest")


def _declarations(env):
    """Every collection the manifest governs for this environment."""
    for binding in _target_dict(GOVERNED_TARGET)["environments"]:
        if binding["env_binding"] == env:
            return binding.get("config_collections") or []
    return []


def _entry(env, address):
    for entry in _declarations(env):
        if entry["address"] == address:
            return entry
    raise ChatHealthyException(
        mode="value_error",
        component="test_config_collection_reconciler",
        message=f"{address} is not declared for {env!r}")


def _identity(env):
    for binding in _target_dict(WRITER_TARGET)["environments"]:
        if binding["env_binding"] == env:
            return (binding.get("identity") or {}).get("name") or ""
    raise ChatHealthyException(
        mode="value_error",
        component="test_config_collection_reconciler",
        message=f"{WRITER_TARGET} names no identity for {env!r}")


def _catalog():
    return [TargetRecord.from_dict(_target_dict(WRITER_TARGET))]


def _run(env, replacement=None):
    """Reconcile as the deploy for `env` would.

    `replacement` swaps one address's records, so a case can declare less
    than the manifest does without inventing anything else.
    """
    target = _target_dict(GOVERNED_TARGET)
    for binding in target["environments"]:
        for entry in binding.get("config_collections") or []:
            entry["address"] = _probe(entry["address"])
    if replacement is not None:
        replacement = (_probe(replacement[0]), replacement[1])
    if replacement is not None:
        address, records = replacement
        for binding in target["environments"]:
            if binding["env_binding"] != env:
                continue
            for entry in binding.get("config_collections") or []:
                if entry["address"] == address:
                    entry["records"] = records
    _deploy_chain.reconcile_config_collections(
        TargetRecord.from_dict(target), env, _catalog())


# The suite never touches the collections the application reads. Each
# declared address is rewritten to a copy of the same name under a test_
# prefix, so the records, the keys and the reconciler are the real ones and
# the only difference from a production run is where the bytes land.
PROBE_PREFIX = "test_"


def _probe(address):
    database, _, collection = address.partition(".")
    return f"{database}.{PROBE_PREFIX}{collection}"


def _split(address):
    database, _, collection = address.partition(".")
    return database, collection


def _key_of(record, identity_key):
    return {field: record[field] for field in identity_key}


def _cases():
    """(env, address) for every declaration the manifest carries."""
    return [(env, entry["address"])
            for env in ENVIRONMENTS for entry in _declarations(env)]


@pytest.fixture()
def cluster():
    return ChatHealthyMongoUtilities().getConnection(_identity("local"), CLUSTER)


@pytest.fixture()
def clean(cluster):
    """Every governed collection absent before the case and after it."""
    addresses = {_probe(entry["address"])
                 for env in ENVIRONMENTS for entry in _declarations(env)}
    for address in addresses:
        database, collection = _split(address)
        cluster[database][collection].drop()
    yield
    for address in addresses:
        database, collection = _split(address)
        cluster[database][collection].drop()


@pytest.mark.parametrize("env,address", _cases())
class TestEachDeclaredCollectionBecomesWhatTheManifestSays:

    def test_the_collection_is_created_and_its_records_land_verbatim(
            self, cluster, clean, env, address):
        database, collection = _split(_probe(address))
        assert collection not in cluster[database].list_collection_names(), \
            "the collection was already there, so creating it proves nothing"
        _run(env)
        assert collection in cluster[database].list_collection_names(), \
            "a declared collection that was absent was not created"
        declared = _entry(env, address)["records"]
        stored = list(cluster[database][collection].find({}, {"_id": 0}))
        assert stored == declared, \
            f"{address} holds records that differ from the " \
            f"{len(declared)} the manifest declares"

    def test_a_record_that_differs_is_put_back(self, cluster, clean, env,
                                               address):
        database, collection = _split(_probe(address))
        entry = _entry(env, address)
        _run(env)
        record = entry["records"][0]
        key = _key_of(record, entry["identity_key"])
        cluster[database][collection].replace_one(
            key, {**key, "corrupted_by_the_test": True})
        _run(env)
        stored = cluster[database][collection].find_one(key, {"_id": 0})
        assert stored == record, \
            f"a corrupted record in {address} was not restored to what the " \
            f"manifest declares"

    def test_a_record_nobody_declares_is_removed(self, cluster, clean, env,
                                                 address):
        database, collection = _split(_probe(address))
        entry = _entry(env, address)
        _run(env)
        intruder = {field: "nobody_declared_this"
                    for field in entry["identity_key"]}
        cluster[database][collection].insert_one(dict(intruder))
        _run(env)
        assert cluster[database][collection].find_one(intruder) is None, \
            f"a record absent from the manifest survived a deploy of {address}"
        declared = _entry(env, address)["records"]
        stored = list(cluster[database][collection].find({}, {"_id": 0}))
        assert stored == declared, \
            f"{address} no longer matches the manifest after the sweep"

    def test_a_record_another_binding_declares_survives(self, cluster, clean,
                                                        env, address):
        database, collection = _split(_probe(address))
        others = [e for other in ENVIRONMENTS if other != env
                  for e in _declarations(other) if e["address"] == address]
        if not others:
            pytest.skip(f"{address} is declared by no other binding")
        theirs = others[0]["records"][0]
        _run(env)
        cluster[database][collection].insert_one(dict(theirs))
        _run(env)
        key = _key_of(theirs, others[0]["identity_key"])
        assert cluster[database][collection].find_one(key) is not None, \
            f"{address}: a record another binding declares was deleted by " \
            f"the {env} deploy"

    def test_declaring_no_records_drops_the_collection(self, cluster, clean,
                                                       env, address):
        database, collection = _split(_probe(address))
        _run(env)
        assert collection in cluster[database].list_collection_names(), \
            "the collection was never created, so dropping it proves nothing"
        _run(env, replacement=(address, []))
        assert collection not in cluster[database].list_collection_names(), \
            f"{address} was declared empty and was not dropped"
