# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).

"""The reconciler brings a governed collection to exactly what is declared.

Every case a deploy can meet is one test: the collection is absent; a
declared record is absent; a stored record differs; a stored record is
declared by nobody; a stored record is declared by another binding; a
declaration mixes records that exist with records that do not; a
declaration is already satisfied; and a declaration names no records at all.

Each runs against both environments, because an environment is only a
binding and the reconciler must not behave differently in one.

The collection is real, on the real cluster, reached with the identity the
manifest declares. A double would prove that the code calls the methods it
calls, which is not the question.
"""

import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(REPO_ROOT / ".env")
sys.path.insert(0, str(REPO_ROOT / "ChatHealthyLib" / "src"))
sys.path.insert(0, str(REPO_ROOT / "architecture" / "DevOpsBuildDeployAndEnvironmentManagement"))

from chathealthy_lib.mongo_utilities import ChatHealthyMongoUtilities  # noqa: E402

import _deploy_chain  # noqa: E402
from target_record import TargetRecord  # noqa: E402

DATABASE = "ChatHealthyConfig"
COLLECTION = "test_reconciler_probe"
ADDRESS = f"{DATABASE}.{COLLECTION}"
IDENTITY = "DevOpsUser"
CLUSTER = "ChatHealthyFrontEnd"
ENVIRONMENTS = ("local", "dev")
OTHER_BINDING = "qa"


def _binding(env, records):
    """One environment binding declaring the governed collection."""
    return {
        "env_binding": env,
        "node_address": CLUSTER,
        "platforms": [{
            "name": "atlas",
            "facts": [
                {"name": "role", "value": "host"},
                {"name": "cluster", "facts": [
                    {"name": "cluster_name", "value": CLUSTER},
                    {"name": "runtime_consumer", "facts": [
                        {"name": "identity_target_id",
                         "value": "target_identity_devops_user"},
                        {"name": "operation", "value": "mongo_write"},
                    ]},
                ]},
            ],
        }],
        "config_collections": [{
            "address": ADDRESS,
            "identity_key": ["env"],
            "records": records,
        }],
    }


def _catalog(env):
    """The identity target the governed target points at."""
    return [TargetRecord.from_dict({
        "target_id": "target_identity_devops_user",
        "target_kind": "identity",
        "files": [],
        "environments": [{
            "env_binding": env,
            "node_address": IDENTITY,
            "identity": {"name": IDENTITY},
        }],
    })]


def _run(env, records, other=None):
    """Reconcile as `env` would. `other` is a second binding's declaration."""
    bindings = [_binding(env, records)]
    if other is not None:
        bindings.append(_binding(OTHER_BINDING, other))
    target = TargetRecord.from_dict({
        "target_id": "target_atlas_frontend",
        "target_kind": "atlas",
        "files": [],
        "environments": bindings,
    })
    _deploy_chain.reconcile_config_collections(target, env, _catalog(env))


@pytest.fixture()
def probe():
    """The collection under test, absent before and after."""
    client = ChatHealthyMongoUtilities().getConnection(IDENTITY, CLUSTER)
    client[DATABASE][COLLECTION].drop()
    yield client[DATABASE][COLLECTION]
    client[DATABASE][COLLECTION].drop()


def _stored(probe):
    return sorted(d["env"] for d in probe.find({}, {"_id": 0}))


@pytest.mark.parametrize("env", ENVIRONMENTS)
class TestAGovernedCollectionBecomesWhatIsDeclared:

    def test_a_collection_that_does_not_exist_is_created(self, probe, env):
        assert COLLECTION not in probe.database.list_collection_names(), \
            "the collection was already there, so creating it proves nothing"
        _run(env, [{"env": env, "colour": "green"}])
        assert COLLECTION in probe.database.list_collection_names(), \
            "a declared collection that was absent was not created"

    def test_a_declared_record_that_is_absent_is_inserted(self, probe, env):
        _run(env, [{"env": env, "colour": "green"}])
        found = probe.find_one({"env": env}, {"_id": 0})
        assert found == {"env": env, "colour": "green"}, \
            f"the declared record was not inserted as declared; found {found!r}"

    def test_a_record_that_differs_is_corrected(self, probe, env):
        _run(env, [{"env": env, "colour": "green"}])
        probe.replace_one({"env": env}, {"env": env, "colour": "red"})
        _run(env, [{"env": env, "colour": "green"}])
        found = probe.find_one({"env": env}, {"_id": 0})
        assert found == {"env": env, "colour": "green"}, \
            f"a record differing from the manifest was left as it was: {found!r}"

    def test_a_record_that_is_not_declared_is_deleted(self, probe, env):
        _run(env, [{"env": env, "colour": "green"}])
        probe.insert_one({"env": "nobody_declared_this", "colour": "blue"})
        _run(env, [{"env": env, "colour": "green"}])
        assert _stored(probe) == [env], \
            f"the collection holds {_stored(probe)!r}; only the declared " \
            f"record should remain"

    def test_a_record_another_binding_declares_is_left_alone(self, probe, env):
        theirs = [{"env": OTHER_BINDING, "colour": "blue"}]
        _run(env, [{"env": env, "colour": "green"}], other=theirs)
        probe.insert_one({"env": OTHER_BINDING, "colour": "blue"})
        _run(env, [{"env": env, "colour": "green"}], other=theirs)
        assert _stored(probe) == sorted([env, OTHER_BINDING]), \
            f"the collection holds {_stored(probe)!r}; a record another " \
            f"binding declares must survive this binding's deploy"

    def test_a_mix_of_present_and_absent_records_lands_whole(self, probe, env):
        _run(env, [{"env": env, "colour": "green"}])
        _run(env, [{"env": env, "colour": "green"},
                   {"env": "second", "colour": "blue"},
                   {"env": "third", "colour": "grey"}])
        found = {d["env"]: d["colour"] for d in probe.find({}, {"_id": 0})}
        assert found == {env: "green", "second": "blue", "third": "grey"}, \
            f"a declaration mixing existing and new records did not land " \
            f"whole; found {found!r}"

    def test_a_declaration_already_satisfied_writes_nothing(self, probe, env):
        _run(env, [{"env": env, "colour": "green"}])
        before = probe.find_one({"env": env})["_id"]
        _run(env, [{"env": env, "colour": "green"}])
        after = probe.find_one({"env": env})["_id"]
        assert after == before, \
            "an unchanged record was rewritten; a satisfied declaration must " \
            "write nothing"
        assert _stored(probe) == [env], \
            f"a second run of the same declaration left {_stored(probe)!r}"

    def test_declaring_no_records_drops_the_collection(self, probe, env):
        _run(env, [{"env": env, "colour": "green"}])
        assert COLLECTION in probe.database.list_collection_names(), \
            "the collection was never created, so dropping it proves nothing"
        _run(env, [])
        assert COLLECTION not in probe.database.list_collection_names(), \
            "a collection declared empty was not dropped"
