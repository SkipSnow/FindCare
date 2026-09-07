# Copyright (c) 2026 ChatHealthy.ai LLC. All rights reserved.
# Licensed under the FindCare Evaluation License (FEL-1.0).

"""The reconciler brings a governed collection to exactly what is declared.

Four things must hold, and each is one test: a declared collection that does
not exist is created and its records inserted; a stored record that differs
from the declaration is corrected; a stored record that is not declared is
deleted; and declaring no records drops the collection.

The collection is real, on the real cluster, reached with the identity the
manifest declares. A double would prove that the code calls the methods it
calls, which is not the question.
"""

import os
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

ENV = "local"
DATABASE = "ChatHealthyConfig"
COLLECTION = "test_reconciler_probe"
ADDRESS = f"{DATABASE}.{COLLECTION}"
IDENTITY = "DevOpsUser"
CLUSTER = "ChatHealthyFrontEnd"


def _target(records):
    """A target declaring one governed collection holding these records."""
    return TargetRecord.from_dict({
        "target_id": "target_atlas_frontend",
        "target_kind": "atlas",
        "files": [],
        "environments": [{
            "env_binding": ENV,
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
        }],
    })


def _catalog():
    """The identity target the governed target points at.

    The reconciler asks the catalog one question -- what does this identity
    target authenticate as here -- and the answer is a fact of the manifest,
    not behaviour under test.
    """
    return [TargetRecord.from_dict({
        "target_id": "target_identity_devops_user",
        "target_kind": "identity",
        "files": [],
        "environments": [{
            "env_binding": ENV,
            "node_address": IDENTITY,
            "identity": {"name": IDENTITY},
        }],
    })]


@pytest.fixture()
def probe():
    """The collection under test, absent before and after."""
    client = ChatHealthyMongoUtilities().getConnection(IDENTITY, CLUSTER)
    client[DATABASE][COLLECTION].drop()
    yield client[DATABASE][COLLECTION]
    client[DATABASE][COLLECTION].drop()


def _run(records):
    _deploy_chain.reconcile_config_collections(_target(records), ENV, _catalog())


class TestAGovernedCollectionBecomesWhatIsDeclared:

    def test_a_collection_that_does_not_exist_is_created(self, probe):
        _run([{"env": ENV, "colour": "green"}])
        assert probe.database.list_collection_names().count(COLLECTION) == 1, \
            "a declared collection that was absent was not created"

    def test_a_declared_record_that_is_absent_is_inserted(self, probe):
        _run([{"env": ENV, "colour": "green"}])
        stored = probe.find_one({"env": ENV}, {"_id": 0})
        assert stored == {"env": ENV, "colour": "green"}, \
            f"the declared record was not inserted as declared; found {stored!r}"

    def test_a_record_that_differs_is_corrected(self, probe):
        _run([{"env": ENV, "colour": "green"}])
        probe.replace_one({"env": ENV}, {"env": ENV, "colour": "red"})
        _run([{"env": ENV, "colour": "green"}])
        stored = probe.find_one({"env": ENV}, {"_id": 0})
        assert stored == {"env": ENV, "colour": "green"}, \
            f"a record differing from the manifest was left as it was: {stored!r}"

    def test_a_record_that_is_not_declared_is_deleted(self, probe):
        _run([{"env": ENV, "colour": "green"}])
        probe.insert_one({"env": "nobody_declared_this", "colour": "blue"})
        _run([{"env": ENV, "colour": "green"}])
        assert probe.find_one({"env": "nobody_declared_this"}) is None, \
            "a stored record absent from the manifest survived the deploy"
        assert probe.count_documents({}) == 1, \
            "the collection holds records the manifest does not declare"

    def test_declaring_no_records_drops_the_collection(self, probe):
        _run([{"env": ENV, "colour": "green"}])
        _run([])
        assert COLLECTION not in probe.database.list_collection_names(), \
            "a collection declared empty was not dropped"
