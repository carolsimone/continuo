"""The code bundle's contract, version 1, pinned against the writer.

External consumers read a release's bundle (docs/read-contract.md). The document
`build_code_bundle` writes for a fixed release is compared with
fixtures/code_bundle_v1.json, a copy of the fixture pkg/codebundle's tests read,
so the Go reader and this writer agree on one document.

The fixture holds a dbt node and a python node. The python node comes from
`parse_python_contract`, so its `raw_code` JSON (whose `kind`, `owner` and
`reads` keys are part of the declared read set) is what the real parser writes.
"""
import json
from pathlib import Path

from domain.model import ManifestNode
from service.code_bundle import CONTRACT_VERSION, build_code_bundle
from service.python_contract_parser import parse_python_contract
from tests.test_python_contract_parser import make_entry, write_contract

_FIXTURE = Path(__file__).parent / "fixtures" / "code_bundle_v1.json"

_CHANGED = (
    "The bundle this writer produces no longer equals fixtures/code_bundle_v1.json. "
    "A key that is added, or one outside the declared read set that changes, is not "
    "breaking: update this fixture and its copy in pkg/codebundle/testdata, and "
    "docs/read-contract.md if the read set grows. A key of the read set that is "
    "removed, renamed, retyped or given another meaning is breaking: raise "
    "CONTRACT_VERSION, add a fixture for the new version, and list the bump under "
    "Breaking in deploy/continuo/CHANGELOG.md."
)


def _dbt_node() -> ManifestNode:
    return ManifestNode(
        table_name="revenue", schema_name="analytics", service_name="service-a",
        owner="team-a", schedule_name="daily", criticality="SECONDARY",
        candidate_sql="select 1 as id from analytics.orders", node_type="dbt-model",
        raw_code="select 1 as id from {{ ref('orders') }}",
        config={"materialized": "table", "meta": {"owner": "team-a"}},
        source_hash="s1", shared_code_hash="m1", config_hash="c1",
        content_hash="sha256:aaa", code_unit_ids=["service-a:macro.svc.m1"],
    )


def _bundle(tmp_path) -> dict:
    python_nodes, _ = parse_python_contract(
        write_contract(tmp_path, make_entry(owner="marketing")), "img:1"
    )
    shared = {
        "service-a:macro.svc.m1": {"source": "select 1", "checksum": "u1", "depends_on": []},
    }
    return build_code_bundle("rel-1", [_dbt_node(), *python_nodes], shared)


def test_the_bundle_matches_the_v1_fixture(tmp_path):
    fixture = json.loads(_FIXTURE.read_text(encoding="utf-8"))

    assert _bundle(tmp_path) == fixture, _CHANGED


def test_the_fixture_is_the_version_the_writer_declares():
    fixture = json.loads(_FIXTURE.read_text(encoding="utf-8"))

    assert fixture["contract_version"] == CONTRACT_VERSION, _CHANGED
