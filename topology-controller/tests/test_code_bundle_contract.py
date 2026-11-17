"""The code bundle's contract, version 1, pinned against the writer.

External consumers read a release's bundle (docs/read-contract.md). The document
`build_code_bundle` writes for a fixed release is compared with
fixtures/code_bundle_v1.json, a copy of the fixture pkg/codebundle's tests read,
so the Go reader and this writer agree on one document.

The fixture holds a dbt node and a python node. The python node comes from
`build_python_node`, which builds every python node, so its `raw_code` JSON
(whose `kind`, `owner` and `reads` keys are part of the declared read set), its
`projection` (the code projection, outside the read set) and its hashes are what
the real builder writes.
"""
import json
from pathlib import Path

from domain.contract_vocabulary import NodeType
from domain.model import ManifestNode
from service.code_bundle import CONTRACT_VERSION, build_code_bundle
from service.python_node_builder import PythonNodeFields, build_python_node

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
        compiled_hash="d1",
        hashed_config={"materialized": "table", "relation": {"schema": "analytics", "alias": "revenue"}},
    )


def _python_node() -> ManifestNode:
    return build_python_node(
        PythonNodeFields(
            schema="analytics", table="table_test", owner="marketing", schedule="daily",
            criticality="SECONDARY", kind=NodeType.PYTHON_NODE, description="",
            script="scripts/table_test.py",
            reads={"joined": "select a from analytics.table_a", "ids": "select id from analytics.table_b"},
            dependency_sqls=["select id from analytics.table_b", "select a from analytics.table_a"],
            csv_source="",
            output_columns=[{"name": "id", "type": "INTEGER", "nullable": False},
                            {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True}],
            extra_columns="raise", config={}, secret_ref="", mode_type="replace",
            primary_key=[], assets=[], timeout_seconds=None,
        ),
        service="marketing-py", image_tag="img:1", source_hash="aaa111", shared_code_hash="bbb222",
    )


def _bundle() -> dict:
    shared = {
        "service-a:macro.svc.m1": {"source": "select 1", "checksum": "u1", "depends_on": []},
    }
    return build_code_bundle("rel-1", [_dbt_node(), _python_node()], shared, tests=[], provenance={})


def test_the_bundle_matches_the_v1_fixture():
    fixture = json.loads(_FIXTURE.read_text(encoding="utf-8"))

    assert _bundle() == fixture, _CHANGED


def test_the_fixture_is_the_version_the_writer_declares():
    fixture = json.loads(_FIXTURE.read_text(encoding="utf-8"))

    assert fixture["contract_version"] == CONTRACT_VERSION, _CHANGED
