import json

from domain.model import ManifestNode
from service.code_bundle import CONTRACT_VERSION, build_code_bundle


def _mk(table="revenue", schema="analytics", **over):
    defaults = dict(
        table_name=table, schema_name=schema, service_name="service-a",
        owner="team-a", schedule_name="daily", criticality="SECONDARY",
        candidate_sql="select 1", node_type="dbt-model",
        raw_code="select 1 -- raw", config={"materialized": "table"},
        source_hash="s", shared_code_hash="m", config_hash="c",
        content_hash="sha256:x", code_unit_ids=["macro.svc.m1"],
    )
    defaults.update(over)
    return ManifestNode(**defaults)


def test_bundle_shape_and_keys():
    shared = {"macro.svc.m1": {"source": "select 1", "checksum": "abc", "depends_on": []}}
    bundle = build_code_bundle("rel-1", [_mk()], shared, tests=[], provenance={})
    assert bundle["contract_version"] == CONTRACT_VERSION == 1
    assert bundle["release_id"] == "rel-1"
    node = bundle["nodes"]["analytics.revenue"]          # continuo unique_id key
    assert node["runtime"] == "dbt"
    assert node["raw_code"] == "select 1 -- raw"
    assert node["compiled_code"] == "select 1"
    assert node["config"] == {"materialized": "table"}
    assert node["source_hash"] == "s" and node["shared_code_hash"] == "m"
    assert node["config_hash"] == "c" and node["content_hash"] == "sha256:x"
    assert node["code_unit_ids"] == ["macro.svc.m1"]
    assert bundle["shared_code"] == shared


def test_seed_ships_empty_code():
    seed = _mk(table="countries", node_type="dbt-seed", candidate_sql="", raw_code="",
               code_unit_ids=[], shared_code_hash="")
    bundle = build_code_bundle("rel-1", [seed], {}, tests=[], provenance={})
    node = bundle["nodes"]["analytics.countries"]
    assert node["raw_code"] == "" and node["compiled_code"] == ""


def test_a_dbt_node_carries_hashed_config_and_compiled_hash():
    node = _mk(hashed_config={"materialized": "table", "relation": {"schema": "analytics", "alias": "revenue"}},
               compiled_hash="6e0f")
    entry = build_code_bundle("rel-1", [node], {}, tests=[], provenance={})["nodes"]["analytics.revenue"]
    assert entry["hashed_config"]["relation"] == {"schema": "analytics", "alias": "revenue"}
    assert entry["compiled_hash"] == "6e0f"


def test_a_python_node_carries_no_hashed_config():
    node = _mk(runtime="python", node_type="python-node", hashed_config=None)
    entry = build_code_bundle("rel-1", [node], {}, tests=[], provenance={})["nodes"]["analytics.revenue"]
    assert "hashed_config" not in entry and entry["compiled_hash"] == ""


def test_a_python_node_carries_its_projection_beside_its_raw_code_and_a_dbt_node_none():
    projection = json.dumps({"kind": "python-node", "table": "revenue"}, sort_keys=True, indent=2)
    py = _mk(runtime="python", node_type="python-node", raw_code='{"owner": "marketing"}', projection=projection)
    entry = build_code_bundle("rel-1", [py], {}, tests=[], provenance={})["nodes"]["analytics.revenue"]
    assert entry["projection"] == projection and entry["raw_code"] == '{"owner": "marketing"}'
    dbt = _mk(hashed_config={"materialized": "table"}, compiled_hash="6e0f")
    assert "projection" not in build_code_bundle("rel-1", [dbt], {}, tests=[], provenance={})["nodes"]["analytics.revenue"]


def test_the_tests_map_carries_each_tracked_test_and_provenance_rides_beside():
    t = _mk(table="not_null_revenue_id", node_type="dbt-test", identity="test.svc.not_null_revenue_id.ab12",
            candidate_sql="select id from analytics.revenue where id is null", compiled_hash="")
    prov = {"svc": {"dbt_version": "1.12.0b1", "image_tag": "abc", "packages": {}, "path_checksum_seeds": []}}
    bundle = build_code_bundle("rel-1", [], {}, tests=[t], provenance=prov)
    assert bundle["tests"]["test.svc.not_null_revenue_id.ab12"]["compiled_code"].startswith("select id")
    assert bundle["tests"]["test.svc.not_null_revenue_id.ab12"] == {
        "compiled_code": "select id from analytics.revenue where id is null",
        "source_hash": "s", "shared_code_hash": "m", "config_hash": "c", "compiled_hash": "",
        "content_hash": "sha256:x",
    }
    assert bundle["provenance"] == prov
