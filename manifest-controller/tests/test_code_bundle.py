from domain.model import ManifestNode
from service.code_bundle import CONTRACT_VERSION, build_code_bundle


def _mk(table="revenue", schema="analytics", **over):
    defaults = dict(
        table_name=table, schema_name=schema, service_name="service-a",
        owner="team-a", schedule_name="daily", criticality="SECONDARY",
        compiled_sql="select 1", node_type="dbt-model",
        raw_code="select 1 -- raw", config={"materialized": "table"},
        source_hash="s", shared_code_hash="m", config_hash="c",
        content_hash="sha256:x", code_unit_ids=["macro.svc.m1"],
    )
    defaults.update(over)
    return ManifestNode(**defaults)


def test_bundle_shape_and_keys():
    shared = {"macro.svc.m1": {"source": "select 1", "checksum": "abc", "depends_on": []}}
    bundle = build_code_bundle("rel-1", [_mk()], shared)
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
    seed = _mk(table="countries", node_type="dbt-seed", compiled_sql="", raw_code="",
               code_unit_ids=[], shared_code_hash="")
    bundle = build_code_bundle("rel-1", [seed], {})
    node = bundle["nodes"]["analytics.countries"]
    assert node["raw_code"] == "" and node["compiled_code"] == ""
