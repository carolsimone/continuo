import pytest

import topology_io as t


def snap(uid, schedule="daily", ups=(), **extra):
    schema, table = uid.split(".")
    node = {"unique_id": uid, "schema_name": schema, "table_name": table, "service_name": "core",
            "node_type": "dbt-model", "content_hash": "h-" + table, "test_count": 2, "image_tag": "abc",
            "upstream_unique_ids": list(ups), "schedule": schedule, "original_file_path": f"models/{table}.sql",
            "resolved_relation_id": "x", "candidate_artifact_uri": "s3://gone"}
    node.update(extra)
    return node


def test_snapshot_node_becomes_an_unchanged_wire_node():
    wire = t.snapshot_to_wire(snap("a.orders", secret_ref=""))
    assert "resolved_relation_id" not in wire and "candidate_artifact_uri" not in wire
    assert "secret_ref" not in wire
    assert wire["changed"] is False and wire["test_count"] == 2
    assert t.snapshot_to_wire(snap("a.api", node_type="python-api", secret_ref="continuo-api-x"))["secret_ref"] == "continuo-api-x"


def test_restore_payload_keeps_the_release_id_and_first_image_tag_per_service():
    payload = t.restore_payload({"release_id": "rel-9", "topology": [snap("a.x"), snap("a.y", image_tag="other")]})
    assert payload["release_id"] == "rel-9"
    assert payload["image_tags"] == {"core": "abc"}
    assert [n["unique_id"] for n in payload["topology"]] == ["a.x", "a.y"]


def test_union_rejects_a_bench_node_that_collides_with_a_live_node():
    base = t.restore_payload({"release_id": "r", "topology": [snap("bench.n00_0000")]})
    with pytest.raises(ValueError, match="clash"):
        t.union(base, {"topology": [{"unique_id": "bench.n00_0000", "schedule": "bench-x"}]}, "b")


def test_union_rejects_a_bench_schedule_the_live_topology_uses():
    base = t.restore_payload({"release_id": "r", "topology": [snap("a.x", schedule="daily")]})
    with pytest.raises(ValueError, match="schedule"):
        t.union(base, {"topology": [{"unique_id": "bench.n", "schedule": "daily"}]}, "b")


def test_union_keeps_live_nodes_first_and_unchanged():
    base = t.restore_payload({"release_id": "r", "topology": [snap("a.x")]})
    bench = {"topology": [{"unique_id": "bench.n", "schedule": "bench-x", "service_name": "bench",
                           "image_tag": "v1", "changed": False}]}
    merged = t.union(base, bench, "bench-rel")
    assert merged["release_id"] == "bench-rel"
    assert [n["unique_id"] for n in merged["topology"]] == ["a.x", "bench.n"]
    assert merged["topology"][0]["changed"] is False
    assert merged["image_tags"] == {"core": "abc", "bench": "v1"}


def test_compare_accepts_live_graphs_that_include_unscheduled_upstreams():
    payload = t.restore_payload({"release_id": "r", "topology": [
        snap("a.x"), snap("a.y", ups=["a.s"]), snap("a.s", schedule="")]})
    graphs = [{"nodes": [{"schema_name": "a", "table_name": "x"}, {"schema_name": "a", "table_name": "y"},
                         {"schema_name": "A", "table_name": "S"}]}]
    assert t.compare(payload, graphs) == []


def test_compare_reports_both_directions():
    payload = t.restore_payload({"release_id": "r", "topology": [snap("a.x"), snap("a.c")]})
    graphs = [{"nodes": [{"schema_name": "a", "table_name": "x"}, {"schema_name": "a", "table_name": "extra"}]}]
    problems = t.compare(payload, graphs)
    assert len(problems) == 2
    assert "a.c" in problems[0] and "a.extra" in problems[1]


def test_restore_payload_drops_test_nodes_a_promotion_never_publishes():
    payload = t.restore_payload({"release_id": "r", "topology": [
        snap("a.x"), snap("a.not_null_x", schedule="", node_type="dbt-test", ups=["a.x"])]})
    assert [n["unique_id"] for n in payload["topology"]] == ["a.x"]


def test_compare_rejects_a_payload_that_carries_test_nodes():
    payload = {"topology": [t.snapshot_to_wire(snap("a.x")),
                            t.snapshot_to_wire(snap("a.t", schedule="", node_type="dbt-test"))]}
    graphs = [{"nodes": [{"schema_name": "a", "table_name": "x"}]}]
    problems = t.compare(payload, graphs)
    assert len(problems) == 1 and "dbt-test" in problems[0]
