import json

import pytest

import gen_topology as g


def topo(**overrides):
    args = dict(nodes=6, levels=2, fan_in=2, schedule="bench-t", service="bench", schema="bench",
                image_tag="v1", fail_root=False)
    args.update(overrides)
    return g.build_topology(**args)


def test_level_sizes_spread_the_remainder_over_early_levels():
    assert g.level_sizes(10, 3, False) == [4, 3, 3]


def test_fail_root_makes_level_zero_a_single_node():
    assert g.level_sizes(10, 3, True) == [1, 5, 4]


def test_levels_cannot_exceed_nodes():
    with pytest.raises(ValueError):
        g.level_sizes(2, 3, False)


def test_edges_follow_the_fan_in():
    by_table = {n["table_name"]: n for n in topo()}
    assert by_table["n00_0000"]["upstream_unique_ids"] == []
    assert by_table["n01_0000"]["upstream_unique_ids"] == ["bench.n00_0000", "bench.n00_0001"]
    assert by_table["n01_0002"]["upstream_unique_ids"] == ["bench.n00_0002", "bench.n00_0000"]


def test_fail_root_feeds_every_level_one_node():
    nodes = topo(nodes=5, levels=3, fail_root=True)
    level_one = [n for n in nodes if n["table_name"].startswith("n01_")]
    assert level_one
    assert all(n["upstream_unique_ids"] == ["bench.fail_root"] for n in level_one)


def test_every_node_is_a_dbt_model_with_one_test_writing_its_own_relation():
    nodes = topo(nodes=4, levels=2, fan_in=1)
    assert {n["node_type"] for n in nodes} == {"dbt-model"}
    assert {n["test_count"] for n in nodes} == {1}
    assert all("changed" not in n for n in nodes)
    assert all(n["resolved_relation_id"] == n["unique_id"] for n in nodes)


def test_validate_rejects_a_label_unsafe_schedule():
    with pytest.raises(ValueError):
        g.validate(topo(schedule="Bench_X"), "Bench_X")


def test_validate_rejects_a_job_name_prefix_over_54_characters():
    with pytest.raises(ValueError, match="54"):
        g.validate(topo(nodes=1, levels=1, service="s" * 40), "bench-t")


def test_payload_carries_the_release_id_and_the_nodes():
    nodes = topo(nodes=2, levels=1)
    assert g.build_payload(nodes, "rel-1") == {"release_id": "rel-1", "topology": nodes}


def test_main_prints_the_payload(capsys):
    assert g.main(["--nodes", "4", "--levels", "2", "--schedule", "bench-t", "--release-id", "rel-x"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["release_id"] == "rel-x"
    assert len(out["topology"]) == 4
