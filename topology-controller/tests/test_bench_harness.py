import argparse
import gzip
import hashlib
import json
import posixpath
import re

import pytest
import yaml
from botocore.exceptions import ClientError
from continuo_contracts import ontology as ontology_file
from continuo_contracts.ontology_check import ontology_findings
from continuo_contracts.vocabulary import continuo_values

from bench.compare import compare
from bench.estate import generate_estate
from bench.memory_s3 import MemoryS3
from bench.run import _child_command, _parser, _run_child, build_handler
from domain.model import ManifestFile, ManifestKind
from service.compile_extras import load_compile_extras
from service.contract_bundle import contract_bundle_key
from service.contract_validation import ContractValidator
from service.odcs_bundle_parser import parse_odcs_bundle
from service.release_timings import ReleaseTimings, Stage
from tests.fakes import FakeClock, ScriptedMemoryProbe


def test_the_estate_is_the_same_for_the_same_seed():
    assert generate_estate(20, columns=4).objects == generate_estate(20, columns=4).objects


def test_the_estate_holds_models_tests_seeds_and_python_nodes():
    estate = generate_estate(20, columns=4)
    assert (estate.models, estate.tests, estate.seeds, estate.python_nodes) == (20, 40, 10, 2)
    assert len(estate.requests) == 11
    assert sum(key.endswith("/compile-extras.json") for key in estate.objects) == 10


def test_the_real_handler_parses_the_estate():
    estate = generate_estate(20, columns=4)
    handler, publisher = build_handler(
        estate, MemoryS3(estate.objects), clock=FakeClock(), memory=ScriptedMemoryProbe(0), validator=ContractValidator())
    handler.handle(estate.release_id)
    assert publisher.ok, publisher.failure
    assert publisher.node_count == estate.dbt_nodes + estate.python_nodes


def test_the_estate_built_without_the_ontology_hashes_to_its_recorded_digest():
    """The 100-model, 20-column estate hashes to one recorded value (the python bundle's `producer`
    stamp left out), so two baselines measured apart parse the same dbt manifests and python
    contracts. A change to the generator that moves a byte of the default estate fails here."""
    estate = generate_estate(100, columns=20)
    digest = hashlib.sha256()
    for key in sorted(estate.objects):
        data = estate.objects[key]
        if key.endswith("/odcs-bundle.json"):
            bundle = json.loads(data)
            del bundle["producer"]
            data = json.dumps(bundle, sort_keys=True).encode()
        digest.update(key.encode() + b"\0" + data + b"\0")
    assert digest.hexdigest() == "08cb9290e5f7a3d2342642c0d8be8ac8201977ca79401b4ed982e5dd54de3a66"


def test_the_estate_writes_the_python_project_as_one_odcs_bundle(tmp_path):
    """The Python project is one `odcs-bundle.json` of kind python with one
    ODCS contract per node, each valid under the library the bundle names, and
    its reads are the ones the estate counts."""
    estate = generate_estate(20, columns=4)
    (request,) = [request for request in estate.requests if request.kind is ManifestKind.PYTHON]
    assert request.key == f"svc-py/{estate.release_id}/odcs-bundle.json"
    path = tmp_path / "odcs-bundle.json"
    path.write_bytes(estate.objects[request.key])
    bundle = json.loads(path.read_bytes())
    assert (bundle["bundle_version"], bundle["project"]) == (1, "svc-py")
    assert bundle["producer"] == {"continuo_python_runtime": "0.15.0", "continuo_contracts": "0.5.0"}
    assert len(bundle["nodes"]) == estate.python_nodes

    timings = ReleaseTimings(estate.release_id, FakeClock(), ScriptedMemoryProbe(0))
    parsed = parse_odcs_bundle(
        ManifestFile(path=str(path), image_tag="bench", declared_project="svc-py", kind=ManifestKind.PYTHON),
        progress=lambda: None, timings=timings, validator=ContractValidator(),
    )
    assert not parsed.failed
    assert len(parsed.nodes) == estate.python_nodes
    contracts = [node.contract for node in parsed.nodes]
    assert timings.contract_failures == {}
    reads = sum(
        len(continuo_values(contract.document["customProperties"])["readQueries"]) for contract in contracts
    )
    assert reads == estate.python_reads


# The benchmark estate must stay parseable now that the parser reads its extras.
def test_every_estate_extras_object_passes_the_loader(tmp_path):
    estate = generate_estate(20, columns=4)
    loaded = 0
    for request in estate.requests:
        key = posixpath.join(posixpath.dirname(request.key), "compile-extras.json")
        if key not in estate.objects:
            continue
        path = tmp_path / request.project / "compile-extras.json"
        path.parent.mkdir()
        path.write_bytes(estate.objects[key])
        extras = load_compile_extras(str(path), request.project)
        assert extras.seeds and all(re.fullmatch(r"[0-9a-f]{64}", s.sha256) for s in extras.seeds.values())
        loaded += 1
    assert loaded == 10


def test_memory_s3_counts_puts_and_serves_downloads(tmp_path):
    store = MemoryS3({"svc/rel/manifest.json": b"{}"})
    store.put_object(Bucket="bench", Key="k", Body=b"abc")
    store.download_file("bench", "svc/rel/manifest.json", str(tmp_path / "m.json"))
    assert (store.puts, store.put_bytes, (tmp_path / "m.json").read_bytes()) == (1, 3, b"{}")


def test_memory_s3_serves_an_object_it_holds_and_refuses_a_key_it_does_not():
    store = MemoryS3({"k": b"abc"})
    assert store.get_object(Bucket="bench", Key="k")["Body"].read() == b"abc"
    with pytest.raises(ClientError) as missing:
        store.get_object(Bucket="bench", Key="absent")
    assert missing.value.response["Error"]["Code"] == "NoSuchKey"


def test_the_handler_writes_one_contract_bundle_and_keeps_it_on_the_next_pass():
    estate = generate_estate(20, columns=4)
    store = MemoryS3(estate.objects)
    key = contract_bundle_key("default", estate.release_id)
    for _ in range(2):
        handler, publisher = build_handler(
            estate, store, clock=FakeClock(), memory=ScriptedMemoryProbe(0), validator=ContractValidator())
        handler.handle(estate.release_id)
        assert publisher.ok, publisher.failure
    document = json.loads(gzip.decompress(store.get_object(Bucket="bench", Key=key)["Body"].read()))
    assert document["release_id"] == estate.release_id
    python = sorted(uid for uid, contract in document["contracts"].items() if contract["family"] == "python")
    assert python == [f"py.py_{position:05d}" for position in range(estate.python_nodes)]
    dbt_contracts = estate.models + estate.seeds
    assert document["summary"]["contracts"] == estate.python_nodes + dbt_contracts
    assert document["summary"]["by_family"] == {"dbt": dbt_contracts, "python": estate.python_nodes}
    assert document["provenance"] == {
        "svc-py": {"continuo_python_runtime": "0.15.0", "image_packages": {"pyarrow": "26.0.0"}},
    }


def test_the_warm_pass_writes_as_many_objects_as_the_cold_one(capsys):
    """Each pass parses a release of its own, so a write-once object of the cold pass
    (the contract bundle) is not read back in place of the warm pass's write."""
    args = argparse.Namespace(size=20, columns=4, put_latency_ms=0.0, dialect="postgres", rep=0, ontology=False)

    assert _run_child(args) == 0

    cold, warm = (json.loads(line) for line in capsys.readouterr().out.splitlines())
    assert (cold["pass"], warm["pass"]) == ("cold", "warm")
    assert warm["puts"] == cold["puts"]


def _read_contract_bundle(store, release_id):
    key = contract_bundle_key("default", release_id)
    return json.loads(gzip.decompress(store.get_object(Bucket="bench", Key=key)["Body"].read()))


def _dbt_manifests(estate):
    return [json.loads(estate.objects[request.key]) for request in estate.requests if request.kind is ManifestKind.DBT]


def test_an_estate_without_the_ontology_has_no_ontology_manifest_and_no_binding():
    """The default estate holds no manifest of kind ontology, no `continuo` key in a dbt node's
    `meta` and no binding on a python contract."""
    estate = generate_estate(20, columns=4)
    assert estate.objects == generate_estate(20, columns=4, ontology=False).objects
    assert ManifestKind.ONTOLOGY not in {request.kind for request in estate.requests}
    for manifest in _dbt_manifests(estate):
        for node in manifest["nodes"].values():
            assert "continuo" not in node["config"].get("meta", {})
            assert all("meta" not in column for column in node.get("columns", {}).values())
    (python,) = [request for request in estate.requests if request.kind is ManifestKind.PYTHON]
    assert "entity" not in estate.objects[python.key].decode()


def test_the_ontology_estate_adds_one_ontology_manifest_and_leaves_the_other_objects_alone():
    plain = generate_estate(20, columns=4)
    estate = generate_estate(20, columns=4, ontology=True)
    (request,) = [request for request in estate.requests if request.kind is ManifestKind.ONTOLOGY]
    assert (request.project, request.key) == ("bench-ontology", "bench-ontology/rel-bench-ontology/ontology.yaml")
    assert estate.objects.keys() == plain.objects.keys() | {request.key}
    assert [r for r in estate.requests if r.kind is not ManifestKind.ONTOLOGY] == plain.requests
    assert estate.image_tags == plain.image_tags
    assert estate.objects == generate_estate(20, columns=4, ontology=True).objects


def test_the_estate_ontology_declares_every_project_under_one_domain_and_one_thing():
    estate = generate_estate(20, columns=4, ontology=True)
    (request,) = [request for request in estate.requests if request.kind is ManifestKind.ONTOLOGY]
    loaded = ontology_file.load(estate.objects[request.key].decode())
    projects = sorted(r.project for r in estate.requests if r.kind is not ManifestKind.ONTOLOGY)
    assert loaded.projects() == tuple(projects)
    assert {loaded.domain_of(project) for project in projects} == {"bench"}
    assert list(loaded.document["principals"]["domains"]) == ["bench"]
    assert loaded.document["entities"]["BenchRow"]["key"] == {"id": "integer"}
    assert list(loaded.document["entities"]) == ["BenchRow"]
    assert ontology_findings(loaded) == []


def test_the_ontology_estate_binds_every_model_seed_and_python_node_to_the_thing():
    """Every model and seed binds `entity: BenchRow`, the column `c0` (a seed's `id`) to the key
    `BenchRow.id` and every other column as `local`; every python contract does the same."""
    estate = generate_estate(20, columns=4, ontology=True)
    bound = 0
    for manifest in _dbt_manifests(estate):
        for node in manifest["nodes"].values():
            if node["resource_type"] == "test":
                continue
            assert node["config"]["meta"]["continuo"] == {"entity": "BenchRow"}
            key = "id" if node["resource_type"] == "seed" else "c0"
            for name, column in node["columns"].items():
                expected = {"property": "BenchRow.id"} if name == key else {"local": True}
                assert column["meta"] == {"continuo": expected}, (node["unique_id"], name)
            bound += 1
    assert bound == estate.models + estate.seeds
    (python,) = [request for request in estate.requests if request.kind is ManifestKind.PYTHON]
    nodes = json.loads(estate.objects[python.key])["nodes"]
    assert len(nodes) == estate.python_nodes
    for node in nodes:
        contract = yaml.safe_load(node["text"])
        (schema_object,) = contract["schema"]
        assert {"vendor": "continuo", "property": "entity", "value": "BenchRow"} in schema_object["customProperties"]
        values = [continuo_values(column.get("customProperties", [])) for column in schema_object["properties"]]
        assert values[0] == {"property": "BenchRow.id"}
        assert all(value == {"local": True} for value in values[1:])


def test_the_real_handler_checks_the_bound_estate_and_finds_no_error():
    estate = generate_estate(20, columns=4, ontology=True)
    store = MemoryS3(estate.objects)
    handler, publisher = build_handler(
        estate, store, clock=FakeClock(), memory=ScriptedMemoryProbe(0), validator=ContractValidator(),
        ontology_enabled=True,
    )
    timings = handler.handle(estate.release_id)
    assert publisher.ok, publisher.failure
    ontology = _read_contract_bundle(store, estate.release_id)["ontology"]
    assert (ontology["enabled"], ontology["project"], ontology["release_id"]) == (True, "bench-ontology", "rel-bench-ontology")
    assert ontology["declared_projects"] == sorted(r.project for r in estate.requests if r.kind is not ManifestKind.ONTOLOGY)
    assert [f for f in ontology["findings"] if f["severity"] != "info"] == []
    assert timings.contract_failures == {}
    record = timings.stages[Stage.ONTOLOGY]
    assert record.runs == 2
    assert record.counts["contracts"] == estate.models + estate.seeds + estate.python_nodes


def test_the_ontology_estate_parses_with_the_switch_off_and_its_ontology_is_ignored():
    """With the switch off the release publishes, the contract bundle names no ontology and the
    ontology stage does not run."""
    estate = generate_estate(20, columns=4, ontology=True)
    store = MemoryS3(estate.objects)
    handler, publisher = build_handler(
        estate, store, clock=FakeClock(), memory=ScriptedMemoryProbe(0), validator=ContractValidator())
    timings = handler.handle(estate.release_id)
    assert publisher.ok, publisher.failure
    assert _read_contract_bundle(store, estate.release_id)["ontology"] is None
    assert timings.stages[Stage.ONTOLOGY].runs == 0


def test_the_ontology_flag_reaches_the_child_and_its_result_lines(capsys):
    parsed = _parser().parse_args(["--sizes", "20", "--out", "o.jsonl", "--ontology"])
    assert parsed.ontology is True
    assert _parser().parse_args(["--sizes", "20", "--out", "o.jsonl"]).ontology is False
    assert "--ontology" in _child_command(parsed, 20, 1)
    assert "--ontology" not in _child_command(_parser().parse_args(["--sizes", "20", "--out", "o.jsonl"]), 20, 1)

    args = argparse.Namespace(size=20, columns=4, put_latency_ms=0.0, dialect="postgres", rep=1, ontology=True)
    assert _run_child(args) == 0
    cold, warm = (json.loads(line) for line in capsys.readouterr().out.splitlines())
    for line in (cold, warm):
        assert line["ontology"] is True
        assert line["stages"]["ontology"]["runs"] == 2
        assert line["stages"]["ontology"]["counts"]["contracts"] > 0


def _line(rep, **stage_seconds):
    return {"size": 1000, "columns": 20, "put_latency_ms": 0.0, "pass": "cold", "rep": rep,
            "nodes": {"dbt": 3000, "python": 0}, "handler_seconds": sum(stage_seconds.values()),
            "stages": {name: {"seconds": s, "runs": 1, "counts": {}} for name, s in stage_seconds.items()}}


def test_compare_flags_a_stage_that_slowed_by_more_than_twenty_percent_per_node():
    before = [_line(rep, resolve=0.600, rewrite=1.000) for rep in (1, 2, 3)]
    after = [_line(rep, resolve=0.730, rewrite=1.190) for rep in (1, 2, 3)]
    rows = {row.stage: row for row in compare(before, after)}
    assert rows["resolve"].slower and not rows["rewrite"].slower
