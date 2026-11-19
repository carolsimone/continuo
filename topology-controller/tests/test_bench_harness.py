import argparse
import gzip
import json
import posixpath
import re

import pytest
from botocore.exceptions import ClientError
from continuo_contracts.vocabulary import continuo_values

from bench.compare import compare
from bench.estate import generate_estate
from bench.memory_s3 import MemoryS3
from bench.run import _run_child, build_handler
from domain.model import ManifestFile, ManifestKind
from service.compile_extras import load_compile_extras
from service.contract_bundle import contract_bundle_key
from service.contract_validation import ContractValidator
from service.odcs_bundle_parser import parse_odcs_bundle
from service.release_timings import ReleaseTimings
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
    handler, publisher = build_handler(estate, MemoryS3(estate.objects), clock=FakeClock(), memory=ScriptedMemoryProbe(0))
    handler.handle(estate.release_id)
    assert publisher.ok, publisher.failure
    assert publisher.node_count == estate.dbt_nodes + estate.python_nodes


def test_the_estate_writes_the_python_service_as_one_odcs_bundle(tmp_path):
    """The Python service is one `odcs-bundle.json` of kind python-odcs with one
    ODCS contract per node, each valid under the library the bundle names, and
    its reads are the ones the estate counts."""
    estate = generate_estate(20, columns=4)
    (request,) = [request for request in estate.requests if request.kind is ManifestKind.PYTHON_ODCS]
    assert request.key == f"svc-py/{estate.release_id}/odcs-bundle.json"
    path = tmp_path / "odcs-bundle.json"
    path.write_bytes(estate.objects[request.key])
    bundle = json.loads(path.read_bytes())
    assert (bundle["bundle_version"], bundle["service"]) == (1, "svc-py")
    assert bundle["producer"]["continuo_contracts"] == "0.2.0"
    assert len(bundle["nodes"]) == estate.python_nodes

    timings = ReleaseTimings(estate.release_id, FakeClock(), ScriptedMemoryProbe(0))
    parsed = parse_odcs_bundle(
        ManifestFile(path=str(path), image_tag="bench", declared_service="svc-py", kind=ManifestKind.PYTHON_ODCS),
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
        path = tmp_path / request.service / "compile-extras.json"
        path.parent.mkdir()
        path.write_bytes(estate.objects[key])
        extras = load_compile_extras(str(path), request.service)
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
        handler, publisher = build_handler(estate, store, clock=FakeClock(), memory=ScriptedMemoryProbe(0))
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
        "svc-py": {"continuo_python_runtime": "0.13.0", "image_packages": {"pyarrow": "26.0.0"}},
    }


def test_the_warm_pass_writes_as_many_objects_as_the_cold_one(capsys):
    """Each pass parses a release of its own, so a write-once object of the cold pass
    (the contract bundle) is not read back in place of the warm pass's write."""
    args = argparse.Namespace(size=20, columns=4, put_latency_ms=0.0, dialect="postgres", rep=0)

    assert _run_child(args) == 0

    cold, warm = (json.loads(line) for line in capsys.readouterr().out.splitlines())
    assert (cold["pass"], warm["pass"]) == ("cold", "warm")
    assert warm["puts"] == cold["puts"]


def _line(rep, **stage_seconds):
    return {"size": 1000, "columns": 20, "put_latency_ms": 0.0, "pass": "cold", "rep": rep,
            "nodes": {"dbt": 3000, "python": 0}, "handler_seconds": sum(stage_seconds.values()),
            "stages": {name: {"seconds": s, "runs": 1, "counts": {}} for name, s in stage_seconds.items()}}


def test_compare_flags_a_stage_that_slowed_by_more_than_twenty_percent_per_node():
    before = [_line(rep, resolve=0.600, rewrite=1.000) for rep in (1, 2, 3)]
    after = [_line(rep, resolve=0.730, rewrite=1.190) for rep in (1, 2, 3)]
    rows = {row.stage: row for row in compare(before, after)}
    assert rows["resolve"].slower and not rows["rewrite"].slower
