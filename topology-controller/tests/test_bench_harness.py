import posixpath
import re

from bench.compare import compare
from bench.estate import generate_estate
from bench.memory_s3 import MemoryS3
from bench.run import build_handler
from domain.model import ManifestKind
from service.compile_extras import load_compile_extras
from service.python_contract_parser import parse_python_contract
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


def test_the_estate_python_nodes_carry_the_hashes_the_parser_computes(tmp_path, caplog):
    """Each python entry's CI hashes are the ones a CI on runtime 0.12.0 writes,
    so parsing the contract logs no content_hash difference per node."""
    estate = generate_estate(20, columns=4)
    (key,) = [request.key for request in estate.requests if request.kind is ManifestKind.PYTHON]
    path = tmp_path / "contract.yaml"
    path.write_bytes(estate.objects[key])
    nodes = parse_python_contract(str(path)).nodes
    assert len(nodes) == estate.python_nodes
    assert not [r for r in caplog.records if hasattr(r, "ci_content_hash")]


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


def _line(rep, **stage_seconds):
    return {"size": 1000, "columns": 20, "put_latency_ms": 0.0, "pass": "cold", "rep": rep,
            "nodes": {"dbt": 3000, "python": 0}, "handler_seconds": sum(stage_seconds.values()),
            "stages": {name: {"seconds": s, "runs": 1, "counts": {}} for name, s in stage_seconds.items()}}


def test_compare_flags_a_stage_that_slowed_by_more_than_twenty_percent_per_node():
    before = [_line(rep, resolve=0.600, rewrite=1.000) for rep in (1, 2, 3)]
    after = [_line(rep, resolve=0.730, rewrite=1.190) for rep in (1, 2, 3)]
    rows = {row.stage: row for row in compare(before, after)}
    assert rows["resolve"].slower and not rows["rewrite"].slower
