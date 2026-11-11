import sqlglot

from bench.estate import generate_estate
from bench.memory_s3 import MemoryS3
from bench.run import build_handler
from domain.exceptions import InvalidCompiledSqlError
from service import candidate_manifest_handler
from tests.fakes import FakeClock, ScriptedMemoryProbe


def _run(estate, store, **kwargs):
    handler, publisher = build_handler(estate, store, clock=kwargs.pop("clock", FakeClock()),
                                       memory=ScriptedMemoryProbe(0), **kwargs)
    handler.handle(estate.release_id)
    assert publisher.ok, publisher.failure


def test_each_sql_is_parsed_twice_once_to_resolve_and_once_to_rewrite(monkeypatch):
    estate = generate_estate(20, columns=4)
    parses = []
    real = sqlglot.parse_one
    monkeypatch.setattr(sqlglot, "parse_one", lambda sql, *a, **kw: parses.append(sql) or real(sql, *a, **kw))
    _run(estate, MemoryS3(estate.objects))
    assert len(parses) == 2 * (estate.dbt_nodes_with_sql + estate.python_reads)


def test_a_release_puts_one_object_per_node_with_sql_or_spec_plus_the_two_bundles():
    estate = generate_estate(20, columns=4)
    store = MemoryS3(estate.objects)
    _run(estate, store)
    assert store.puts == estate.dbt_nodes_with_sql + estate.python_nodes + 2


def test_progress_is_reported_after_every_manifest_and_every_node_of_every_per_node_stage(monkeypatch):
    estate = generate_estate(20, columns=4, python_share=0.0)
    clock = FakeClock()
    real_resolve = candidate_manifest_handler.resolve_upstream_deps

    def slow_resolve(node, registry, *, dialect):
        clock.t += 1.0
        return real_resolve(node, registry, dialect=dialect)

    class _SlowStore(MemoryS3):
        def put_object(self, **kwargs):
            clock.t += 1.0
            return super().put_object(**kwargs)

    monkeypatch.setattr(candidate_manifest_handler, "resolve_upstream_deps", slow_resolve)
    calls = []
    _run(estate, _SlowStore(estate.objects), clock=clock, progress=lambda: calls.append(clock.t))
    assert len(calls) == len(estate.requests) + 2 * estate.dbt_nodes
    assert max(b - a for a, b in zip(calls, calls[1:])) <= 1.0


def test_progress_is_reported_after_a_node_that_fails_to_resolve(monkeypatch):
    estate = generate_estate(20, columns=4, python_share=0.0)

    def refuse(node, registry, *, dialect):
        raise InvalidCompiledSqlError(node.table_name, "refused")

    monkeypatch.setattr(candidate_manifest_handler, "resolve_upstream_deps", refuse)
    calls = []
    handler, publisher = build_handler(
        estate, MemoryS3(estate.objects), clock=FakeClock(), memory=ScriptedMemoryProbe(0),
        progress=lambda: calls.append(1),
    )
    handler.handle(estate.release_id)

    assert not publisher.ok
    assert len(calls) == len(estate.requests) + estate.dbt_nodes


def test_progress_is_reported_after_a_node_whose_build_fails():
    estate = generate_estate(20, columns=4, python_share=0.0)
    calls = []
    calls_when_the_put_failed = []

    class _FailingStore(MemoryS3):
        def put_object(self, **kwargs):
            calls_when_the_put_failed.append(len(calls))
            raise RuntimeError("the store refused the put")

    handler, publisher = build_handler(
        estate, _FailingStore(estate.objects), clock=FakeClock(), memory=ScriptedMemoryProbe(0),
        progress=lambda: calls.append(1),
    )
    handler.handle(estate.release_id)

    assert not publisher.ok
    assert len(calls_when_the_put_failed) == 1
    assert len(calls) == calls_when_the_put_failed[0] + 1
