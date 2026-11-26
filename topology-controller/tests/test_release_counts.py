import json

import sqlglot
import yaml

from bench.estate import generate_estate
from bench.memory_s3 import MemoryS3
from bench.run import build_handler
from domain.exceptions import InvalidCompiledSqlError
from domain.model import ManifestKind
from service import candidate_manifest_handler, contract_bundle, contract_validation
from service.contract_validation import ContractValidator
from tests.fakes import FakeClock, ScriptedMemoryProbe


def _dbt_contracts(estate) -> int:
    """The dbt nodes of the estate that get a contract: its models and seeds (a test has none)."""
    return estate.models + estate.seeds


def _run(estate, store, **kwargs):
    kwargs.setdefault("validator", ContractValidator())
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


def test_a_release_puts_one_object_per_node_with_sql_or_spec_plus_the_three_release_documents():
    """The documents are the code bundle, the contract bundle and the topology artifact."""
    estate = generate_estate(20, columns=4)
    store = MemoryS3(estate.objects)
    _run(estate, store)
    assert store.puts == estate.dbt_nodes_with_sql + estate.python_nodes + 3


def test_progress_is_reported_after_every_manifest_and_every_node_of_every_per_node_stage(monkeypatch):
    estate = generate_estate(20, columns=4, python_share=0.0)
    clock = FakeClock()
    real_referenced_tables = candidate_manifest_handler.referenced_tables

    def slow_resolve(node, *, dialect, **kwargs):
        clock.t += 1.0
        return real_referenced_tables(node, dialect=dialect, **kwargs)

    class _SlowStore(MemoryS3):
        def put_object(self, **kwargs):
            clock.t += 1.0
            return super().put_object(**kwargs)

    monkeypatch.setattr(candidate_manifest_handler, "referenced_tables", slow_resolve)
    calls = []
    _run(estate, _SlowStore(estate.objects), clock=clock, progress=lambda: calls.append(clock.t))
    # One call after each manifest and one per node in the resolve and the build, and
    # one per dbt contract in the parse and in the stored form.
    assert len(calls) == len(estate.requests) + 2 * estate.dbt_nodes + 2 * _dbt_contracts(estate)
    assert max(b - a for a, b in zip(calls, calls[1:])) <= 1.0


def test_progress_is_reported_after_a_node_that_fails_to_resolve(monkeypatch):
    estate = generate_estate(20, columns=4, python_share=0.0)

    def refuse(node, *, dialect, **kwargs):
        raise InvalidCompiledSqlError(node.table_name, "refused")

    monkeypatch.setattr(candidate_manifest_handler, "referenced_tables", refuse)
    calls = []
    handler, publisher = build_handler(
        estate, MemoryS3(estate.objects), clock=FakeClock(), memory=ScriptedMemoryProbe(0),
        validator=ContractValidator(), progress=lambda: calls.append(1),
    )
    handler.handle(estate.release_id)

    assert not publisher.ok
    assert len(calls) == len(estate.requests) + estate.dbt_nodes + _dbt_contracts(estate)


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
        validator=ContractValidator(), progress=lambda: calls.append(1),
    )
    handler.handle(estate.release_id)

    assert not publisher.ok
    assert len(calls_when_the_put_failed) == 1
    assert len(calls) == calls_when_the_put_failed[0] + 1


def _counting_schema_checks(monkeypatch) -> list[int]:
    """Return a list that gets one entry per schema check that really runs."""
    schema_checks: list[int] = []
    real_schema_issues = contract_validation.schema_issues
    monkeypatch.setattr(
        contract_validation, "schema_issues", lambda doc: schema_checks.append(1) or real_schema_issues(doc),
    )
    return schema_checks


def _with_one_contract_changed(estate, position):
    """The estate's objects with the description of one Python contract rewritten."""
    (key,) = [request.key for request in estate.requests if request.kind is ManifestKind.PYTHON]
    bundle = json.loads(estate.objects[key])
    entry = bundle["nodes"][position]
    contract = yaml.safe_load(entry["text"])
    contract["description"]["purpose"] += " (changed)"
    entry["text"] = yaml.safe_dump(contract, sort_keys=False)
    return {**estate.objects, key: json.dumps(bundle, sort_keys=True).encode()}


def test_a_warm_pass_validates_only_the_changed_contracts(monkeypatch):
    """The dbt and Python contracts share one validator, so a warm pass checks none of
    either again, and a changed Python contract is the only check of the next pass."""
    estate = generate_estate(100)
    assert estate.python_nodes == 10
    dbt = _dbt_contracts(estate)
    schema_checks = _counting_schema_checks(monkeypatch)
    validator = ContractValidator()

    def parse(objects):
        schema_checks.clear()
        handler, publisher = build_handler(
            estate, MemoryS3(objects), clock=FakeClock(), memory=ScriptedMemoryProbe(0), validator=validator,
        )
        timings = handler.handle(estate.release_id)
        assert publisher.ok, publisher.failure
        return len(schema_checks), dict(timings.validation_cache)

    assert parse(estate.objects) == (10 + dbt, {"hit": 0, "miss": 10 + dbt})
    assert parse(estate.objects) == (0, {"hit": 10 + dbt, "miss": 0})
    assert parse(_with_one_contract_changed(estate, position=4)) == (1, {"hit": 9 + dbt, "miss": 1})


def test_progress_runs_at_least_once_per_python_node_in_every_per_node_stage(monkeypatch):
    """A Python node is progress in the contract validation of the parse, in the
    resolve, in the stored form and in the build, so the caller's staleness
    budget holds on an estate of Python nodes as it does on one of dbt nodes."""
    estate = generate_estate(20, columns=4)
    clock = FakeClock()
    real_check = ContractValidator.check
    real_referenced_tables = candidate_manifest_handler.referenced_tables
    real_normalise = contract_bundle.normalise_python

    def slow_check(self, *args, **kwargs):
        clock.t += 1.0
        return real_check(self, *args, **kwargs)

    def slow_resolve(node, *, dialect, **kwargs):
        clock.t += 1.0
        return real_referenced_tables(node, dialect=dialect, **kwargs)

    def slow_normalise(*args, **kwargs):
        clock.t += 1.0
        return real_normalise(*args, **kwargs)

    class _SlowStore(MemoryS3):
        def put_object(self, **kwargs):
            clock.t += 1.0
            return super().put_object(**kwargs)

    monkeypatch.setattr(ContractValidator, "check", slow_check)
    monkeypatch.setattr(candidate_manifest_handler, "referenced_tables", slow_resolve)
    monkeypatch.setattr(contract_bundle, "normalise_python", slow_normalise)
    calls = []
    _run(estate, _SlowStore(estate.objects), clock=clock, progress=lambda: calls.append(clock.t))

    # One call after each manifest and one per node in the resolve and the build,
    # one per Python node in the contract validation and in the stored form, and
    # one per dbt contract in the parse and in the stored form.
    assert len(calls) == (
        len(estate.requests) + 2 * estate.dbt_nodes + 4 * estate.python_nodes + 2 * _dbt_contracts(estate))
    assert max(b - a for a, b in zip(calls, calls[1:])) <= 1.0
