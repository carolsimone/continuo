import pytest

from domain.contract_vocabulary import NODE_TYPE_RUNTIME, NODE_TYPES_WITH_SECRET_REF, NodeRuntime, NodeType
from domain.exceptions import MalformedContractError
from service.python_kind_rules import RULES, STORED_KIND_ALIASES, CsvRead, NoReads, SqlReads, resolve_kind


def test_every_python_node_type_has_rules():
    python_types = {t for t, r in NODE_TYPE_RUNTIME.items() if r is NodeRuntime.PYTHON}
    assert set(RULES) == python_types


def test_secret_allowed_kinds_match_the_contract_vocabulary():
    """The kinds whose contract may name a Secret are declared once, in
    pkg/streams/contract.yaml (secret_ref: true); execution-controller enforces
    the same generated set at Job creation."""
    assert {t for t, r in RULES.items() if r.secret_allowed} == NODE_TYPES_WITH_SECRET_REF


def test_aliases_point_at_declared_kinds_only():
    assert set(STORED_KIND_ALIASES.values()) <= set(RULES)
    assert not set(STORED_KIND_ALIASES) & {str(t) for t in NodeType}


def test_absent_kind_defaults_to_python_node():
    assert resolve_kind(None, "L") is NodeType.PYTHON_NODE


def test_stored_python_model_resolves_to_python_node():
    assert resolve_kind("python-model", "L") is NodeType.PYTHON_NODE


@pytest.mark.parametrize("raw", ["dbt-model", "spark", "", 7])
def test_non_python_or_unknown_kind_is_rejected(raw):
    with pytest.raises(MalformedContractError, match="kind must be one of"):
        resolve_kind(raw, "L")


def test_sql_reads_rejects_an_empty_mapping():
    with pytest.raises(MalformedContractError, match="reads must be a non-empty mapping"):
        SqlReads().parse({}, "L")


def test_sql_reads_orders_dependency_sqls_by_read_name():
    parsed = SqlReads().parse({"b": "select 2", "a": "select 1"}, "L")
    assert parsed.dependency_sqls == ["select 1", "select 2"]
    assert parsed.csv_source == ""


def test_csv_read_has_no_dependency_sqls_and_carries_the_uri():
    parsed = CsvRead().parse({"csv": "s3://b/k.csv"}, "L")
    assert parsed.dependency_sqls == []
    assert parsed.csv_source == "s3://b/k.csv"


def test_python_api_rules():
    rules = RULES[NodeType.PYTHON_API]
    assert rules.script_required is True
    assert isinstance(rules.reads, NoReads)
    assert rules.secret_allowed is True


def test_only_python_api_may_name_a_secret():
    assert {t for t, r in RULES.items() if r.secret_allowed} == {NodeType.PYTHON_API}


def test_no_reads_accepts_empty_and_yields_no_dependencies():
    parsed = NoReads().parse({}, "L")
    assert parsed.reads == {} and parsed.dependency_sqls == [] and parsed.csv_source == ""


@pytest.mark.parametrize("raw", [{"x": "select 1"}, None, [], "select 1"])
def test_no_reads_rejects_anything_but_an_empty_mapping(raw):
    with pytest.raises(MalformedContractError, match="python-api node declares no reads"):
        NoReads().parse(raw, "L")
