import pytest

from domain.contract_vocabulary import NODE_TYPE_RUNTIME, NodeRuntime, NodeType
from domain.exceptions import MalformedContractError
from service.python_kind_rules import RULES, CsvRead, NoReads, SqlReads


def test_every_python_node_type_has_rules():
    python_types = {t for t, r in NODE_TYPE_RUNTIME.items() if r is NodeRuntime.PYTHON}
    assert set(RULES) == python_types


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


def test_a_script_is_required_of_every_kind_but_csv():
    assert {t for t, r in RULES.items() if not r.script_required} == {NodeType.PYTHON_CSV}


def test_python_api_reads_nothing():
    assert isinstance(RULES[NodeType.PYTHON_API].reads, NoReads)


def test_no_reads_accepts_empty_and_yields_no_dependencies():
    parsed = NoReads().parse({}, "L")
    assert parsed.reads == {} and parsed.dependency_sqls == [] and parsed.csv_source == ""


@pytest.mark.parametrize("raw", [{"x": "select 1"}, None, [], "select 1"])
def test_no_reads_rejects_anything_but_an_empty_mapping(raw):
    with pytest.raises(MalformedContractError, match="python-api node declares no reads"):
        NoReads().parse(raw, "L")


@pytest.mark.parametrize("uri", [
    "s3://bucket/path/to/orders.csv",
    "https://example.com",
    "https://example.com/orders.csv",
])
def test_csv_read_accepts_an_s3_object_or_an_https_host(uri):
    parsed = CsvRead().parse({"csv": uri}, "L")
    assert parsed.reads == {"csv": uri} and parsed.csv_source == uri


@pytest.mark.parametrize("uri, match", [
    ("s3://bucket", "bucket or key"),
    ("s3:///key", "bucket or key"),
    ("https://", "host"),
    ("http://example.com/orders.csv", "s3://bucket/key or https"),
    ("ftp://bad", "s3://bucket/key or https"),
])
def test_csv_read_refuses_what_the_runners_uri_parser_refuses(uri, match):
    with pytest.raises(MalformedContractError, match=match):
        CsvRead().parse({"csv": uri}, "L")


@pytest.mark.parametrize("raw", [{"csv": "s3://b/k", "other": "select 1"}, {}, {"uri": "s3://b/k"}, None])
def test_csv_read_takes_exactly_one_csv_key(raw):
    with pytest.raises(MalformedContractError, match="exactly"):
        CsvRead().parse(raw, "L")
