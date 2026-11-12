import datetime
import hashlib
import importlib.resources
import json
from pathlib import Path

import pytest
import yaml
from continuo_contracts.hashing import fold3
from continuo_contracts.python import PythonColumn, PythonNode, code_projection, projection_hash

from domain.exceptions import MalformedContractError
from domain.model import NodeType, Runtime
from service.python_contract_parser import parse_python_contract


def _fold_of(entry):
    """The fold of the entry's three parts. A part a test replaced with a value that
    is not a string (which the parser refuses) is folded as its repr, so the
    entry still carries a content_hash."""
    parts = (entry.get(key, "") for key in ("source_hash", "shared_code_hash", "config_hash"))
    return fold3(*(part if isinstance(part, str) else repr(part) for part in parts))


def make_entry(remove=(), **overrides):
    """A valid wire entry; overrides mutate it, remove drops keys entirely.

    The four hash fields stand for what a producer CI writes. config_hash is a
    placeholder and content_hash the fold of the three CI parts: the parser
    keeps the CI source_hash and shared_code_hash, computes the config part
    itself from the entry's code projection, and only logs a CI content_hash
    that differs from the fold it computes.
    """
    entry = {
        "schema": "analytics",
        "table": "table_test",
        "owner": "marketing",
        "schedule": "daily",
        "criticality": "SECONDARY",
        "script": "scripts/table_test.py",
        "reads": {
            "joined": "select a from analytics.table_a",
            "ids": "select id from analytics.table_b",
        },
        "output_columns": [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "amount", "type": "NUMERIC(10,2)"},
        ],
        "source_hash": "aaa111",
        "shared_code_hash": "bbb222",
        "config_hash": "ccc333",
    }
    entry.update(overrides)
    entry.setdefault("content_hash", _fold_of(entry))
    for key in remove:
        entry.pop(key, None)
    return entry


def make_csv_entry(remove=(), **overrides):
    """A valid python-csv wire entry; overrides mutate it, remove drops keys.

    A csv node has no script and its sole read is {"csv": <uri>}. The
    "script": "" key mirrors the wire-exact shape: the runtime's merge tool
    (merge.py node_entry()) emits "script": node.script unconditionally, so
    every real csv entry topology-controller receives carries an empty-string
    script, never an absent one. Its CI hash parts mirror what the runtime's
    merge computes for a csv node: source_hash over the uri bytes and
    shared_code_hash empty (no in-repo imports). config_hash is a placeholder:
    the parser computes the config part from the entry's code projection and
    folds it with the two CI parts, so the fixture does not need to reproduce
    the runtime's hash.
    """
    uri = "s3://drops/orders.csv"
    entry = {
        "schema": "analytics",
        "table": "orders_csv",
        "owner": "team",
        "schedule": "daily",
        "criticality": "SECONDARY",
        "kind": "python-csv",
        "script": "",
        "reads": {"csv": uri},
        "output_columns": [{"name": "order_id", "type": "INTEGER", "nullable": False}],
        "source_hash": hashlib.sha256(uri.encode()).hexdigest(),
        "shared_code_hash": "",
        "config_hash": "csvcfg000",
    }
    entry.update(overrides)
    entry.setdefault("content_hash", _fold_of(entry))
    for key in remove:
        entry.pop(key, None)
    return entry


def write_contract(tmp_path, *entries, **doc_overrides):
    doc = {"contract_version": 1, "service": "marketing-py", "nodes": list(entries)}
    doc.update(doc_overrides)
    path = tmp_path / "contract.yaml"
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return str(path)


WIRE_FIXTURE = Path(__file__).parent / "fixtures" / "wire_contract_write_modes.yml"
# Placeholders for the four hash fields an entry needs and a vector or the shared fixture leaves out.
# topology-controller computes the config part and only logs a CI content_hash that differs.
HASH_FIELDS = {"source_hash": "aaa111", "shared_code_hash": "", "config_hash": "ccc333", "content_hash": "sha256:" + "0" * 64}
# The keys topology-controller's wire parser requires that a projection vector's node does not carry.
VECTOR_CONTRACT_KEYS = {"owner": "vectors", "schedule": "daily", "criticality": "SECONDARY"}


def _projection(**over):
    base = dict(schema="analytics", table="table_test", kind="python-node", script="scripts/table_test.py",
                reads={"joined": "select a from analytics.table_a", "ids": "select id from analytics.table_b"},
                output_columns=[PythonColumn("id", "INTEGER", False), PythonColumn("amount", "NUMERIC(10,2)", True)],
                extra_columns="raise", config={}, secret_ref="", mode_type="replace", primary_key=[], assets=[])
    return PythonNode(**{**base, **over})


def test_happy_path_maps_every_manifest_node_field(tmp_path):
    entry = make_entry(
        config={"indexes": [{"columns": ["id"], "unique": True}]},
        description="test data",
        extra_columns="warn",
    )
    parsed = parse_python_contract(write_contract(tmp_path, entry), "img:1")

    assert parsed.shared_code == {}
    (node,) = parsed.nodes
    assert node.table_name == "table_test"
    assert node.schema_name == "analytics"
    # A python contract has no alias concept: the resolved relation is always
    # the declared table.
    assert node.resolved_relation == "table_test"
    assert node.resolved_relation_id == "analytics.table_test"
    assert node.service_name == "marketing-py"
    assert node.owner == "marketing"
    assert node.schedule_name == "daily"
    assert node.criticality == "SECONDARY"
    assert node.node_type == "python-node"
    assert node.runtime == "python"
    assert node.candidate_sql == ""
    # dependency_sqls follow SORTED read names ("ids" < "joined") for determinism
    assert node.dependency_sqls == [
        "select id from analytics.table_b",
        "select a from analytics.table_a",
    ]
    assert node.output_columns == [
        {"name": "id", "type": "INTEGER", "nullable": False},
        {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True},
    ]
    assert node.config == {"indexes": [{"columns": ["id"], "unique": True}]}
    assert node.original_file_path == "scripts/table_test.py"
    assert node.image_tag == "img:1"
    assert node.test_count == 0
    assert node.code_unit_ids == []
    assert node.upstream_deps == []
    assert node.source_hash == "aaa111"
    assert node.shared_code_hash == "bbb222"
    expected = projection_hash(_projection(
        config={"indexes": [{"columns": ["id"], "unique": True}]}, extra_columns="warn",
    ))
    assert node.config_hash == expected
    assert node.content_hash == fold3("aaa111", "bbb222", expected)


def test_raw_code_is_deterministic_pretty_json_without_hash_fields(tmp_path):
    nodes = parse_python_contract(write_contract(tmp_path, make_entry())).nodes
    raw = json.loads(nodes[0].raw_code)
    assert raw["reads"] == make_entry()["reads"]
    assert raw["output_columns"] == [
        {"name": "id", "type": "INTEGER", "nullable": False},
        {"name": "amount", "type": "NUMERIC(10,2)", "nullable": True},
    ]
    assert raw["config"] == {}
    assert raw["description"] == ""
    assert raw["extra_columns"] == "raise"
    assert not {"source_hash", "shared_code_hash", "config_hash", "content_hash"} & set(raw)
    assert nodes[0].raw_code == json.dumps(raw, sort_keys=True, indent=2)


def test_empty_reads_are_rejected(tmp_path):
    with pytest.raises(MalformedContractError, match="reads must be a non-empty mapping"):
        parse_python_contract(write_contract(tmp_path, make_entry(reads={})))


def test_stored_python_model_contract_parses_as_python_node(tmp_path):
    nodes = parse_python_contract(write_contract(tmp_path, make_entry(kind="python-model"))).nodes
    assert nodes[0].node_type == NodeType.PYTHON_NODE
    assert json.loads(nodes[0].raw_code)["kind"] == "python-node"


def test_empty_nodes_list_yields_no_nodes(tmp_path):
    parsed = parse_python_contract(write_contract(tmp_path))
    assert parsed.nodes == []
    assert parsed.shared_code == {}


def test_image_tag_defaults_to_empty(tmp_path):
    nodes = parse_python_contract(write_contract(tmp_path, make_entry())).nodes
    assert nodes[0].image_tag == ""


@pytest.mark.parametrize(
    "mutation, match",
    [
        ({"owner": ""}, "owner"),
        ({"schedule": "   "}, "schedule"),
        ({"criticality": "HIGH"}, "criticality"),
        ({"extra_columns": "ignore"}, "extra_columns"),
        ({"description": 7}, "description"),
        ({"reads": {"joined": "  "}}, "reads"),
        ({"reads": ["select 1"]}, "reads"),
        ({"output_columns": []}, "output_columns"),
        ({"output_columns": [{"name": "id"}]}, "missing"),
        ({"output_columns": [{"name": "id", "type": "INT", "nullable": "yes"}]}, "nullable"),
        ({"output_columns": [{"name": "id", "type": "INT", "sortkey": True}]}, "unknown"),
        ({"surprise_field": 1}, "unknown fields"),
        ({"config": "btree"}, "config"),
        ({"source_hash": ""}, "source_hash"),
        ({"shared_code_hash": None}, "shared_code_hash"),
    ],
)
def test_malformed_entries_fail_the_whole_artifact(tmp_path, mutation, match):
    entry = make_entry(**mutation)
    with pytest.raises(MalformedContractError, match=match):
        parse_python_contract(write_contract(tmp_path, entry))


@pytest.mark.parametrize(
    "field",
    [
        "schema", "table", "owner", "schedule", "criticality", "script",
        "reads", "output_columns",
        "source_hash", "shared_code_hash", "config_hash", "content_hash",
    ],
)
def test_missing_required_field_fails(tmp_path, field):
    entry = make_entry(remove=(field,))
    with pytest.raises(MalformedContractError, match="missing"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_wrong_contract_version_fails(tmp_path):
    path = write_contract(tmp_path, make_entry(), contract_version=2)
    with pytest.raises(MalformedContractError, match="contract_version"):
        parse_python_contract(path)


def test_unknown_top_level_field_fails(tmp_path):
    path = write_contract(tmp_path, make_entry(), compiled_at="2026-08-07")
    with pytest.raises(MalformedContractError, match="unknown top-level"):
        parse_python_contract(path)


def test_empty_service_fails(tmp_path):
    path = write_contract(tmp_path, make_entry(), service="")
    with pytest.raises(MalformedContractError, match="service"):
        parse_python_contract(path)


def test_nodes_not_a_list_fails(tmp_path):
    path = write_contract(tmp_path, nodes="nope")
    with pytest.raises(MalformedContractError, match="nodes"):
        parse_python_contract(path)


def test_non_mapping_document_fails(tmp_path):
    path = tmp_path / "contract.yaml"
    path.write_text("- just\n- a\n- list\n")
    with pytest.raises(MalformedContractError, match="mapping"):
        parse_python_contract(str(path))


def test_duplicate_relation_fails(tmp_path):
    first = make_entry()
    second = make_entry(owner="finance")
    with pytest.raises(MalformedContractError, match="duplicate"):
        parse_python_contract(write_contract(tmp_path, first, second))


def test_invalid_yaml_fails_as_malformed_contract(tmp_path):
    path = tmp_path / "contract.yaml"
    path.write_text("nodes: [unclosed")
    with pytest.raises(MalformedContractError, match="yaml"):
        parse_python_contract(str(path))


def test_non_serializable_config_value_fails(tmp_path):
    entry = make_entry(config={"refresh_after": datetime.date(2026, 8, 7)})
    with pytest.raises(MalformedContractError, match="unsupported type date"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_a_config_nested_deeper_than_the_projection_allows_rejects_the_artifact(tmp_path):
    nested: list = []
    for _ in range(100):
        nested = [nested]
    with pytest.raises(MalformedContractError):
        parse_python_contract(write_contract(tmp_path, make_entry(config={"deep": nested})))


def test_boolean_contract_version_fails(tmp_path):
    path = write_contract(tmp_path, make_entry(), contract_version=True)
    with pytest.raises(MalformedContractError, match="contract_version"):
        parse_python_contract(path)


def test_future_version_with_new_top_level_field_reports_the_version(tmp_path):
    path = write_contract(tmp_path, make_entry(), contract_version=2, compiled_at="x")
    with pytest.raises(MalformedContractError, match="contract_version"):
        parse_python_contract(path)


def test_non_mapping_node_entry_fails(tmp_path):
    path = write_contract(tmp_path, "oops")
    with pytest.raises(MalformedContractError, match="mapping"):
        parse_python_contract(path)


def test_non_mapping_column_fails(tmp_path):
    entry = make_entry(output_columns=["id"])
    with pytest.raises(MalformedContractError, match="mapping"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_duplicate_column_name_fails(tmp_path):
    entry = make_entry(output_columns=[
        {"name": "id", "type": "INTEGER"},
        {"name": "ID", "type": "TEXT"},
    ])
    with pytest.raises(MalformedContractError, match="duplicate column"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_composite_criticality_fails_as_contract_error(tmp_path):
    entry = make_entry(criticality=[])
    with pytest.raises(MalformedContractError, match="criticality"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_composite_extra_columns_fails_as_contract_error(tmp_path):
    entry = make_entry(extra_columns={})
    with pytest.raises(MalformedContractError, match="extra_columns"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_mixed_type_unknown_entry_keys_fail_as_contract_error(tmp_path):
    entry = make_entry()
    entry[1] = "x"
    entry["surprise"] = 2
    with pytest.raises(MalformedContractError, match="unknown fields"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_csv_entry_parses(tmp_path):
    nodes = parse_python_contract(write_contract(tmp_path, make_csv_entry())).nodes
    (node,) = nodes
    assert node.node_type == NodeType.PYTHON_CSV
    assert node.runtime == Runtime.PYTHON
    assert node.dependency_sqls == []
    assert node.csv_source == "s3://drops/orders.csv"
    assert node.original_file_path == ""


def test_csv_entry_hash_fields_verify(tmp_path):
    uri = "s3://drops/orders.csv"
    entry = make_csv_entry()
    nodes = parse_python_contract(write_contract(tmp_path, entry)).nodes
    (node,) = nodes
    assert node.source_hash == hashlib.sha256(uri.encode()).hexdigest()
    assert node.shared_code_hash == ""
    expected = projection_hash(PythonNode(
        schema="analytics", table="orders_csv", kind="python-csv", reads={"csv": uri},
        output_columns=[PythonColumn("order_id", "INTEGER", False)],
    ))
    assert node.config_hash == expected
    assert node.content_hash == fold3(node.source_hash, node.shared_code_hash, expected)


def test_csv_entry_with_script_rejected(tmp_path):
    entry = make_csv_entry(script="scripts/x.py")
    with pytest.raises(MalformedContractError, match="script"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_csv_entry_reads_must_be_csv_only(tmp_path):
    entry = make_csv_entry(reads={"csv": "s3://b/k", "o": "select 1"})
    with pytest.raises(MalformedContractError, match="exactly"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_csv_entry_reads_value_must_be_a_valid_uri(tmp_path):
    entry = make_csv_entry(reads={"csv": "ftp://bad"})
    with pytest.raises(MalformedContractError, match="csv"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_csv_uri_http_scheme_rejected(tmp_path):
    """http:// (not https://) is rejected — the pinned runner's
    parse_csv_uri only ever recognizes s3:// and https://."""
    entry = make_csv_entry(reads={"csv": "http://example.com/orders.csv"})
    with pytest.raises(MalformedContractError, match="csv"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_csv_uri_s3_without_key_rejected(tmp_path):
    """s3://bucket with no object key: the runtime's parse_csv_uri does
    ``uri[5:].partition('/')`` and rejects an empty key the same way it
    rejects an empty bucket, so this must fail here too rather than only
    surfacing once the validation Job actually runs."""
    entry = make_csv_entry(reads={"csv": "s3://bucket"})
    with pytest.raises(MalformedContractError, match="bucket or key"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_csv_uri_s3_without_bucket_rejected(tmp_path):
    """s3:///key (empty bucket, non-empty key) is the other half of the same
    partition — bucket is empty even though a key is present."""
    entry = make_csv_entry(reads={"csv": "s3:///key"})
    with pytest.raises(MalformedContractError, match="bucket or key"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_csv_uri_https_without_host_rejected(tmp_path):
    """https:// with nothing after the scheme: the runtime's parse_csv_uri
    partitions on the first "/" and rejects an empty host."""
    entry = make_csv_entry(reads={"csv": "https://"})
    with pytest.raises(MalformedContractError, match="host"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_csv_uri_https_with_host_and_no_path_accepted(tmp_path):
    """https://<host> with no trailing path is a valid uri — only an empty
    host is rejected, not an absent path."""
    entry = make_csv_entry(reads={"csv": "https://example.com"})
    nodes = parse_python_contract(write_contract(tmp_path, entry)).nodes
    (node,) = nodes
    assert node.csv_source == "https://example.com"


def test_csv_uri_s3_with_bucket_and_key_accepted(tmp_path):
    """The valid shape this whole grammar exists to keep accepting."""
    entry = make_csv_entry(reads={"csv": "s3://bucket/path/to/orders.csv"})
    nodes = parse_python_contract(write_contract(tmp_path, entry)).nodes
    (node,) = nodes
    assert node.csv_source == "s3://bucket/path/to/orders.csv"


def test_unknown_kind_rejected(tmp_path):
    entry = make_csv_entry(kind="python-parquet")
    with pytest.raises(MalformedContractError, match="kind"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_non_string_kind_rejected_cleanly(tmp_path):
    entry = make_csv_entry(kind=["python-csv"])
    with pytest.raises(MalformedContractError, match="kind"):
        parse_python_contract(write_contract(tmp_path, entry))


def test_entry_without_kind_defaults_to_python_node(tmp_path):
    nodes = parse_python_contract(write_contract(tmp_path, make_entry())).nodes
    (node,) = nodes
    assert node.node_type == NodeType.PYTHON_NODE
    assert node.csv_source == ""


def test_stored_python_model_kind_parses_and_hash_verifies(tmp_path):
    """A contract written by continuo-python-runtime < 0.6.0 declares this
    kind as "python-model"; STORED_KIND_ALIASES resolves it to python-node so
    the stored contract keeps parsing."""
    entry = make_entry(kind="python-model")
    nodes = parse_python_contract(write_contract(tmp_path, entry)).nodes
    (node,) = nodes
    assert node.node_type == NodeType.PYTHON_NODE
    assert node.config_hash == projection_hash(_projection())
    assert node.content_hash == fold3("aaa111", "bbb222", projection_hash(_projection()))


def make_api_entry(remove=(), **overrides):
    return make_entry(remove=remove, **{"kind": "python-api", "reads": {}, **overrides})


def _parse_one(tmp_path, entry):
    return parse_python_contract(write_contract(tmp_path, entry), "img:1").nodes[0]


def test_python_api_entry_parses_as_a_root(tmp_path):
    node = _parse_one(tmp_path, make_api_entry())
    assert node.node_type is NodeType.PYTHON_API
    assert node.dependency_sqls == []
    assert node.secret_ref == ""
    assert json.loads(node.raw_code)["reads"] == {}
    assert "secret_ref" not in json.loads(node.raw_code)


def test_python_api_secret_ref_reaches_the_node_and_raw_code(tmp_path):
    node = _parse_one(tmp_path, make_api_entry(secret_ref="continuo-api-fx"))
    assert node.secret_ref == "continuo-api-fx"
    assert json.loads(node.raw_code)["secret_ref"] == "continuo-api-fx"


def test_secret_ref_on_python_node_rejects_the_artifact(tmp_path):
    with pytest.raises(MalformedContractError, match="secret_ref is not allowed for kind python-node"):
        _parse_one(tmp_path, make_entry(secret_ref="continuo-api-fx"))


def test_bad_secret_ref_rejects_the_artifact(tmp_path):
    with pytest.raises(MalformedContractError, match="secret_ref"):
        _parse_one(tmp_path, make_api_entry(secret_ref="continuo-app-credentials"))


def test_python_api_with_reads_rejects_the_artifact(tmp_path):
    with pytest.raises(MalformedContractError, match="declares no reads"):
        _parse_one(tmp_path, make_api_entry(reads={"x": "select 1 from analytics.a"}))


def test_python_timeout_minutes_reaches_the_node(tmp_path):
    node = _parse_one(tmp_path, make_entry(timeout_minutes=90))
    assert node.timeout_seconds == 5400


def test_an_entry_without_timeout_minutes_has_none(tmp_path):
    assert _parse_one(tmp_path, make_entry()).timeout_seconds is None


@pytest.mark.parametrize("value", [0, 10081, -5, "2h", "120", 1.5, True, []])
def test_bad_timeout_minutes_rejects_the_artifact(tmp_path, value):
    with pytest.raises(MalformedContractError) as excinfo:
        _parse_one(tmp_path, make_entry(timeout_minutes=value))
    assert "analytics.table_test" in str(excinfo.value)
    assert "timeout_minutes" in str(excinfo.value)


def test_a_null_timeout_minutes_is_undeclared(tmp_path):
    assert _parse_one(tmp_path, make_entry(timeout_minutes=None)).timeout_seconds is None


def test_timeout_minutes_is_not_in_raw_code(tmp_path):
    node = _parse_one(tmp_path, make_entry(timeout_minutes=90))
    assert "timeout_minutes" not in json.loads(node.raw_code)


def test_a_python_node_counts_its_output_columns(tmp_path):
    nodes = parse_python_contract(write_contract(tmp_path, make_entry())).nodes
    assert nodes[0].declared_columns == 2


def test_a_python_contract_skips_no_node(tmp_path):
    skipped = []
    parse_python_contract(write_contract(tmp_path, make_entry()), skipped=skipped)
    assert skipped == []


def test_the_content_hash_folds_ci_source_and_shared_with_the_projection_hash(tmp_path):
    node = _parse_one(tmp_path, make_entry())
    assert node.config_hash == projection_hash(_projection())
    assert node.content_hash == fold3("aaa111", "bbb222", projection_hash(_projection()))


def test_c19_owner_schedule_criticality_and_description_do_not_move_the_hash(tmp_path):
    a = _parse_one(tmp_path, make_entry()).content_hash
    b = _parse_one(tmp_path, make_entry(owner="other", schedule="hourly", criticality="CORE", description="x")).content_hash
    assert a == b


def test_c20_int_and_integer_and_the_python_model_alias_hash_alike(tmp_path):
    cols = [{"name": "id", "type": "INT", "nullable": False}, {"name": "amount", "type": "NUMERIC(10, 2)"}]
    assert _parse_one(tmp_path, make_entry(output_columns=cols, kind="python-model")).content_hash == _parse_one(tmp_path, make_entry()).content_hash


def test_c21_a_read_edited_after_ci_hashed_moves_the_hash(tmp_path):
    edited = make_entry(reads={"joined": "select a from analytics.other", "ids": "select id from analytics.table_b"})
    assert _parse_one(tmp_path, edited).content_hash != _parse_one(tmp_path, make_entry()).content_hash


def test_a_contract_from_runtime_0_11_parses_and_logs_the_hash_difference(tmp_path, caplog):
    node = _parse_one(tmp_path, make_entry(config_hash="ccc333", content_hash="sha256:" + "0" * 64))
    assert node.content_hash == fold3("aaa111", "bbb222", projection_hash(_projection()))
    assert any(r.levelname == "WARNING" and getattr(r, "ci_content_hash", None) == "sha256:" + "0" * 64 for r in caplog.records)


def test_n19_the_projection_is_the_code_projection_and_raw_code_keeps_the_entry(tmp_path):
    node = _parse_one(tmp_path, make_entry())
    assert json.loads(node.projection) == code_projection(_projection())
    assert node.projection == json.dumps(code_projection(_projection()), sort_keys=True, indent=2)
    assert "owner" not in json.loads(node.projection)
    assert json.loads(node.raw_code)["owner"] == "marketing", "raw_code keeps the read set's owner"


# Runtime 0.12.0's harness hashes the loaded node with its defaults; an entry that leaves them out must hash alike.
def test_an_entry_omitting_kind_extra_columns_and_config_hashes_as_one_stating_the_defaults(tmp_path):
    stated = _parse_one(tmp_path, make_entry(kind="python-node", extra_columns="raise", config={}))
    omitted = _parse_one(tmp_path, make_entry(remove=("kind", "extra_columns", "config")))
    assert omitted.config_hash == stated.config_hash == projection_hash(_projection())
    assert omitted.content_hash == stated.content_hash


def test_upsert_takes_a_primary_key_and_other_modes_refuse_one(tmp_path):
    cols = [{"name": "id", "type": "INTEGER", "nullable": False}, {"name": "amount", "type": "NUMERIC(10,2)"}]
    node = _parse_one(tmp_path, make_entry(mode_type="upsert", primary_key=["id"], output_columns=cols))
    assert node.config_hash == projection_hash(_projection(mode_type="upsert", primary_key=["id"]))
    with pytest.raises(MalformedContractError, match="primary_key"):
        _parse_one(tmp_path, make_entry(primary_key=["id"]))
    with pytest.raises(MalformedContractError, match="primary_key"):
        _parse_one(tmp_path, make_entry(mode_type="upsert"))


def test_csv_nodes_accept_only_replace_and_no_assets(tmp_path):
    with pytest.raises(MalformedContractError, match="mode_type"):
        _parse_one(tmp_path, make_csv_entry(mode_type="append"))
    with pytest.raises(MalformedContractError, match="assets"):
        _parse_one(tmp_path, make_csv_entry(assets=["data/x.json"]))


def test_assets_join_the_projection(tmp_path):
    node = _parse_one(tmp_path, make_entry(assets=["lib/ref.json"]))
    assert node.config_hash == projection_hash(_projection(assets=["lib/ref.json"]))


@pytest.mark.parametrize("vector", json.loads((importlib.resources.files("continuo_contracts") / "vectors" / "projection.json").read_text()), ids=lambda v: v["name"])
def test_the_library_projection_vectors(tmp_path, vector):
    entry = {**HASH_FIELDS, **VECTOR_CONTRACT_KEYS, **vector["node"]}
    node = _parse_one(tmp_path, entry)
    assert node.config_hash == vector["projection_hash"]
    assert json.loads(node.projection) == vector["projection"]


def test_the_runtime_wire_fixture_parses(tmp_path):
    entries = [{**HASH_FIELDS, **entry} for entry in yaml.safe_load(WIRE_FIXTURE.read_text())["nodes"]]
    nodes = {n.table_name: n for n in parse_python_contract(write_contract(tmp_path, *entries), "img:1").nodes}
    assert set(nodes) == {"replace_node", "append_node", "upsert_node", "assets_node"}
    assert json.loads(nodes["append_node"].projection)["mode_type"] == "append"
    assert json.loads(nodes["upsert_node"].projection)["primary_key"] == ["rate_date", "base"]
    assert json.loads(nodes["assets_node"].projection)["assets"] == ["data/ref.json"]


def test_a_type_the_projection_refuses_rejects_the_artifact(tmp_path):
    cols = [{"name": "id", "type": "FLOAT", "nullable": False}]
    with pytest.raises(MalformedContractError, match="unsupported or invalid SQL type"):
        _parse_one(tmp_path, make_entry(output_columns=cols))


def test_raw_code_carries_the_mode_primary_key_and_assets_that_differ_from_the_default(tmp_path):
    cols = [{"name": "id", "type": "INTEGER", "nullable": False}, {"name": "amount", "type": "NUMERIC(10,2)"}]
    raw = json.loads(_parse_one(tmp_path, make_entry(mode_type="upsert", primary_key=["id"], output_columns=cols,
                                                     assets=["lib/ref.json"])).raw_code)
    assert (raw["mode_type"], raw["primary_key"], raw["assets"]) == ("upsert", ["id"], ["lib/ref.json"])
    default = json.loads(_parse_one(tmp_path, make_entry(mode_type="replace")).raw_code)
    assert not {"mode_type", "primary_key", "assets"} & set(default)


def test_a_ci_content_hash_mismatch_warns_that_a_ci_older_than_0_12_hashes_the_config_part_differently(tmp_path, caplog):
    _parse_one(tmp_path, make_entry(content_hash="sha256:" + "0" * 64))
    (record,) = [r for r in caplog.records if r.levelname == "WARNING"]
    assert record.getMessage() == (
        "python contract: the producer CI's content_hash differs from the one topology-controller computes:"
        " a CI on continuo-python-runtime older than 0.12.0 hashes the config part differently, and the two"
        " agree once the service releases on 0.12.0 or later")
