"""A manifest whose nodes hold a field in a shape dbt never writes is an invalid artifact, published
as a rejection: the hashing and the parse read these fields, and a wrong shape there used to fail the
fold with an `AttributeError`, which the consumer retried until the message was dead-lettered."""
import json
from unittest.mock import MagicMock

import pytest

from domain.contract_vocabulary import ParseFailureKind
from domain.model import ManifestFile
from service import manifest_shape
from tests.fakes import build_handler, dbt_node, make_uploader, source_of

_HOOK = {
    "unique_id": "operation.service-1.service-1-on-run-start-0", "resource_type": "operation",
    "name": "service-1-on-run-start-0", "raw_code": "select 1", "package_name": "service-1",
    "depends_on": {"macros": [], "nodes": []}, "tags": [],
}


def _test_node(**over):
    return {
        "unique_id": "test.service-1.not_null_orders_id", "resource_type": "test", "name": "not_null_orders_id",
        "schema": "test_schema_dbt_test__audit", "fqn": ["service-1", "not_null_orders_id"],
        "attached_node": "model.service-1.orders", "compiled_code": "select id from test_schema.orders",
        "depends_on": {"macros": [], "nodes": ["model.service-1.orders"]}, **over,
    }


def _of(node):
    return {node["unique_id"]: node}


def _refusal(nodes, **collections):
    """What the shape check says of a manifest of these nodes and these other collections."""
    return manifest_shape.refusal({"nodes": nodes, **collections})


KINDS = {"model": lambda **over: dbt_node("orders") | over, "test": _test_node, "operation": lambda **over: _HOOK | over}


# --- refusal -------------------------------------------------------------------------

@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("field, bad, kind_of_value", [
    ("raw_code", 5, "a string"),
    ("compiled_code", ["select 1"], "a string"),
    ("database", 5, "a string"),
    ("depends_on", "oops", "a mapping"),
])
def test_a_field_of_another_shape_is_refused_by_node_and_field(kind, field, bad, kind_of_value):
    node = KINDS[kind](**{field: bad})

    assert _refusal(_of(node)) == (
        f"manifest: nodes.{node['unique_id']}.{field} must be {kind_of_value}, got {type(bad).__name__}")


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("field", ["macros", "nodes"])
@pytest.mark.parametrize("bad", ["oops", [{"a": 1}], [1]])
def test_a_depends_on_list_that_is_not_a_list_of_strings_is_refused(kind, field, bad):
    node = KINDS[kind](depends_on={"macros": [], "nodes": [], field: bad})

    assert _refusal(_of(node)) == (
        f"manifest: nodes.{node['unique_id']}.depends_on.{field} must be a list of strings,"
        f" got {type(bad).__name__}")


@pytest.mark.parametrize("node", [None, "oops", ["model.service-1.orders"], 5])
def test_a_node_that_is_not_an_object_is_refused(node):
    assert _refusal({"model.service-1.orders": node}) == (
        f"manifest: nodes.model.service-1.orders must be a mapping, got {type(node).__name__}")


def test_the_first_unreadable_node_in_manifest_order_is_the_one_named():
    nodes = _of(dbt_node("fine")) | {"model.service-1.b": dbt_node("b") | {"database": 1}} | _of(
        dbt_node("c") | {"raw_code": 1})

    assert _refusal(nodes) == "manifest: nodes.model.service-1.b.database must be a string, got int"


@pytest.mark.parametrize("kind", KINDS)
def test_dbt_writes_null_for_what_a_node_does_not_have_and_that_is_read_as_empty(kind):
    node = KINDS[kind](raw_code=None, compiled_code=None, database=None, depends_on=None)

    assert _refusal(_of(node)) is None


@pytest.mark.parametrize("kind", KINDS)
def test_empty_values_and_values_of_the_right_shape_are_not_refused(kind):
    node = KINDS[kind](raw_code="", compiled_code="", database="db", depends_on={"macros": None, "nodes": None})

    assert _refusal(_of(node)) is None


@pytest.mark.parametrize("resource_type", ["analysis", "sql_operation", "rpc", "macro"])
def test_a_node_the_parse_never_reads_is_not_checked(resource_type):
    node = {"unique_id": f"{resource_type}.service-1.x", "resource_type": resource_type,
            "compiled_code": 5, "raw_code": 5, "database": 5, "depends_on": "oops"}

    assert _refusal(_of(node)) is None


@pytest.mark.parametrize("resource_type", [["model"], {"a": 1}, 5, None])
def test_a_resource_type_of_another_shape_does_not_raise(resource_type):
    node = dbt_node("orders") | {"resource_type": resource_type, "compiled_code": 5}

    assert _refusal(_of(node)) is None


@pytest.mark.parametrize("nodes", [None])
def test_nodes_that_is_null_is_refused(nodes):
    assert manifest_shape.refusal({"nodes": nodes}) == "manifest: nodes must be a mapping, got NoneType"


def test_a_manifest_with_no_nodes_key_raises_the_key_error_the_parse_reports():
    with pytest.raises(KeyError, match="nodes"):
        manifest_shape.refusal({"metadata": {}})


@pytest.mark.parametrize("collection", ["macros", "functions", "unit_tests"])
@pytest.mark.parametrize("bad", [["x"], "oops", 5])
def test_a_collection_that_is_not_an_object_is_refused(collection, bad):
    assert _refusal({}, **{collection: bad}) == (
        f"manifest: {collection} must be a mapping, got {type(bad).__name__}")


@pytest.mark.parametrize("collection", ["macros", "functions", "unit_tests"])
@pytest.mark.parametrize("bad", [[], ["x"], "oops", 5, None])
def test_an_entry_of_a_collection_that_is_not_an_object_is_refused(collection, bad):
    assert _refusal({}, **{collection: {f"{collection[:-1]}.pkg.x": bad}}) == (
        f"manifest: {collection}.{collection[:-1]}.pkg.x must be a mapping, got {type(bad).__name__}")


@pytest.mark.parametrize("collection", ["macros", "functions", "unit_tests"])
@pytest.mark.parametrize("value", [None, {}, {"x.y.z": {"depends_on": {"macros": []}}}])
def test_a_collection_that_is_absent_null_empty_or_of_objects_is_not_refused(collection, value):
    assert _refusal(_of(dbt_node("orders")), **{collection: value}) is None
    assert _refusal(_of(dbt_node("orders"))) is None


# --- through the handler ---------------------------------------------------------------

def _released(tmp_path, document):
    """The one manifest `document` released as `service-1`, through a handler whose publisher records
    what it is told. Returns the publisher and the release's timings."""
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(document))
    publisher = MagicMock()
    source = source_of(ManifestFile(path=str(path), image_tag="", declared_service="service-1"))
    timings = build_handler(source, publisher, make_uploader()).handle(release_id="rel-1")
    return publisher, timings


def _rejection(publisher) -> dict:
    publisher.publish_ok.assert_not_called()
    publisher.publish_failed.assert_called_once()
    return publisher.publish_failed.call_args.kwargs


@pytest.mark.parametrize("document, field", [
    ({"nodes": [dbt_node("orders")]}, "nodes"),
    ({"nodes": {"model.service-1.orders": "oops"}}, "nodes.model.service-1.orders"),
    ({"nodes": {"model.service-1.orders": None}}, "nodes.model.service-1.orders"),
    ({"nodes": _of(dbt_node("orders") | {"compiled_code": 5})}, "nodes.model.service-1.orders.compiled_code"),
    ({"nodes": _of(dbt_node("orders") | {"depends_on": "oops"})}, "nodes.model.service-1.orders.depends_on"),
], ids=["nodes-list", "node-string", "node-null", "compiled-code-number", "depends-on-string"])
def test_a_manifest_of_a_shape_dbt_never_writes_is_rejected_not_retried(tmp_path, document, field):
    publisher, timings = _released(tmp_path, document)

    rejection = _rejection(publisher)
    assert rejection["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    assert rejection["detail"].startswith(f"{tmp_path / 'manifest.json'}: InvalidNodeSettingError('manifest: {field}")
    assert rejection["failed_nodes"] == []


def _reaching(macro):
    """A manifest whose model calls the macro `macro.dbt.x`, which the manifest holds as `macro`."""
    return {
        "nodes": _of(dbt_node("orders") | {"depends_on": {"macros": ["macro.dbt.x"], "nodes": []}}),
        "macros": {"macro.dbt.x": macro},
    }


@pytest.mark.parametrize("document, field", [
    ({"nodes": None}, "nodes"),
    (_reaching([]), "macros.macro.dbt.x"),
    (_reaching("oops"), "macros.macro.dbt.x"),
    (_reaching(5), "macros.macro.dbt.x"),
    ({"nodes": _of(dbt_node("orders")), "macros": ["x"]}, "macros"),
    ({"nodes": _of(dbt_node("orders")), "functions": {"function.pkg.f": []}}, "functions.function.pkg.f"),
    ({"nodes": _of(dbt_node("orders")), "unit_tests": {"unit_test.pkg.u": "x"}}, "unit_tests.unit_test.pkg.u"),
    ({"nodes": _of(dbt_node("orders")), "unit_tests": {"unit_test.pkg.u": None}}, "unit_tests.unit_test.pkg.u"),
], ids=["nodes-null", "macro-list", "macro-string", "macro-number", "macros-list", "function-list",
        "unit-test-string", "unit-test-null"])
def test_a_manifest_whose_collections_have_a_shape_dbt_never_writes_is_rejected_not_retried(tmp_path, document, field):
    publisher, timings = _released(tmp_path, document)

    rejection = _rejection(publisher)
    assert rejection["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    assert rejection["detail"].startswith(f"{tmp_path / 'manifest.json'}: InvalidNodeSettingError('manifest: {field} must be a mapping")
    assert rejection["failed_nodes"] == []
    assert timings.contract_failures == {("generate", "manifest_shape"): 1}


@pytest.mark.parametrize("kind", KINDS)
def test_a_node_test_or_hook_of_a_shape_dbt_never_writes_is_rejected_and_counted(tmp_path, kind):
    nodes = _of(dbt_node("orders")) | _of(KINDS[kind](depends_on="oops"))

    publisher, timings = _released(tmp_path, {"nodes": nodes})

    assert _rejection(publisher)["failure_kind"] == ParseFailureKind.INVALID_ARTIFACT
    assert timings.contract_failures == {("generate", "manifest_shape"): 1}
