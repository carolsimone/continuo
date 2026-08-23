from domain.model import ManifestNode


def _node(schema: str, table: str) -> ManifestNode:
    return ManifestNode(
        table_name=table,
        schema_name=schema,
        service_name="finance",
        owner="data-team",
        schedule_name="daily",
        criticality="CORE",
    )


def test_unique_id_is_lowercase():
    """unique_id is the identity key and must match the case-insensitive
    lookups in NodeRegistry.to_lookup() and service/rewriter.py."""
    assert _node("Analytics", "Orders").unique_id == "analytics.orders"


def test_unique_id_folds_case_variants_together():
    """Two nodes whose declared names differ only in case address the same
    physical relation, so they must share one identity key."""
    assert _node("analytics", "TABLE_JESUS").unique_id == _node("analytics", "table_jesus").unique_id


def test_declared_schema_and_table_are_not_normalized():
    """schema_name and table_name render into real SQL and DDL downstream, so
    they keep the case the manifest declared."""
    node = _node("Analytics", "Orders")
    assert node.schema_name == "Analytics"
    assert node.table_name == "Orders"
