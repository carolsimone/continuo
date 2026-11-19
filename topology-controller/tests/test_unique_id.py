from domain.model import ManifestNode, NodeType, UpstreamDep, relation_key


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


def test_dbt_test_identity_is_the_manifest_unique_id():
    n = ManifestNode(table_name="not_null_tbind_amount_eur", schema_name="e2e_schema_dbt_test__audit",
                     service_name="service-2", owner="", schedule_name="", criticality="SECONDARY",
                     node_type=NodeType.DBT_TEST, identity="test.service_2.not_null_tbind_amount_eur.abc123")
    assert n.unique_id == "test.service_2.not_null_tbind_amount_eur.abc123"
    assert n.resolved_relation_id == ""


def test_model_identity_is_schema_dot_table_when_no_identity_given():
    n = ManifestNode(table_name="Orders", schema_name="Analytics", service_name="s", owner="o",
                     schedule_name="daily", criticality="SECONDARY")
    assert n.unique_id == "analytics.orders"


def test_every_relation_key_lowers_the_schema_and_the_table_each_on_its_own():
    """A node's unique_id, the upstream it is named by and the relation it writes come
    from one rule, relation_key, which lowers each part apart: a capital Σ at the end
    of the schema keeps its final form, as lowering the joined string would not."""
    node = _node("DATAΣ", "Orders")
    upstream = UpstreamDep(table_name="Orders", schema_name="DATAΣ", service_name="finance")
    assert relation_key("DATAΣ", "Orders") == "data\u03c2.orders" != "DATAΣ.Orders".lower()
    assert node.unique_id == upstream.unique_id == node.resolved_relation_id == "data\u03c2.orders"
