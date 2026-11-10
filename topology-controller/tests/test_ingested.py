import pytest

from config.config import _SYSTEM_RELATIONS
from domain.ingested import SystemRelations
from domain.model import TableRef


@pytest.mark.parametrize("engine,ref,system", [
    ("postgres", TableRef("information_schema", "tables"), True),
    ("postgres", TableRef("pg_catalog", "pg_class"), True),
    ("postgres", TableRef("pg_toast", "pg_toast_1"), True),
    ("postgres", TableRef("pg_temp_3", "scratch"), True),
    ("postgres", TableRef("raw", "orders"), False),
    ("postgres", TableRef("pgx", "orders"), False),
    ("trino", TableRef("information_schema", "tables"), True),
    ("trino", TableRef("information_schema", "tables", catalog="hive"), True),
    ("trino", TableRef("runtime", "nodes", catalog="system"), True),
    ("trino", TableRef("raw", "orders", catalog="hive"), False),
    ("trino", TableRef("raw", "orders"), False),
])
def test_system_relations_per_engine(engine, ref, system):
    assert _SYSTEM_RELATIONS[engine].matches(ref) is system


def test_a_catalog_only_matches_for_an_engine_that_lists_it():
    """Postgres lists no catalog, so a catalog name never makes a relation a
    system one there."""
    assert not _SYSTEM_RELATIONS["postgres"].matches(TableRef("raw", "orders", catalog="system"))


def test_a_relation_matches_on_any_one_of_the_three_rules():
    relations = SystemRelations(
        schemas=frozenset({"meta"}), schema_prefixes=("sys_",), catalogs=frozenset({"admin"}),
    )
    assert relations.matches(TableRef("meta", "t"))
    assert relations.matches(TableRef("sys_stats", "t"))
    assert relations.matches(TableRef("anything", "t", catalog="admin"))
    assert not relations.matches(TableRef("raw", "t", catalog="warehouse"))
