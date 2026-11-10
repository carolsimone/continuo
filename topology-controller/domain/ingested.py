from __future__ import annotations
from dataclasses import dataclass

from domain.model import TableRef


@dataclass(frozen=True)
class SystemRelations:
    """The relations a warehouse engine keeps for itself: its catalog views and
    internal tables, which no release produces and which therefore never become
    nodes.

    A relation is a system relation when its schema is one of schemas, when its
    schema starts with one of schema_prefixes, or when its catalog is one of
    catalogs. Every name is lower-case, as a TableRef holds it.
    """

    schemas: frozenset[str] = frozenset()
    schema_prefixes: tuple[str, ...] = ()
    catalogs: frozenset[str] = frozenset()

    def matches(self, ref: TableRef) -> bool:
        return (
            ref.schema in self.schemas
            or ref.schema.startswith(self.schema_prefixes)
            or ref.catalog in self.catalogs
        )
