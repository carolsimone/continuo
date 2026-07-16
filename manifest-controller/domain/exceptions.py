from __future__ import annotations


class MalformedRuntimeManifestError(ValueError):
    """A runtime manifest descriptor is unreadable or fails validation.

    Permanent: no re-delivery can turn a rejected descriptor into a valid one,
    so the release is failed rather than left pending for retry.
    """


class UnqualifiedTableReferenceError(ValueError):
    def __init__(self, table_name: str, node_table_name: str) -> None:
        self.table_name = table_name
        self.node_table_name = node_table_name
        super().__init__(
            f"Unqualified table reference '{table_name}' in node '{node_table_name}'"
            " — all tables must use schema.table form"
        )
