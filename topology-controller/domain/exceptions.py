from __future__ import annotations


class UnqualifiedTableReferenceError(ValueError):
    def __init__(self, table_name: str, node_table_name: str) -> None:
        self.table_name = table_name
        self.node_table_name = node_table_name
        super().__init__(
            f"Unqualified table reference '{table_name}' in node '{node_table_name}'"
            " — all tables must use schema.table form"
        )


class InvalidCompiledSqlError(ValueError):
    def __init__(self, node_table_name: str, detail: str) -> None:
        self.node_table_name = node_table_name
        self.detail = detail
        super().__init__(f"Invalid compiled SQL in node '{node_table_name}': {detail}")


class MalformedContractError(ValueError):
    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"Malformed python contract: {detail}")
