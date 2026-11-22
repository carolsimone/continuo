from __future__ import annotations

from domain.contract_vocabulary import ParseFailureKind


class UnqualifiedTableReferenceError(ValueError):
    kind = ParseFailureKind.UNQUALIFIED_REFERENCE

    def __init__(self, table_name: str, node_table_name: str) -> None:
        self.table_name = table_name
        self.node_table_name = node_table_name
        super().__init__(
            f"Unqualified table reference '{table_name}' in node '{node_table_name}'"
            " — all tables must use schema.table form"
        )


class InvalidCompiledSqlError(ValueError):
    kind = ParseFailureKind.INVALID_SQL

    def __init__(self, node_table_name: str, detail: str) -> None:
        self.node_table_name = node_table_name
        self.detail = detail
        super().__init__(f"Invalid compiled SQL in node '{node_table_name}': {detail}")


class MalformedContractError(ValueError):
    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"Malformed python contract: {detail}")


class InvalidOdcsBundleError(ValueError):
    """A `python-odcs` producer bundle (`odcs-bundle.json`) that cannot be read
    as a whole: it is not the JSON document of a version this install reads, it
    names another service, one of its keys is missing or mistyped, two of its
    nodes share a path or resolve to one node, or its producer's
    continuo-contracts is newer than the install's. A re-delivery cannot fix
    it. A contract that is invalid on its own is not this error: it is one
    failed node of an otherwise readable bundle."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


class InvalidNodeSettingError(ValueError):
    """A dbt manifest holds a value the parse cannot honour as a whole: a
    `metadata.project_name` or a `nodes` of the wrong shape, a test whose `fqn`
    or links to the nodes it tests cannot be read, a node the parse builds with
    no `fqn` entry, or a value the hashing refuses. No one node's contract is to
    blame, so the release is an invalid artifact. The message names where the
    value was found (a node, a test, `manifest` or the manifest's path) and the
    field."""


class InvalidCompileExtrasError(ValueError):
    """A compile-extras.json that cannot be read or does not hold what the
    compile promises to write. A re-delivery cannot fix it."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"compile-extras.json: {detail}")


class InvalidOntologyError(ValueError):
    """The install's ontology file cannot be used as a whole: it is not UTF-8 text, it is
    not an ontology file the library reads, one of its own rules finds an error in it, or
    the release it belongs to cannot be read from its key. A re-delivery cannot fix it.
    The message leads with the service that released the file.

    rule_errors holds the rule of each error the file's own rules found, one entry per
    error, so the release can count them; it is empty when the file did not load."""

    def __init__(self, detail: str, rule_errors: tuple[str, ...] = ()) -> None:
        self.detail = detail
        self.rule_errors = rule_errors
        super().__init__(detail)
