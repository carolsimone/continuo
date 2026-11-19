"""The check of a contract: the ODCS schema check, cached, then continuo's rules.

The JSON schema check of continuo-contracts is the costly part of checking a contract, and
a release re-checks every contract of every service, nearly all of them unchanged since the
release before. `ContractValidator` runs it once per distinct document and keeps the answer
for the next release, keyed by the SHA-256 of the document's canonical JSON. A Python
contract (`check`) and a dbt contract (`check_dbt`) are checked against the same ODCS
schema, so the two families share one memory and a document is checked once whichever
family asks. continuo's rules differ by family, and the Python rules read the contract's
file name and the release's service, which the document does not carry, so the rules run
on every check.
"""
from collections.abc import Callable, Mapping
from typing import Any, cast

from continuo_contracts.canonical import canonical_json
from continuo_contracts.hashing import sha256_hex
from continuo_contracts.validate import Issue, dbt_rule_issues, rule_issues, schema_issues

DEFAULT_MAX_ENTRIES = 100_000
"""How many documents a validator remembers before it forgets the oldest. A release of a
large estate holds tens of thousands of contracts, and the Python ones must stay
remembered while the dbt ones are checked."""


class ContractValidator:
    """Checks contracts against the ODCS schema and continuo's rules, remembering
    each document's schema issues.

    The memory is the object's own and lives as long as the object does; it holds
    at most `max_entries` documents, and one that no longer fits replaces the
    oldest, which is only ever checked again from scratch.
    """

    def __init__(self, *, max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
        if max_entries < 1:
            raise ValueError(f"max_entries must be at least 1, got {max_entries}")
        self._max_entries = max_entries
        self._schema_issues: dict[str, tuple[Issue, ...]] = {}

    def check(
        self, doc: object, *, path: str, service: str, on_cache: Callable[[bool], None]
    ) -> list[Issue]:
        """Return every issue of `doc`, the Python contract of the file at `path` in the
        release of `service`, or an empty list when it is valid.

        `doc` is what `yaml_strict.load` returned. The result is its schema issues
        when it has any, else the issues of continuo's Python rules. The schema check is
        answered from the memory when `doc` was checked before, and `on_cache` is
        told how it was answered: True for a hit, False for a miss that ran the check.
        """
        known = self._schema_issues_of(doc, on_cache)
        if known:
            return list(known)
        # The schema accepts only an object, so a document without schema issues is a mapping.
        return rule_issues(cast(Mapping[str, Any], doc), path=path, service=service)

    def check_dbt(self, doc: object, *, on_cache: Callable[[bool], None]) -> list[Issue]:
        """Return every issue of `doc`, the contract of a dbt node, or an empty list when
        it is valid.

        The result is its schema issues when it has any, else the issues of continuo's dbt
        rules, which read nothing but the document. `on_cache` is told how the schema
        check was answered, as for `check`.
        """
        known = self._schema_issues_of(doc, on_cache)
        if known:
            return list(known)
        # The schema accepts only an object, so a document without schema issues is a mapping.
        return dbt_rule_issues(cast(Mapping[str, Any], doc))

    def _schema_issues_of(self, doc: object, on_cache: Callable[[bool], None]) -> tuple[Issue, ...]:
        """The schema issues of `doc`, from the memory when it holds them, else from the
        schema check, whose answer it then remembers."""
        key = sha256_hex(canonical_json(doc))
        known = self._schema_issues.get(key)
        on_cache(known is not None)
        if known is None:
            known = tuple(schema_issues(doc))
            if len(self._schema_issues) >= self._max_entries:
                del self._schema_issues[next(iter(self._schema_issues))]
            self._schema_issues[key] = known
        return known
