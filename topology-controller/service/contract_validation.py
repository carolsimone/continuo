"""The check of an authored contract: the ODCS schema check, cached, then continuo's rules.

The JSON schema check of continuo-contracts is the costly part of checking a contract, and
a release re-checks every contract of every service, nearly all of them unchanged since the
release before. `ContractValidator` runs it once per distinct document and keeps the answer
for the next release, keyed by the SHA-256 of the document's canonical JSON. continuo's
rules read the contract's file name and the release's service, which the document does not
carry, so they run on every check.
"""
from collections.abc import Callable, Mapping
from typing import Any, cast

from continuo_contracts.canonical import canonical_json
from continuo_contracts.hashing import sha256_hex
from continuo_contracts.validate import Issue, rule_issues, schema_issues

DEFAULT_MAX_ENTRIES = 10_000
"""How many documents a validator remembers before it forgets the oldest."""


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
        self._schema_issues: dict[str, list[Issue]] = {}

    def check(
        self, doc: object, *, path: str, service: str, on_cache: Callable[[bool], None]
    ) -> list[Issue]:
        """Return every issue of `doc`, the contract of the file at `path` in the
        release of `service`, or an empty list when it is valid.

        `doc` is what `yaml_strict.load` returned. The result is its schema issues
        when it has any, else the issues of continuo's rules. The schema check is
        answered from the memory when `doc` was checked before, and `on_cache` is
        told how it was answered: True for a hit, False for a miss that ran the check.
        """
        key = sha256_hex(canonical_json(doc))
        known = self._schema_issues.get(key)
        on_cache(known is not None)
        if known is None:
            known = schema_issues(doc)
            if len(self._schema_issues) >= self._max_entries:
                del self._schema_issues[next(iter(self._schema_issues))]
            self._schema_issues[key] = known
        if known:
            return list(known)
        # The schema accepts only an object, so a document without schema issues is a mapping.
        return rule_issues(cast(Mapping[str, Any], doc), path=path, service=service)
