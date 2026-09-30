"""Per-kind rules for python contract entries.

Each python node type is one KindRules entry: whether it carries a script and
how its reads are validated and turned into upstream SQL or a csv source. The
parser looks the kind up here instead of branching on its name.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from domain.contract_vocabulary import NodeType
from domain.exceptions import MalformedContractError


def _fail(detail: str) -> None:
    raise MalformedContractError(detail)


def _non_empty_str(value, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        _fail(f"{label} must be a non-empty string, got {value!r}")
    return value


@dataclass(frozen=True)
class ParsedReads:
    reads: dict[str, str]
    dependency_sqls: list[str]
    csv_source: str


class ReadsRule(ABC):
    @abstractmethod
    def parse(self, raw, label: str) -> ParsedReads: ...


class SqlReads(ReadsRule):
    """A non-empty mapping of read name -> SQL; every read is an upstream query."""

    def parse(self, raw, label: str) -> ParsedReads:
        if not isinstance(raw, dict) or not raw:
            _fail(f"{label}: reads must be a non-empty mapping of read name -> SQL")
        reads: dict[str, str] = {}
        for name, sql in raw.items():
            _non_empty_str(name, f"{label}: read name")
            _non_empty_str(sql, f"{label}: reads[{name!r}]")
            reads[name] = sql
        return ParsedReads(
            reads=reads,
            dependency_sqls=[reads[name] for name in sorted(reads)],
            csv_source="",
        )


class CsvRead(ReadsRule):
    """Exactly {csv: <uri>}; the uri is a file location, never SQL."""

    def parse(self, raw, label: str) -> ParsedReads:
        if not isinstance(raw, dict) or set(raw) != {"csv"}:
            _fail(f"{label}: a python-csv node's reads must be exactly {{csv: <uri>}}")
        uri = _non_empty_str(raw["csv"], f"{label}: reads['csv']")
        _validate_csv_uri(uri, label)
        return ParsedReads(reads={"csv": uri}, dependency_sqls=[], csv_source=uri)


class NoReads(ReadsRule):
    """An empty mapping: the node's script fetches its own data, so it has no
    upstream query and is a DAG root."""

    def parse(self, raw, label: str) -> ParsedReads:
        if not isinstance(raw, dict) or raw:
            _fail(f"{label}: a python-api node declares no reads; its wire entry carries reads: {{}}")
        return ParsedReads(reads={}, dependency_sqls=[], csv_source="")


def _validate_csv_uri(uri: str, label: str) -> None:
    """Mirrors continuo_python_runtime.csv_source.parse_csv_uri's grammar
    exactly, so a contract this loader accepts is one the pinned runner's
    validation header-fetch can actually parse. A prefix-only check would
    accept shapes the runner rejects (a bucket-less ``s3://bucket`` with no
    object key, a host-less ``https://``), letting topology-controller wave
    a malformed csv contract through a release that only fails once the
    validation Job actually runs. Accepts exactly ``s3://bucket/key`` (both
    non-empty) or ``https://<non-empty-host>[/...]``; every other shape —
    including ``http://`` — is rejected here, at parse time.
    """
    if uri.startswith("s3://"):
        bucket, _, key = uri[len("s3://"):].partition("/")
        if bucket and key:
            return
        _fail(f"{label}: invalid s3 csv uri (missing bucket or key): {uri!r}")
    elif uri.startswith("https://"):
        host, _, _ = uri[len("https://"):].partition("/")
        if host:
            return
        _fail(f"{label}: invalid https csv uri (missing host): {uri!r}")
    else:
        _fail(
            f"{label}: reads['csv'] must be an s3://bucket/key or"
            f" https://<host>/... uri, got {uri!r}"
        )


@dataclass(frozen=True)
class KindRules:
    script_required: bool
    reads: ReadsRule
    secret_allowed: bool = False


RULES: dict[NodeType, KindRules] = {
    NodeType.PYTHON_NODE: KindRules(script_required=True, reads=SqlReads()),
    NodeType.PYTHON_CSV: KindRules(script_required=False, reads=CsvRead()),
    NodeType.PYTHON_API: KindRules(script_required=True, reads=NoReads(), secret_allowed=True),
}

# Contracts produced by continuo-python-runtime < 0.6.0 declare python-node as
# "python-model". Every release re-parses every service's stored production
# contract, so those contracts keep resolving until their service releases
# again on a newer runtime.
STORED_KIND_ALIASES: dict[str, NodeType] = {"python-model": NodeType.PYTHON_NODE}


def resolve_kind(raw, label: str) -> NodeType:
    """The python NodeType an entry declares; absent means python-node."""
    if raw is None:
        return NodeType.PYTHON_NODE
    if isinstance(raw, str):
        if raw in STORED_KIND_ALIASES:
            return STORED_KIND_ALIASES[raw]
        for node_type in RULES:
            if raw == node_type:
                return node_type
    _fail(f"{label}: kind must be one of {sorted(str(t) for t in RULES)}, got {raw!r}")
