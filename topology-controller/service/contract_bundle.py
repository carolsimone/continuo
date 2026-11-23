"""The contract bundle: one immutable document per release holding the stored form of
every contract it carries, a dbt node's and a Python node's, stored gzipped at
tenants/<tenant>/topologies/<release>/contracts.json.gz.

topology-controller writes it for every release it publishes as ok, after the code
bundle and before the topology artifact, so a release with no contract writes an
empty one. The Go readers (pkg/contractbundle) verify the SHA-256 an event carries
against the stored bytes. The document is canonical JSON (RFC 8785, the
continuo-contracts library's canonical_json), and tests/fixtures/contracts_v1.json, a
copy of the fixture the Go package reads, pins this builder to what that package decodes.

`store_contracts` turns each node's contract into its stored form (the document the
hashes are taken from, with the tables the node reads), lets the caller's `bind` add
the ontology part to it or take it away, and hashes it, and `build_contract_bundle`
assembles the document, with the ontology the release used or null. A contract that
cannot list a table its node reads (`invalid_contract_reads`), whether the node's SQL
names it or a dbt contract declares it, is the team's mistake: it is reported as an
invalid contract before any stored form is built.
"""
import copy
import dataclasses
import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from continuo_contracts.contract import contract_hash, normalise_dbt, normalise_python
from continuo_contracts.schema import SCHEMA_SHA256
from continuo_contracts.vocabulary import continuo_values

from domain.contract_vocabulary import NodeType, ParseFailureKind
from domain.external import SystemRelations
from domain.model import ContractFamily, FailedNode, ManifestNode, NodeContract, TableRef
from service.release_timings import REASON_VALUE, FailureStage, ReleaseTimings

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

# What a failure names the tables of a node's SQL as, by contract family.
_SQL_SOURCE = {ContractFamily.PYTHON: "readQueries", ContractFamily.DBT: "compiled SQL"}

# The control characters the library refuses in a listed table: U+0000 to U+001F and U+007F to U+009F.
_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")


def contract_bundle_key(tenant_id: str, release_id: str) -> str:
    """The S3 object key of a release's contract bundle."""
    return f"tenants/{tenant_id}/topologies/{release_id}/contracts.json.gz"


@dataclass(frozen=True)
class StoredContract:
    """One node's contract as the bundle keeps it: the stored form of its ODCS document
    and that document's hash, the family of the producer that wrote it, the node's own
    code fingerprint, and the file it stands for (its repository path and the SHA-256 of
    what it was written from, bare hex)."""
    unique_id: str
    family: ContractFamily
    service: str
    contract_hash: str
    content_hash: str
    source_path: str
    source_sha256: str
    document: dict


def _no_progress() -> None:
    """The progress callback of a caller nobody watches."""


def _as_stored(stored: dict[str, Any], node: ManifestNode) -> dict[str, Any]:
    """The binder of a caller that has no ontology part to add or take away: the stored
    form as it is."""
    return stored


def _unlistable_reason(entry: str) -> str | None:
    """Why a contract cannot list `entry` as a table, "<schema>.<table>", or None when it can.

    The rule is the library's for a listed table: exactly two parts joined by a dot,
    neither empty, and no whitespace (what `str.isspace` reads as such) or control
    character in the entry.
    """
    parts = entry.split(".")
    if len(parts) < 2:
        return "the entry has no dot"
    if len(parts) > 2:
        return "a name holds a dot"
    if not all(parts):
        return "a name is empty"
    if any(character.isspace() for character in entry):
        return "a name holds whitespace"
    if _CONTROL.search(entry):
        return "a name holds a control character"
    return None


def _as_written(part: str) -> str:
    """A name part as the message shows it: plain when it is a plain name, quoted (and
    escaped) when it holds whitespace, a dot or a control character."""
    needs_quotes = (
        not part
        or "." in part
        or any(character.isspace() for character in part)
        or _CONTROL.search(part) is not None
    )
    return json.dumps(part, ensure_ascii=False) if needs_quotes else part


def _declared_reads(contract: NodeContract) -> list[str]:
    """The tables a dbt contract's schema object lists in its `continuo` property `reads`:
    the reads the node's meta declares, which `dbt.generate` writes as a list of strings."""
    table_object = contract.document["schema"][0]
    return list(continuo_values(table_object.get("customProperties", [])).get("reads", []))


def invalid_contract_reads(
    node: ManifestNode,
    refs: Sequence[TableRef],
    system: SystemRelations,
    *,
    timings: ReleaseTimings | None = None,
) -> FailedNode | None:
    """The failed node of a contract that cannot list a table its node reads, or None when
    every table the node reads can be listed.

    refs are the tables the node's SQL names (the resolve step's), a system relation of
    the warehouse engine left out as `store_contracts` leaves it out. A table the
    contract cannot list as "<schema>.<table>" (for one, a quoted name holding whitespace or
    a dot) is the team's mistake, not a defect: the node is reported as an invalid
    contract, named by its unique_id and the contract's file, and `detail` names each such
    table in schema and table order. A dbt contract also lists the tables its node's meta
    declares it reads, system relations included, which `detail` names after those of the
    SQL, each entry as written, in code point order. Each is counted on timings, stage
    `normalise`, reason `value`. A node with no contract has nothing to list.
    """
    contract = node.contract
    if contract is None:
        return None
    family = contract.family
    problems: list[str] = []
    # A table is one problem however many catalogs the queries name it through.
    tables = sorted({(ref.schema, ref.table) for ref in refs if not system.matches(ref)})
    for schema, table in tables:
        reason = _unlistable_reason(f"{schema}.{table}")
        if reason is not None:
            problems.append(
                f"{_SQL_SOURCE[family]}: table {_as_written(schema)}.{_as_written(table)}"
                f" cannot be listed as <schema>.<table>: {reason}"
            )
    if family is ContractFamily.DBT:
        for entry in sorted(set(_declared_reads(contract))):
            reason = _unlistable_reason(entry)
            if reason is not None:
                problems.append(
                    f"reads: {json.dumps(entry, ensure_ascii=False)} cannot be listed as <schema>.<table>: {reason}"
                )
    if timings is not None:
        for _ in problems:
            timings.add_contract_failure(FailureStage.NORMALISE, REASON_VALUE)
    if not problems:
        return None
    return FailedNode(
        node_id=node.unique_id,
        kind=ParseFailureKind.INVALID_CONTRACT,
        service=node.service_name,
        file_path=contract.path,
        node_type=node.node_type,
        detail="; ".join(problems),
    )


def shared_unique_ids(nodes: Sequence[ManifestNode]) -> dict[str, list[str]]:
    """Each unique_id that more than one node of the release declares (a dbt test, which
    writes no relation, aside), with the services that declare it, sorted.

    Such a release is release-controller's to reject as duplicate_table, which names
    every service involved. Its contracts stay out of the bundle: the bundle keys a
    contract by unique_id and cannot hold two."""
    services: dict[str, list[str]] = {}
    for node in nodes:
        if node.node_type != NodeType.DBT_TEST:
            services.setdefault(node.unique_id, []).append(node.service_name)
    return {uid: sorted(names) for uid, names in sorted(services.items()) if len(names) > 1}


def store_contracts(
    nodes: Sequence[ManifestNode],
    reads: Mapping[tuple[str, str], Sequence[TableRef]],
    system: SystemRelations,
    *,
    progress: Callable[[], None] = _no_progress,
    timings: ReleaseTimings | None = None,
    bind: Callable[[dict[str, Any], ManifestNode], dict[str, Any]] = _as_stored,
) -> list[StoredContract]:
    """The stored form and hash of the contract of each node that has one, in node order.

    reads holds the tables each node's SQL names, by the node's node_key (the resolve
    step's map). The stored form of a contract names the tables its node reads, each as
    "<schema>.<table>": a system relation of the warehouse engine is left out, and the
    catalog of a table, which a contract has no place for, is dropped. A dbt contract
    also keeps the tables its meta declares it reads. The library lower-cases the names,
    sorts them and lists each once.

    bind receives each stored form and its node and returns the form that is kept and
    hashed: the caller's way to take the ontology part away, keep it as written or fill it
    from the ontology. It runs after the stored form is made and before the hash, so the
    hash is the bound form's.

    A contract reaching this point has passed every check of the parse, so a library
    error here, bind's included, is a defect, not a fault of the contract. It is counted on
    timings (stage `normalise` or `hash`, reason `value`) and raised, and the caller reports
    it as an internal failure of the release. progress is called after each contract.

    Once a contract's stored form is made, its node keeps the contract without the
    producer's document (NodeContract.document is None), which nothing reads after this
    step, so the release does not hold both through the rest of its parse. A contract
    whose document is released already raises ValueError: it was stored before.
    """
    stored: list[StoredContract] = []
    for node in nodes:
        contract = node.contract
        if contract is None:
            continue
        if contract.document is None:
            raise ValueError(f"{node.unique_id}: the contract is stored already, and its document released")
        tables = [f"{ref.schema}.{ref.table}" for ref in reads.get(node.node_key, ()) if not system.matches(ref)]
        document = _library_call(
            FailureStage.NORMALISE, node, timings, lambda: bind(_normalise(contract, node, tables), node))
        digest = _library_call(FailureStage.HASH, node, timings, lambda: contract_hash(document))
        stored.append(StoredContract(
            unique_id=node.unique_id,
            family=contract.family,
            service=node.service_name,
            contract_hash=digest,
            content_hash=node.content_hash,
            source_path=contract.path,
            source_sha256=contract.source_sha256,
            document=document,
        ))
        node.contract = dataclasses.replace(contract, document=None)
        progress()
    return stored


def _normalise(contract: NodeContract, node: ManifestNode, tables: list[str]) -> dict[str, Any]:
    """The stored form of a node's contract, by the rules of its family: a dbt contract
    joins the tables it names with the ones it declares, a Python contract takes the
    node's service."""
    if contract.family is ContractFamily.DBT:
        return normalise_dbt(contract.document, reads=tables)
    return normalise_python(contract.document, service=node.service_name, reads=tables)


def _library_call[T](stage: FailureStage, node: ManifestNode, timings: ReleaseTimings | None, call: Callable[[], T]) -> T:
    """Run one call of the contracts library for node. A failure is logged with the node's
    name, counted under stage and raised as it is."""
    try:
        return call()
    except Exception:
        logger.exception(
            "candidate: the library refused a contract that passed every check",
            extra={"node_id": node.unique_id, "service": node.service_name, "stage": stage.value},
        )
        if timings is not None:
            timings.add_contract_failure(stage, REASON_VALUE)
        raise


def contracts_by_family(contracts: Sequence[StoredContract]) -> dict[str, int]:
    """How many of `contracts` each family holds, keyed by the family's value in
    sorted order. A family with none has no key."""
    counts: dict[str, int] = {}
    for contract in contracts:
        family = contract.family.value
        counts[family] = counts.get(family, 0) + 1
    return dict(sorted(counts.items()))


def build_contract_bundle(
    *,
    tenant_id: str,
    release_id: str,
    library_version: str,
    contracts: Sequence[StoredContract],
    provenance: Mapping[str, Mapping[str, Any]],
    timings: Mapping[str, Any],
    ontology: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The bundle document of a release.

    contracts are keyed by unique_id in the document, so no two may share one: the
    caller leaves out the contracts of every id shared_unique_ids names. Each contract
    keeps its own family. provenance holds, per service whose contracts a producer bundle
    carried, the runtime that packed them and the packages of its image; timings is the
    release's ReleaseTimings.bundle_timings(). ontology is the value of the document's
    `ontology` key: None, written as null, for a release with the ontology switch off.
    The summary counts the contracts the bundle
    holds, in all and by family (a family with none has no key). Its `invalid` and
    `skipped` are 0: a release with an invalid contract fails and publishes no bundle.
    library_version is the continuo-contracts version that stored the contracts, and the
    document names the ODCS schema they were checked against. The result shares nothing
    with its inputs.
    """
    by_id: dict[str, dict[str, Any]] = {}
    for contract in contracts:
        if contract.unique_id in by_id:
            raise ValueError(f"two contracts of the release carry the unique id {contract.unique_id}")
        by_id[contract.unique_id] = {
            "family": contract.family.value,
            "service": contract.service,
            "contract_hash": contract.contract_hash,
            "content_hash": contract.content_hash,
            "source": {"path": contract.source_path, "sha256": contract.source_sha256},
            "document": contract.document,
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "tenant_id": tenant_id,
        "release_id": release_id,
        "odcs_schema_sha256": SCHEMA_SHA256,
        "continuo_contracts": library_version,
        "ontology": copy.deepcopy(dict(ontology)) if ontology is not None else None,
        "provenance": copy.deepcopy(dict(provenance)),
        "summary": {
            "contracts": len(by_id),
            "by_family": contracts_by_family(contracts),
            "invalid": 0,
            "skipped": 0,
        },
        "failures": [],
        "timings": copy.deepcopy(dict(timings)),
        "contracts": by_id,
    }
