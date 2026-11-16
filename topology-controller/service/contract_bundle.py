"""The contract bundle: one immutable document per release holding the stored form of
every Python contract it carries, stored gzipped at
tenants/<tenant>/topologies/<release>/contracts.json.gz.

topology-controller writes it for every release it publishes as ok, after the code
bundle and before the topology artifact, so a release with no Python contract writes
an empty one. The Go readers (pkg/contractbundle) verify the SHA-256 an event carries
against the stored bytes. The document is canonical JSON (RFC 8785, the
continuo-contracts library's canonical_json), and tests/fixtures/contracts_v1.json, a
copy of the fixture the Go package reads, pins this builder to what that package decodes.

`store_contracts` turns each node's authored contract into its stored form (the
document the hashes are taken from, with the tables the node's read queries name) and
hashes it, and `build_contract_bundle` assembles the document.
"""
import copy
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from continuo_contracts.contract import contract_hash, normalise_python
from continuo_contracts.schema import SCHEMA_SHA256

from domain.external import SystemRelations
from domain.model import ManifestNode, TableRef
from service.release_timings import ReleaseTimings

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

# Every contract of the bundle is a Python node's: the family that names the producer.
_FAMILY = "python"


def contract_bundle_key(tenant_id: str, release_id: str) -> str:
    """The S3 object key of a release's contract bundle."""
    return f"tenants/{tenant_id}/topologies/{release_id}/contracts.json.gz"


@dataclass(frozen=True)
class StoredContract:
    """One node's contract as the bundle keeps it: the stored form of its ODCS document
    and that document's hash, the node's own code fingerprint, and the contract file it
    was read from (its repository path and the SHA-256 of its text, bare hex)."""
    unique_id: str
    service: str
    contract_hash: str
    content_hash: str
    source_path: str
    source_sha256: str
    document: dict


def _no_progress() -> None:
    """The progress callback of a caller nobody watches."""


def store_contracts(
    nodes: Sequence[ManifestNode],
    reads: Mapping[str, Sequence[TableRef]],
    system: SystemRelations,
    *,
    progress: Callable[[], None] = _no_progress,
    timings: ReleaseTimings | None = None,
) -> list[StoredContract]:
    """The stored form and hash of the contract of each node that has one, in node order.

    reads holds the tables each node's SQL names, by the node's unique_id (the resolve
    step's map). The stored form of a contract names the tables its node reads, each as
    "<schema>.<table>": a system relation of the warehouse engine is left out, and the
    catalog of a table, which a contract has no place for, is dropped. The library
    lower-cases the names, sorts them and lists each once.

    A contract reaching this point has passed every check of the parse, so a library
    error here is a defect, not a fault of the contract. It is counted on timings
    (stage `normalise` or `hash`, reason `value`) and raised, and the caller reports it as
    an internal failure of the release. progress is called after each contract.
    """
    stored: list[StoredContract] = []
    for node in nodes:
        authored = node.contract
        if authored is None:
            continue
        tables = [f"{ref.schema}.{ref.table}" for ref in reads.get(node.unique_id, ()) if not system.matches(ref)]
        document = _library_call(
            "normalise", node, timings,
            lambda: normalise_python(authored.document, service=node.service_name, reads=tables))
        digest = _library_call("hash", node, timings, lambda: contract_hash(document))
        stored.append(StoredContract(
            unique_id=node.unique_id,
            service=node.service_name,
            contract_hash=digest,
            content_hash=node.content_hash,
            source_path=authored.path,
            source_sha256=authored.source_sha256,
            document=document,
        ))
        progress()
    return stored


def _library_call[T](stage: str, node: ManifestNode, timings: ReleaseTimings | None, call: Callable[[], T]) -> T:
    """Run one call of the contracts library for node. A failure is logged with the node's
    name, counted under stage and raised as it is."""
    try:
        return call()
    except Exception:
        logger.exception(
            "candidate: the library refused a contract that passed every check",
            extra={"node_id": node.unique_id, "service": node.service_name, "stage": stage},
        )
        if timings is not None:
            timings.add_contract_failure(stage, "value")
        raise


def build_contract_bundle(
    *,
    tenant_id: str,
    release_id: str,
    library_version: str,
    contracts: Sequence[StoredContract],
    provenance: Mapping[str, Mapping[str, Any]],
    timings: Mapping[str, Any],
) -> dict[str, Any]:
    """The bundle document of a release.

    contracts are keyed by unique_id in the document. provenance holds, per service whose
    contracts a producer bundle carried, the runtime that packed them and the packages of
    its image; timings is the release's ReleaseTimings.bundle_timings(). The summary counts
    the contracts the bundle holds, by family (no key for a release with none). Its
    `invalid` and `skipped` are 0: a release with an invalid contract fails and publishes
    no bundle. library_version is the continuo-contracts version that stored the contracts, and the
    document names the ODCS schema they were checked against. The result shares nothing
    with its inputs.
    """
    by_id: dict[str, dict[str, Any]] = {}
    for contract in contracts:
        if contract.unique_id in by_id:
            raise ValueError(f"two contracts of the release carry the unique id {contract.unique_id}")
        by_id[contract.unique_id] = {
            "family": _FAMILY,
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
        "ontology": None,
        "provenance": copy.deepcopy(dict(provenance)),
        "summary": {
            "contracts": len(by_id),
            "by_family": {_FAMILY: len(by_id)} if by_id else {},
            "invalid": 0,
            "skipped": 0,
        },
        "failures": [],
        "timings": copy.deepcopy(dict(timings)),
        "contracts": by_id,
    }
