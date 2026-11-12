"""Builds the code-bundle contract document (contract_version 1).

One bundle per release, uploaded to S3 by the candidate handler; downstream
consumers read this contract instead of ever parsing a dbt manifest. Nodes are
keyed by continuo unique_id (schema_name.table_name) and each carries a
`runtime` marker, so non-dbt runtimes can share the same document shape.

Every node carries the four parts of its content_hash: `compiled_hash` is ""
for a seed and a python node. A dbt node also carries `hashed_config`, the
config its config part hashes, and a python node whose code projection is set
carries it as `projection`, beside its `raw_code` contract entry. The top-level
`tests` map holds each tracked dbt test, keyed by its dbt unique id, with its
compiled SQL and hash parts, and `provenance` holds, per dbt service, the dbt
version, image tag and package versions its compile ran with and the seeds
whose source part is still dbt's `path` checksum.
"""
from domain.model import ManifestNode, Runtime

CONTRACT_VERSION = 1


def _node_entry(n: ManifestNode) -> dict:
    entry = {
        "runtime": n.runtime,
        "raw_code": n.raw_code,
        "compiled_code": n.candidate_sql,
        "config": n.config,
        "source_hash": n.source_hash,
        "shared_code_hash": n.shared_code_hash,
        "config_hash": n.config_hash,
        "compiled_hash": n.compiled_hash,
        "content_hash": n.content_hash,
        "code_unit_ids": n.code_unit_ids,
    }
    if n.runtime == Runtime.DBT and n.hashed_config is not None:
        entry["hashed_config"] = n.hashed_config
    if n.runtime == Runtime.PYTHON and n.projection:
        entry["projection"] = n.projection
    return entry


def _test_entry(t: ManifestNode) -> dict:
    return {
        "compiled_code": t.candidate_sql,
        "source_hash": t.source_hash,
        "shared_code_hash": t.shared_code_hash,
        "config_hash": t.config_hash,
        "compiled_hash": t.compiled_hash,
        "content_hash": t.content_hash,
    }


def build_code_bundle(
    release_id: str,
    nodes: list[ManifestNode],
    shared_code: dict,
    *,
    tests: list[ManifestNode],
    provenance: dict[str, dict],
) -> dict:
    """The bundle document of a release: its published nodes, the shared-code
    units they fold, its tracked dbt tests and each dbt service's provenance."""
    return {
        "contract_version": CONTRACT_VERSION,
        "release_id": release_id,
        "nodes": {n.unique_id: _node_entry(n) for n in nodes},
        "shared_code": shared_code,
        "tests": {t.unique_id: _test_entry(t) for t in tests},
        "provenance": provenance,
    }
