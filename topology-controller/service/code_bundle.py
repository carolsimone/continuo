"""Builds the code-bundle contract document (contract_version 1).

One bundle per release, uploaded to S3 by the candidate handler; downstream
consumers read this contract instead of ever parsing a dbt manifest. Nodes are
keyed by continuo unique_id (schema_name.table_name) and each carries a
`runtime` marker, so non-dbt runtimes can share the same document shape.
"""
from domain.model import ManifestNode

CONTRACT_VERSION = 1


def build_code_bundle(release_id: str, nodes: list[ManifestNode], shared_code: dict) -> dict:
    return {
        "contract_version": CONTRACT_VERSION,
        "release_id": release_id,
        "nodes": {
            n.unique_id: {
                "runtime": n.runtime,
                "raw_code": n.raw_code,
                "compiled_code": n.candidate_sql,
                "config": n.config,
                "source_hash": n.source_hash,
                "shared_code_hash": n.shared_code_hash,
                "config_hash": n.config_hash,
                "content_hash": n.content_hash,
                "code_unit_ids": n.code_unit_ids,
            }
            for n in nodes
        },
        "shared_code": shared_code,
    }
