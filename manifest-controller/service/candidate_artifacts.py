"""Per-node candidate artifact: the object this node's validation Job fetches.

Blue/green validation builds each changed node as an empty table in an isolated
candidate schema. What it needs in order to do that differs by runtime — a dbt
node has compiled SQL to materialize empty, a python node has no SELECT at all
and declares its output columns instead — but the topology carries the same one
key either way, because node_type already says which shape the object has.

Each builder owns its whole step: rewrite to the candidate schema, upload, and
return the topology keys that reference the result.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass

from domain.model import ManifestNode, NodeRegistryEntry
from service.ports import CandidateSpecUploaderPort, CandidateSqlUploaderPort
from service.rewriter import rewrite_to_candidate_schema


@dataclass(frozen=True)
class RewriteContext:
    """Everything a builder needs about the release it is building for.

    dialect is the sqlglot dialect of the warehouse this install targets,
    resolved once at boot by the composition root; it decides the syntax of
    every SQL string a builder emits.
    """
    release_id: str
    registry: dict[tuple[str, str], NodeRegistryEntry]
    candidate_schema: str
    dialect: str


class CandidateArtifactBuilder(ABC):
    @abstractmethod
    def build(self, node: ManifestNode, ctx: RewriteContext) -> dict[str, str]:
        """Upload this node's validation input and return the topology keys
        referencing it. Raises on upload failure; the caller treats that as
        fatal for the release rather than publish a dangling reference."""


class DbtSqlArtifactBuilder(CandidateArtifactBuilder):
    """dbt nodes: the compiled SELECT, rewritten to the candidate schema.

    Seeds carry no candidate SQL; the uploader stores nothing for them and
    yields an empty URI.
    """

    def __init__(self, uploader: CandidateSqlUploaderPort) -> None:
        self._uploader = uploader

    def build(self, node: ManifestNode, ctx: RewriteContext) -> dict[str, str]:
        candidate_sql = rewrite_to_candidate_schema(
            node.candidate_sql, ctx.registry, ctx.candidate_schema,
            self_schema=node.schema_name, self_table=node.table_name,
            dialect=ctx.dialect,
        )
        uri = self._uploader.upload(
            release_id=ctx.release_id,
            unique_id=node.unique_id,
            sql=candidate_sql,
        )
        return {"candidate_artifact_uri": uri}


class PythonSpecArtifactBuilder(CandidateArtifactBuilder):
    """python nodes: a validation spec, because there is no SELECT to shape the
    output from.

    Reads are rewritten with the same self-reference exclusion dbt uses: the
    validation Job bind-checks the reads BEFORE creating the node's own empty
    table, so a self-reference redirected to the candidate schema would bind
    against a relation that does not exist yet.
    """

    def __init__(self, uploader: CandidateSpecUploaderPort) -> None:
        self._uploader = uploader

    def build(self, node: ManifestNode, ctx: RewriteContext) -> dict[str, str]:
        reads = [
            rewrite_to_candidate_schema(
                sql, ctx.registry, ctx.candidate_schema,
                self_schema=node.schema_name, self_table=node.table_name,
                dialect=ctx.dialect,
            )
            for sql in node.dependency_sqls
        ]
        uri = self._uploader.upload(
            release_id=ctx.release_id,
            unique_id=node.unique_id,
            spec={
                "reads": reads,
                "output_columns": node.output_columns,
                "config": node.config,
            },
        )
        return {"candidate_artifact_uri": uri}
