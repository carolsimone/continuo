"""Per-node candidate artifact: the object this node's validation Job fetches.

Blue/green validation builds each changed node as an empty table in an isolated
candidate schema. What it needs in order to do that differs by runtime — a dbt
node has compiled SQL to materialize empty, a python node has no SELECT at all
and declares its output columns instead. Either way the object lives at the key
release-controller derives from the release id, the node's unique_id and its
node_type (pkg/topologyartifact.CandidateObjectKey), so the topology carries no
reference to it.

Each builder owns its whole step: rewrite to the candidate schema and upload.
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
    def build(self, node: ManifestNode, ctx: RewriteContext) -> None:
        """Upload this node's validation input. Raises on upload failure; the
        caller never publishes a release whose inputs did not land."""


class DbtSqlArtifactBuilder(CandidateArtifactBuilder):
    """dbt nodes: the compiled SELECT, rewritten to the candidate schema.

    Seeds carry no candidate SQL; the uploader stores nothing for them, and
    release-controller derives no key for a seed.
    """

    def __init__(self, uploader: CandidateSqlUploaderPort) -> None:
        self._uploader = uploader

    def build(self, node: ManifestNode, ctx: RewriteContext) -> None:
        candidate_sql = rewrite_to_candidate_schema(
            node.candidate_sql, ctx.registry, ctx.candidate_schema,
            self_schema=node.schema_name, self_table=node.table_name,
            dialect=ctx.dialect,
        )
        self._uploader.upload(
            release_id=ctx.release_id,
            unique_id=node.unique_id,
            sql=candidate_sql,
        )


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

    def build(self, node: ManifestNode, ctx: RewriteContext) -> None:
        reads = [
            rewrite_to_candidate_schema(
                sql, ctx.registry, ctx.candidate_schema,
                self_schema=node.schema_name, self_table=node.table_name,
                dialect=ctx.dialect,
            )
            for sql in node.dependency_sqls
        ]
        spec = {
            "reads": reads,
            "output_columns": node.output_columns,
            "config": node.config,
        }
        if node.csv_source:
            # Not schema-rewritten: the uri is a file location, not a
            # warehouse reference. The runner range-fetches its header and
            # checks it against output_columns before building the table.
            spec["csv_source"] = node.csv_source
        self._uploader.upload(
            release_id=ctx.release_id,
            unique_id=node.unique_id,
            spec=spec,
        )
