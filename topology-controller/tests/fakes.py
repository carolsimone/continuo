"""Test doubles and handler builders shared by the candidate-handler test modules."""
import json
from unittest.mock import MagicMock, create_autospec

import yaml
from continuo_contracts.hashing import fold3
from continuo_contracts.python import PythonNode, projection_hash

from adapters.redis.error_class import is_infrastructure
from config.config import _SYSTEM_RELATIONS
from domain.model import ManifestFile, Runtime, TopologyArtifactRef
from service.candidate_artifacts import DbtSqlArtifactBuilder, PythonSpecArtifactBuilder
from service.candidate_manifest_handler import CandidateManifestHandler
from service.ports import ManifestSourcePort, NoReleaseObserver


class RecordingArtifactWriter:
    """A TopologyArtifactWriterPort that records every write and returns a
    reference shaped like the real writer's. Pass `fail` to make write raise
    it instead."""

    def __init__(self, fail: BaseException | None = None) -> None:
        self.writes: list[dict] = []
        self.refs: list[TopologyArtifactRef] = []
        self._fail = fail

    def write(self, *, tenant_id: str, release_id: str, nodes: list[dict]) -> TopologyArtifactRef:
        if self._fail is not None:
            raise self._fail
        self.writes.append({"tenant_id": tenant_id, "release_id": release_id, "nodes": list(nodes)})
        ref = TopologyArtifactRef(
            uri=f"s3://continuo/tenants/{tenant_id}/topologies/{release_id}/topology.json.gz",
            sha256=f"{len(self.writes):064x}",
            node_count=len(nodes),
        )
        self.refs.append(ref)
        return ref

    @property
    def nodes(self) -> list[dict]:
        """The nodes of the one artifact this writer stored."""
        assert len(self.writes) == 1, f"expected one artifact write, got {len(self.writes)}"
        return self.writes[0]["nodes"]


def make_uploader(uri=""):
    uploader = MagicMock()
    uploader.upload.return_value = uri
    return uploader


class FakeBundleUploader:
    """Records (release_id, bundle) calls and returns a canned URI.

    Pass `fail` to make `upload` raise instead — the failing variant used to
    exercise the CodeBundleUploadFailed path.
    """

    def __init__(self, uri: str = "s3://continuo/code-bundles/rel-1/bundle.json", fail: Exception | None = None):
        self.uploads: list[tuple[str, dict]] = []
        self._uri = uri
        self._fail = fail

    def upload(self, release_id: str, bundle: dict) -> str:
        if self._fail is not None:
            raise self._fail
        self.uploads.append((release_id, bundle))
        return self._uri


def build_handler(source, publisher, uploader, bundle_uploader=None, dialect="postgres",
                  image_tags=None, artifact_writer=None,
                  system_relations=_SYSTEM_RELATIONS["postgres"], clock=None, memory=None,
                  observer=None) -> CandidateManifestHandler:
    """Build a handler pinned to the postgres dialect and its system relations,
    a fake bundle uploader, a recording artifact writer, an empty image-tag map
    and no release observer unless a test names otherwise. Outages are
    recognised by the consumer's own classifier.

    `uploader` is the dbt candidate-SQL uploader; it is wrapped in the dbt
    artifact builder here so these cases keep asserting directly on the upload
    calls the builder makes.
    """
    return CandidateManifestHandler(
        source=source,
        publisher=publisher,
        bundle_uploader=bundle_uploader if bundle_uploader is not None else FakeBundleUploader(),
        artifact_writer=artifact_writer if artifact_writer is not None else RecordingArtifactWriter(),
        artifact_builders={Runtime.DBT: DbtSqlArtifactBuilder(uploader)},
        dialect=dialect,
        system_relations=system_relations,
        image_tags=image_tags if image_tags is not None else {},
        is_infrastructure_error=is_infrastructure,
        clock=clock or FakeClock(),
        memory=memory or ScriptedMemoryProbe(0),
        observer=observer if observer is not None else NoReleaseObserver(),
    )


def dispatch_handler(source, publisher, artifact_builders=None, dialect="postgres", artifact_writer=None,
                     system_relations=_SYSTEM_RELATIONS["postgres"], clock=None, memory=None):
    """A handler for the parse-dispatch cases.

    These cases all fail (or are rejected) before any node reaches the artifact
    builders, so the default map carries dbt only; tests that publish a python
    node pass their own map.
    """
    return CandidateManifestHandler(
        source=source,
        publisher=publisher,
        bundle_uploader=FakeBundleUploader(),
        artifact_writer=artifact_writer if artifact_writer is not None else RecordingArtifactWriter(),
        artifact_builders=artifact_builders or {Runtime.DBT: DbtSqlArtifactBuilder(make_uploader())},
        dialect=dialect,
        system_relations=system_relations,
        image_tags={},
        is_infrastructure_error=is_infrastructure,
        clock=clock or FakeClock(),
        memory=memory or ScriptedMemoryProbe(0),
    )


def python_handler(source, publisher, spec_uploader=None, dialect="postgres", artifact_writer=None,
                   system_relations=_SYSTEM_RELATIONS["postgres"]):
    """A handler wired for both kinds, for cases that publish a python node."""
    return dispatch_handler(
        source, publisher, dialect=dialect, artifact_writer=artifact_writer,
        system_relations=system_relations,
        artifact_builders={
            Runtime.DBT: DbtSqlArtifactBuilder(make_uploader()),
            Runtime.PYTHON: PythonSpecArtifactBuilder(
                spec_uploader if spec_uploader is not None
                else make_uploader("s3://continuo/candidate-sql/rel-1/candidate_test_schema.py_metrics.json")),
        },
    )


def dbt_node(name, *, original_file_path=""):
    """A minimal valid dbt model node, for manifests built inline where a test
    needs more than one node or a node with a source file_path — neither of
    which manifest_service1.json's single, path-less fixture node has."""
    node = {
        "unique_id": f"model.service-1.{name}",
        "name": name,
        "schema": "test_schema",
        "fqn": ["service-1", name],
        "tags": ["daily"],
        "resource_type": "model",
        "config": {"meta": {"owner": "team-a", "criticality": "CORE"}},
        "compiled_code": "SELECT 1 AS id",
        "checksum": {"name": "sha256", "checksum": f"hash-{name}"},
    }
    if original_file_path:
        node["original_file_path"] = original_file_path
    return node


def source_with_nodes(tmp_path, *nodes):
    """A source whose one manifest holds `nodes`. The manifest carries the
    compile-extras.json in tmp_path when that file exists at the moment
    list_manifests() is called, so a test may write it after building the source."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"nodes": {n["unique_id"]: n for n in nodes}}))
    extras = tmp_path / "compile-extras.json"
    source = create_autospec(ManifestSourcePort)
    source.list_manifests.side_effect = lambda: [
        ManifestFile(path=str(manifest), image_tag="", extras_path=str(extras) if extras.exists() else "")
    ]
    source.manifest_count.return_value = 1
    return source


def python_entry(**overrides):
    """A valid python wire entry, with the hash fields a producer CI on
    continuo-python-runtime 0.12.0 writes: config_hash is the projection hash of
    the (possibly overridden) entry and content_hash the fold of the three
    parts, so the parser computes the same content_hash and logs no difference.
    An overridden hash field stands for what another CI wrote."""
    entry = {
        "schema": "test_schema", "table": "py_metrics",
        "owner": "team-py", "schedule": "daily", "criticality": "SECONDARY",
        "script": "scripts/py_metrics.py",
        "reads": {"orders": "select id from test_schema.orders"},
        "output_columns": [{"name": "id", "type": "INTEGER", "nullable": False}],
        "source_hash": "aaa111", "shared_code_hash": "bbb222",
    }
    entry.update(overrides)
    entry.setdefault("config_hash", projection_hash(PythonNode.from_mapping(entry)))
    entry.setdefault("content_hash", fold3(
        entry["source_hash"], entry["shared_code_hash"], entry["config_hash"]))
    return entry


def python_contract(tmp_path, *entries, service="service-py", name="contract.yaml"):
    doc = {"contract_version": 1, "service": service, "nodes": list(entries)}
    path = tmp_path / name
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return str(path)


def source_of(*files):
    source = create_autospec(ManifestSourcePort)
    source.list_manifests.return_value = list(files)
    source.manifest_count.return_value = len(files)
    return source


class FakeClock:
    """A ClockPort whose time the test controls. `now()` returns `t`, then adds
    `step` to it, so a `step` of 1.0 makes every pair of reads (a stage's entry
    and exit) one second apart."""

    def __init__(self, step: float = 0.0) -> None:
        self.t = 0.0
        self._step = step

    def now(self) -> float:
        reading = self.t
        self.t += self._step
        return reading


class ScriptedMemoryProbe:
    """A MemoryProbePort that returns its readings in turn, then repeats the
    last one. `calls` counts the readings taken."""

    def __init__(self, *readings: int) -> None:
        assert readings, "a probe needs at least one reading"
        self._readings = readings
        self.calls = 0

    def resident_bytes(self) -> int:
        reading = self._readings[min(self.calls, len(self._readings) - 1)]
        self.calls += 1
        return reading
