"""Test doubles and handler builders shared by the candidate-handler test modules."""
import io
import json
from unittest.mock import MagicMock, create_autospec

import botocore.exceptions

from adapters.redis.error_class import is_infrastructure
from config.config import _SYSTEM_RELATIONS
from domain.model import ContractBundleRef, ManifestFile, Runtime, TopologyArtifactRef
from service.candidate_artifacts import DbtSqlArtifactBuilder, PythonSpecArtifactBuilder
from service.candidate_manifest_handler import CandidateManifestHandler
from service.contract_validation import ContractValidator
from service.parser import parse_manifest as _parse_manifest
from service.ports import ManifestSourcePort, NoReleaseObserver
from service.release_timings import ReleaseTimings


class RecordingArtifactWriter:
    """A TopologyArtifactWriterPort that records every write and returns a
    reference shaped like the real writer's. Pass `fail` to make write raise
    it instead, and `log` to have each write append "topology_artifact" to a
    list the test shares with the other recording fakes."""

    def __init__(self, fail: BaseException | None = None, log: list[str] | None = None) -> None:
        self.writes: list[dict] = []
        self.refs: list[TopologyArtifactRef] = []
        self._fail = fail
        self._log = log

    def write(self, *, tenant_id: str, release_id: str, nodes: list[dict]) -> TopologyArtifactRef:
        if self._fail is not None:
            raise self._fail
        if self._log is not None:
            self._log.append("topology_artifact")
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


class RecordingContractBundleWriter:
    """A ContractBundleWriterPort that records every write and returns a
    reference shaped like the real writer's. Pass `fail` to make write raise
    it instead, and `log` to have each write append "contract_bundle" to a
    list the test shares with the other recording fakes."""

    def __init__(self, fail: BaseException | None = None, log: list[str] | None = None) -> None:
        self.writes: list[dict] = []
        self.refs: list[ContractBundleRef] = []
        self._fail = fail
        self._log = log

    def write(self, *, tenant_id: str, release_id: str, document: dict) -> ContractBundleRef:
        if self._fail is not None:
            raise self._fail
        if self._log is not None:
            self._log.append("contract_bundle")
        self.writes.append({"tenant_id": tenant_id, "release_id": release_id, "document": document})
        ref = ContractBundleRef(
            uri=f"s3://continuo/tenants/{tenant_id}/topologies/{release_id}/contracts.json.gz",
            sha256=f"{len(self.writes):064x}",
            by_family=dict(document["summary"]["by_family"]),
        )
        self.refs.append(ref)
        return ref

    @property
    def document(self) -> dict:
        """The document of the one bundle this writer stored."""
        assert len(self.writes) == 1, f"expected one contract bundle write, got {len(self.writes)}"
        return self.writes[0]["document"]


def make_uploader(uri=""):
    uploader = MagicMock()
    uploader.upload.return_value = uri
    return uploader


class FakeBundleUploader:
    """Records (release_id, bundle) calls and returns a canned URI.

    Pass `fail` to make `upload` raise instead — the failing variant used to
    exercise the CodeBundleUploadFailed path — and `log` to have each upload
    append "code_bundle" to a list the test shares with the other recording
    fakes.
    """

    def __init__(self, uri: str = "s3://continuo/code-bundles/rel-1/bundle.json", fail: Exception | None = None,
                 log: list[str] | None = None):
        self.uploads: list[tuple[str, dict]] = []
        self._uri = uri
        self._fail = fail
        self._log = log

    def upload(self, release_id: str, bundle: dict) -> str:
        if self._fail is not None:
            raise self._fail
        if self._log is not None:
            self._log.append("code_bundle")
        self.uploads.append((release_id, bundle))
        return self._uri


def build_handler(source, publisher, uploader, bundle_uploader=None, dialect="postgres",
                  image_tags=None, artifact_writer=None, contract_bundle_writer=None,
                  system_relations=_SYSTEM_RELATIONS["postgres"], clock=None, memory=None,
                  observer=None, validator=None) -> CandidateManifestHandler:
    """Build a handler pinned to the postgres dialect and its system relations,
    a fake bundle uploader, a recording artifact writer, a recording contract
    bundle writer, an empty image-tag map and no release observer unless a test
    names otherwise. Outages are recognised by the consumer's own classifier.

    `uploader` is the dbt candidate-SQL uploader; it is wrapped in the dbt
    artifact builder here so these cases keep asserting directly on the upload
    calls the builder makes.
    """
    return CandidateManifestHandler(
        source=source,
        publisher=publisher,
        bundle_uploader=bundle_uploader if bundle_uploader is not None else FakeBundleUploader(),
        artifact_writer=artifact_writer if artifact_writer is not None else RecordingArtifactWriter(),
        contract_bundle_writer=(
            contract_bundle_writer if contract_bundle_writer is not None else RecordingContractBundleWriter()),
        artifact_builders={Runtime.DBT: DbtSqlArtifactBuilder(uploader)},
        dialect=dialect,
        system_relations=system_relations,
        image_tags=image_tags if image_tags is not None else {},
        is_infrastructure_error=is_infrastructure,
        clock=clock or FakeClock(),
        memory=memory or ScriptedMemoryProbe(0),
        validator=validator if validator is not None else ContractValidator(),
        observer=observer if observer is not None else NoReleaseObserver(),
    )


def dispatch_handler(source, publisher, artifact_builders=None, dialect="postgres", artifact_writer=None,
                     system_relations=_SYSTEM_RELATIONS["postgres"], clock=None, memory=None, progress=None,
                     contract_bundle_writer=None, bundle_uploader=None, validator=None):
    """A handler for the parse-dispatch cases.

    These cases all fail (or are rejected) before any node reaches the artifact
    builders, so the default map carries dbt only; tests that publish a python
    node pass their own map.
    """
    return CandidateManifestHandler(
        source=source,
        publisher=publisher,
        bundle_uploader=bundle_uploader if bundle_uploader is not None else FakeBundleUploader(),
        artifact_writer=artifact_writer if artifact_writer is not None else RecordingArtifactWriter(),
        contract_bundle_writer=(
            contract_bundle_writer if contract_bundle_writer is not None else RecordingContractBundleWriter()),
        artifact_builders=artifact_builders or {Runtime.DBT: DbtSqlArtifactBuilder(make_uploader())},
        dialect=dialect,
        system_relations=system_relations,
        image_tags={},
        is_infrastructure_error=is_infrastructure,
        clock=clock or FakeClock(),
        memory=memory or ScriptedMemoryProbe(0),
        validator=validator if validator is not None else ContractValidator(),
        progress=progress or (lambda: None),
    )


def python_handler(source, publisher, spec_uploader=None, dialect="postgres", artifact_writer=None,
                   system_relations=_SYSTEM_RELATIONS["postgres"], progress=None, contract_bundle_writer=None,
                   bundle_uploader=None, clock=None, memory=None, validator=None):
    """A handler wired for both kinds, for cases that publish a python node."""
    return dispatch_handler(
        source, publisher, dialect=dialect, artifact_writer=artifact_writer,
        system_relations=system_relations, progress=progress, contract_bundle_writer=contract_bundle_writer,
        bundle_uploader=bundle_uploader, clock=clock, memory=memory, validator=validator,
        artifact_builders={
            Runtime.DBT: DbtSqlArtifactBuilder(make_uploader()),
            Runtime.PYTHON: PythonSpecArtifactBuilder(
                spec_uploader if spec_uploader is not None
                else make_uploader("s3://continuo/candidate-sql/rel-1/candidate_test_schema.py_metrics.json")),
        },
    )


def parse_manifest(manifest_path, *args, validator=None, timings=None, **kwargs):
    """`service.parser.parse_manifest` for the parse cases: each call gets a fresh validator and a
    fresh timing record unless the test passes its own."""
    return _parse_manifest(
        manifest_path, *args,
        validator=validator if validator is not None else ContractValidator(),
        timings=timings if timings is not None else ReleaseTimings("rel-1", FakeClock(), ScriptedMemoryProbe(0)),
        **kwargs,
    )


def dbt_node(name, *, original_file_path=""):
    """A minimal valid dbt model node, for manifests built inline where a test
    needs more than one node or a node of its own. It carries the
    `config.materialized` and `original_file_path` dbt writes for every node
    (`models/<name>.sql` unless the test names another path)."""
    return {
        "unique_id": f"model.service-1.{name}",
        "name": name,
        "schema": "test_schema",
        "fqn": ["service-1", name],
        "tags": ["daily"],
        "resource_type": "model",
        "original_file_path": original_file_path or f"models/{name}.sql",
        "config": {"materialized": "table", "meta": {"owner": "team-a", "criticality": "CORE"}},
        "compiled_code": "SELECT 1 AS id",
        "checksum": {"name": "sha256", "checksum": f"hash-{name}"},
    }


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


def client_error(code: str, status: int, operation: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, operation)


class FakeS3:
    """The two calls the contract bundle writer makes, over a dict. A key that is not there answers
    the way S3 does: a ClientError whose code is NoSuchKey. A put with
    IfNoneMatch="*" on a key that is there answers 412 PreconditionFailed and
    stores nothing. before_put runs before each put is applied, so a test can
    store another writer's bytes between the writer's read and its put; put_errors
    are raised by the puts in order, one each, before anything is stored."""

    def __init__(
        self,
        *,
        get_error: Exception | None = None,
        put_error: Exception | None = None,
        put_errors: list[Exception] | None = None,
        before_put=None,
    ) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.puts: list[dict] = []
        self.gets: list[dict] = []
        self._get_error = get_error
        self._put_error = put_error
        self._put_errors = list(put_errors or [])
        self._before_put = before_put

    def get(self, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        self.gets.append({"Bucket": Bucket, "Key": Key})
        if self._get_error is not None:
            raise self._get_error
        if (Bucket, Key) not in self.objects:
            raise client_error("NoSuchKey", 404, "GetObject")
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **kwargs) -> dict:
        self.puts.append({"Bucket": Bucket, "Key": Key, "Body": Body, **kwargs})
        if self._before_put is not None:
            self._before_put(self)
        if self._put_error is not None:
            raise self._put_error
        if self._put_errors:
            raise self._put_errors.pop(0)
        if kwargs.get("IfNoneMatch") == "*" and (Bucket, Key) in self.objects:
            raise client_error("PreconditionFailed", 412, "PutObject")
        self.objects[(Bucket, Key)] = Body
        return {}
