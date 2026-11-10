"""Application-layer ports for collaborators that are not domain concepts.

The adapters under adapters/* implement these structurally; the application
depends on the protocol, never on the concrete class, so the dependency arrow
runs adapter -> port. Ports the conformance test checks (test_adapters_conform
_to_ports.py) are runtime_checkable so a renamed adapter method is caught.
"""
from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from domain.model import FailedNode, ManifestFile, TopologyArtifactRef
from domain.contract_vocabulary import ParseFailureKind

if TYPE_CHECKING:
    # service.release_timings imports this module for the clock and memory
    # ports, so the record is named for type checkers only.
    from service.release_timings import ReleaseTimings


class CandidateSqlUploaderPort(Protocol):
    def upload(self, release_id: str, unique_id: str, sql: str) -> str:
        """Store a node's rewritten candidate SQL and return its s3:// URI."""
        ...


class CandidateSpecUploaderPort(Protocol):
    def upload(self, release_id: str, unique_id: str, spec: dict) -> str:
        """Store a node's validation spec document and return its s3:// URI."""
        ...


@runtime_checkable
class CodeBundleUploaderPort(Protocol):
    def upload(self, release_id: str, bundle: dict) -> str:
        """Store a release's code-bundle document and return its s3:// URI."""
        ...


@runtime_checkable
class TopologyArtifactWriterPort(Protocol):
    def write(self, *, tenant_id: str, release_id: str, nodes: list[dict]) -> TopologyArtifactRef:
        """Store a release's topology artifact and return where it lives, the
        SHA-256 of its stored bytes, and how many nodes it holds."""
        ...


@runtime_checkable
class CandidatePublisherPort(Protocol):
    """Publishes the outcome of a candidate-parse attempt back to
    release-controller: publish_ok carries the reference to the release's
    topology artifact, publish_failed a typed failure kind, a summary detail,
    and one FailedNode per node the parse could not resolve (empty for
    artifact and internal failures)."""

    def publish_ok(self, *, release_id: str, artifact: TopologyArtifactRef, code_bundle_uri: str) -> None:
        ...

    def publish_failed(
        self,
        release_id: str,
        failure_kind: ParseFailureKind,
        detail: str,
        failed_nodes: Sequence[FailedNode] = (),
    ) -> None:
        ...


@runtime_checkable
class ManifestSourcePort(Protocol):
    """Supplies the release's manifest files to the parse handler and owns the
    temporary storage they were fetched into."""

    def manifest_count(self) -> int:
        """How many manifests `list_manifests` will fetch."""
        ...

    def list_manifests(self) -> list[ManifestFile]:
        ...

    def cleanup(self) -> None:
        """Release any temporary resources acquired to serve the manifests."""
        ...


@runtime_checkable
class ClockPort(Protocol):
    """A monotonic clock the release stage timer reads."""

    def now(self) -> float:
        """Monotonic seconds: only the difference between two readings means anything."""
        ...


@runtime_checkable
class MemoryProbePort(Protocol):
    """Reads the memory the process holds, for the release stage timer."""

    def resident_bytes(self) -> int:
        """The process's resident set, in bytes."""
        ...


@runtime_checkable
class ReleaseObserverPort(Protocol):
    """Told how each release ended, so an adapter can turn the record into
    metrics. The handler tells it once per release that ended, published or
    failed, and never for one that raised."""

    def release_finished(self, timings: "ReleaseTimings") -> None:
        """Take the record of a release that ended: its outcome is set."""
        ...


class NoReleaseObserver:
    """The observer of a handler nobody measures: it ignores every release."""

    def release_finished(self, timings: "ReleaseTimings") -> None:
        pass
