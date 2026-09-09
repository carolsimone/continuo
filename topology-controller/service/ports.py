"""Application-layer ports for collaborators that are not domain concepts.

The adapters under adapters/* implement these structurally; the application
depends on the protocol, never on the concrete class, so the dependency arrow
runs adapter -> port. Ports the conformance test checks (test_adapters_conform
_to_ports.py) are runtime_checkable so a renamed adapter method is caught.
"""
from typing import Protocol, runtime_checkable

from domain.model import ManifestFile


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
class CandidatePublisherPort(Protocol):
    """Publishes the outcome of a candidate-parse attempt back to
    release-controller: publish_ok carries the resolved topology, publish_failed
    an error class and detail."""

    def publish_ok(
        self, release_id: str, topology: list[dict], code_bundle_uri: str = ""
    ) -> None:
        ...

    def publish_failed(
        self, release_id: str, error_class: str, error_detail: str
    ) -> None:
        ...


@runtime_checkable
class ManifestSourcePort(Protocol):
    """Supplies the release's manifest files to the parse handler and owns the
    temporary storage they were fetched into."""

    def list_manifests(self) -> list[ManifestFile]:
        ...

    def cleanup(self) -> None:
        """Release any temporary resources acquired to serve the manifests."""
        ...
