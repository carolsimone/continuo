"""Application-layer ports for collaborators that are not domain concepts.

The adapters under adapters/* implement these structurally; the application
depends on the protocol, never on the concrete class, so the dependency arrow
runs adapter -> port.
"""
from typing import Protocol


class CandidateSqlUploaderPort(Protocol):
    def upload(self, release_id: str, unique_id: str, sql: str) -> str:
        """Store a node's rewritten candidate SQL and return its s3:// URI."""
        ...


class CandidateSpecUploaderPort(Protocol):
    def upload(self, release_id: str, unique_id: str, spec: dict) -> str:
        """Store a node's validation spec document and return its s3:// URI."""
        ...
