"""One dbt Manifest, hydrated once per worker process from a pinned artifact.

This is what makes a worker cheaper than a Job: the Manifest is loaded from a
partial parse the release already produced, so no task re-parses the project.

Nothing here falls back to parsing. A worker that cannot prove the artifact is
the one its pool was pinned to, and that it was parsed under conditions this
process reproduces, fails and stays unready. A silent re-parse would produce a
Manifest that disagrees with the release the run is pinned to, which is the
failure this whole path exists to prevent.
"""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dbt.contracts.graph.manifest import Manifest

from continuo_dbt_runtime.descriptor import validate_descriptor
from continuo_dbt_runtime.parse_context import parse_context_sha256

if TYPE_CHECKING:  # pragma: no cover - import cycle broken for type checkers only
    from continuo_dbt_runtime.worker import WorkerConfig

# The adapter this image can execute. The artifact names the adapter it was
# parsed for, and this image ships exactly one.
SUPPORTED_ADAPTER = "postgres"

# Resource types a service's own work is made of. A manifest carrying none of
# them for this service is not this service's manifest.
EXECUTABLE_RESOURCE_TYPES = frozenset({"model", "seed", "snapshot"})

# How long a download may take. An artifact is a few megabytes over a signed URL.
DOWNLOAD_TIMEOUT_SECONDS = 120.0


class InitializationError(Exception):
    """A reason this worker will not serve its pool's artifact.

    The code is the stable class the executor records; the message is for an
    operator reading logs.
    """

    def __init__(self, code: str, message: str = ""):
        self.code = code
        super().__init__(message or code)


@dataclass(frozen=True)
class LoadedArtifact:
    manifest: Manifest
    canonical_path: Path
    descriptor: dict


def _redacted(url: str) -> str:
    """A signed URL with its query dropped.

    The query of a presigned URL is the capability itself, so it never reaches
    an exception or a log line.
    """
    parts = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def download_bytes(url: str) -> bytes:
    """Read a signed URL, without putting its signature in the failure."""
    try:
        with urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
            return response.read()
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
        reason = getattr(exc, "code", None) or getattr(exc, "reason", exc.__class__.__name__)
        raise RuntimeError(f"download failed for {_redacted(url)}: {reason}") from None


def download_json(url: str) -> dict:
    return json.loads(download_bytes(url))


class ArtifactStore:
    """The pool's artifact, fetched and checked once."""

    def __init__(self, config: "WorkerConfig", client):
        self._config = config
        self._client = client
        self._loaded: LoadedArtifact | None = None

    def load(self) -> LoadedArtifact:
        if self._loaded is not None:
            return self._loaded

        runtime = self._client.runtime()
        descriptor = self._descriptor(runtime)
        packed = self._artifact(runtime, descriptor)
        manifest = self._hydrate(packed)
        self._check_service_nodes(manifest)
        self._check_parse_context(manifest, descriptor)

        self._config.cache_dir.mkdir(parents=True, exist_ok=True)
        canonical = self._config.cache_dir / "partial_parse.msgpack"
        canonical.write_bytes(packed)
        self._loaded = LoadedArtifact(manifest, canonical, descriptor)
        return self._loaded

    def _descriptor(self, runtime: dict) -> dict:
        """The descriptor, checked against what this pool was pinned to.

        The pinned digest is the only check that binds the artifact to this
        pool. Every other check is self-consistent: a descriptor written for a
        different release of the same service and image satisfies all of them.
        """
        try:
            descriptor = download_json(runtime["descriptor_url"])
        except (KeyError, json.JSONDecodeError) as exc:
            raise InitializationError(
                "runtime_manifest_rejected", f"descriptor is unreadable: {exc}"
            ) from None
        try:
            validate_descriptor(
                descriptor,
                expected_service=self._config.service_name,
                expected_image_tag=self._config.image_tag,
                expected_sha256=self._config.runtime_manifest_sha256,
            )
        except RuntimeError as exc:
            raise InitializationError("runtime_manifest_rejected", str(exc)) from None
        if descriptor["adapter_type"] != SUPPORTED_ADAPTER:
            raise InitializationError(
                "runtime_manifest_adapter_mismatch",
                f"artifact was parsed for {descriptor['adapter_type']!r}, "
                f"this image runs {SUPPORTED_ADAPTER!r}",
            )
        return descriptor

    def _artifact(self, runtime: dict, descriptor: dict) -> bytes:
        packed = download_bytes(runtime["artifact_url"])
        if hashlib.sha256(packed).hexdigest() != descriptor["sha256"]:
            raise InitializationError(
                "runtime_manifest_checksum_mismatch",
                "the downloaded artifact is not the one the descriptor names",
            )
        installed = importlib.metadata.version("dbt-core")
        if installed != descriptor["dbt_core_version"]:
            raise InitializationError(
                "runtime_manifest_dbt_version_mismatch",
                f"artifact was written by dbt-core {descriptor['dbt_core_version']}, "
                f"this image runs {installed}",
            )
        return packed

    def _hydrate(self, packed: bytes) -> Manifest:
        try:
            return Manifest.from_msgpack(packed)
        except Exception as exc:
            raise InitializationError(
                "runtime_manifest_unreadable", f"partial parse did not load: {exc}"
            ) from None

    def _check_service_nodes(self, manifest: Manifest) -> None:
        """A node's fqn starts with its dbt project, which names the service."""
        for node in manifest.nodes.values():
            if (
                node.resource_type in EXECUTABLE_RESOURCE_TYPES
                and node.fqn
                and node.fqn[0].replace("_", "-") == self._config.service_name
            ):
                return
        raise InitializationError(
            "runtime_manifest_service_nodes_missing",
            f"artifact carries no runnable node for {self._config.service_name!r}",
        )

    def _check_parse_context(self, manifest: Manifest, descriptor: dict) -> None:
        """Refuse an artifact this process would not have parsed the same way."""
        actual = parse_context_sha256(manifest, self._config.controller_context_json)
        if actual != descriptor["parse_context_sha256"]:
            raise InitializationError(
                "runtime_manifest_parse_context_mismatch",
                "this worker's parse context differs from the one the artifact "
                "was produced under",
            )
