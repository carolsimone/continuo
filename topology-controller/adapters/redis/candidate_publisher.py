"""Publishes manifest.loaded.candidate:v2 entries back to release-controller.

Each entry carries the envelope (see adapters/redis/envelope.py) and a payload:
publish_ok a reference to the release's topology artifact (its URI, the
SHA-256 of its stored bytes and its node count), never the nodes themselves;
publish_failed the failure kind, a summary detail and the per-node failures.
release-controller decodes both with pkg/events.DecodeManifestLoadedCandidate,
and tests/fixtures/manifest_loaded_candidate_v2_{ok,failed}.json pin the two
sides to the same entries.
"""
import logging
import re
import uuid
from collections.abc import Callable, Sequence
from datetime import datetime, timezone

from adapters.redis.envelope import EVENT_ID_NAMESPACE, envelope_fields
from domain.contract_vocabulary import ParseFailureKind
from domain.model import DEFAULT_TENANT_ID, FailedNode, TopologyArtifactRef

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

# sqlglot underlines the offending token with terminal escape sequences; they
# must not reach a JSON payload another service stores and renders.
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    return _ANSI_ESCAPE.sub("", text)


def manifest_loaded_candidate_event_id(tenant_id: str, release_id: str) -> str:
    """Deterministic id of one release's parse result: a redelivered
    release.requested re-publishes its result under the same id."""
    name = "|".join(["manifest.loaded.candidate", tenant_id, release_id])
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, name))


class CandidateManifestPublisher:
    def __init__(
        self,
        redis_client,
        stream_name: str,
        *,
        producer: str,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._redis = redis_client
        self._stream = stream_name
        self._producer = producer
        self._clock = clock

    def publish_ok(self, *, release_id: str, artifact: TopologyArtifactRef, code_bundle_uri: str) -> None:
        self._xadd(release_id, {
            "release_id": release_id,
            "status": "ok",
            "topology_uri": artifact.uri,
            "topology_sha256": artifact.sha256,
            "node_count": artifact.node_count,
            "code_bundle_uri": code_bundle_uri,
        })
        logger.info(
            "Published manifest.loaded.candidate ok",
            extra={"release_id": release_id, "node_count": artifact.node_count, "topology_uri": artifact.uri},
        )

    def publish_failed(
        self,
        release_id: str,
        failure_kind: ParseFailureKind,
        detail: str,
        failed_nodes: Sequence[FailedNode] = (),
    ) -> None:
        body = {
            "release_id": release_id,
            "status": "failed",
            "failure_kind": failure_kind.value,
            "detail": _plain(detail),
            "failed_nodes": [
                {
                    "node_id": n.node_id,
                    "kind": n.kind.value,
                    "service": n.service,
                    "file_path": n.file_path,
                    "node_type": str(n.node_type),
                    "detail": _plain(n.detail),
                }
                for n in failed_nodes
            ],
        }
        self._xadd(release_id, body)
        logger.error(
            "Published manifest.loaded.candidate failed",
            extra={
                "release_id": release_id,
                "failure_kind": failure_kind.value,
                "detail": body["detail"],
                "failed_nodes": len(body["failed_nodes"]),
            },
        )

    def _xadd(self, release_id: str, payload: dict) -> None:
        fields = envelope_fields(
            event_id=manifest_loaded_candidate_event_id(DEFAULT_TENANT_ID, release_id),
            tenant_id=DEFAULT_TENANT_ID,
            occurred_at=self._clock(),
            producer=self._producer,
            schema_version=SCHEMA_VERSION,
            payload=payload,
        )
        self._redis.xadd(self._stream, fields)
