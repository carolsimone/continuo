import json
import logging
import re
from collections.abc import Sequence

from adapters.redis.constants import STREAM_MAXLEN
from domain.model import FailedNode
from streams_contract import ParseFailureKind

logger = logging.getLogger(__name__)

# sqlglot underlines the offending token with terminal escape sequences; they
# must not reach a JSON payload another service stores and renders.
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    return _ANSI_ESCAPE.sub("", text)


class CandidateManifestPublisher:
    """Publishes manifest.loaded.candidate:v1 events back to release-controller.

    publish_ok carries the resolved topology; publish_failed carries the
    failure kind, a summary detail, and the per-node failures. Both write a
    single 'payload' field with a JSON body, matching the wire format expected
    by release-controller's manifest.loaded.candidate handler.
    """

    def __init__(self, redis_client, stream_name: str) -> None:
        self._redis = redis_client
        self._stream = stream_name

    def publish_ok(self, release_id: str, topology: list[dict], code_bundle_uri: str = "") -> None:
        body = {
            "release_id": release_id,
            "status": "ok",
            "topology": topology,
            "code_bundle_uri": code_bundle_uri,
        }
        self._redis.xadd(self._stream, {"payload": json.dumps(body)}, maxlen=STREAM_MAXLEN)
        logger.info(
            "Published manifest.loaded.candidate ok",
            extra={"release_id": release_id, "node_count": len(topology)},
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
        self._redis.xadd(self._stream, {"payload": json.dumps(body)}, maxlen=STREAM_MAXLEN)
        logger.error(
            "Published manifest.loaded.candidate failed",
            extra={
                "release_id": release_id,
                "failure_kind": failure_kind.value,
                "failed_nodes": len(body["failed_nodes"]),
            },
        )
