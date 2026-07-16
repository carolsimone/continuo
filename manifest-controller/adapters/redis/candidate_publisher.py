import json
import logging

from adapters.redis.constants import STREAM_MAXLEN
from domain.model import RuntimeManifestRef

logger = logging.getLogger(__name__)


class CandidateManifestPublisher:
    """Publishes manifest.loaded.candidate:v1 events back to release-controller.

    Two methods reflect the two business outcomes of a parse attempt:
    publish_ok carries the resolved topology; publish_failed carries an
    error class and detail. Both write a single 'payload' field with a
    JSON body, matching the wire format expected by release-controller's
    manifest.loaded.candidate handler.
    """

    def __init__(self, redis_client, stream_name: str) -> None:
        self._redis = redis_client
        self._stream = stream_name

    def publish_ok(
        self,
        release_id: str,
        topology: list[dict],
        runtime_manifests: dict[str, RuntimeManifestRef],
    ) -> None:
        """Publish the resolved topology plus, per service, the runtime manifest
        its nodes execute against. Services whose release carries no runtime
        manifest are simply absent from runtime_manifests."""
        body = {
            "release_id": release_id,
            "status": "ok",
            "topology": topology,
            "runtime_manifests": {
                service: ref.to_wire() for service, ref in runtime_manifests.items()
            },
        }
        self._redis.xadd(self._stream, {"payload": json.dumps(body)}, maxlen=STREAM_MAXLEN)
        logger.info(
            "Published manifest.loaded.candidate ok",
            extra={
                "release_id": release_id,
                "node_count": len(topology),
                "runtime_manifest_count": len(runtime_manifests),
            },
        )

    def publish_failed(self, release_id: str, error_class: str, error_detail: str) -> None:
        body = {
            "release_id": release_id,
            "status": "failed",
            "error_class": error_class,
            "error_detail": error_detail,
        }
        self._redis.xadd(self._stream, {"payload": json.dumps(body)}, maxlen=STREAM_MAXLEN)
        logger.error(
            "Published manifest.loaded.candidate failed",
            extra={"release_id": release_id, "error_class": error_class, "error_detail": error_detail},
        )
