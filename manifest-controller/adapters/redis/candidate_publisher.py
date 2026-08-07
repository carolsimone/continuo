import json
import logging

from adapters.redis.constants import STREAM_MAXLEN

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
