import json
import logging

from adapters.redis.constants import STREAM_MAXLEN

logger = logging.getLogger(__name__)


class ManifestLoadedPublisher:
    """Publishes manifest.loaded:v1 events containing topology nodes to a Redis stream."""

    def __init__(self, redis_client, stream_name: str) -> None:
        self._redis = redis_client
        self._stream = stream_name

    def publish(self, nodes: list[dict]) -> None:
        payload = json.dumps(nodes)
        self._redis.xadd(self._stream, {"payload": payload}, maxlen=STREAM_MAXLEN)
        logger.info(
            "Published manifest.loaded event",
            extra={"node_count": len(nodes)},
        )
