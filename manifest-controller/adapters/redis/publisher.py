import json
import logging

logger = logging.getLogger(__name__)


class SchedulesLoadedPublisher:
    """Publishes schedules.loaded:v1 events to a Redis stream."""

    def __init__(self, redis_client, stream_name: str) -> None:
        self._redis = redis_client
        self._stream = stream_name

    def publish(
        self,
        event_id: str,
        schedule_names: list[str],
        manifest_versions: dict[str, str],
    ) -> None:
        payload = json.dumps({
            "event_id": event_id,
            "schedule_names": schedule_names,
            "manifest_versions": manifest_versions,
        })
        self._redis.xadd(self._stream, {"payload": payload})
        logger.info(
            "Published schedules.loaded event",
            extra={"event_id": event_id, "schedule_count": len(schedule_names)},
        )
