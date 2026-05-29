import logging
import time
import uuid
from collections.abc import Callable
from redis import Redis

logger = logging.getLogger(__name__)

# Only reclaim messages that have been pending longer than this. A live peer
# may legitimately hold a message for the duration of an S3 download + sqlglot
# resolve, so the window is wide enough that reclaim never steals in-flight work.
_RECLAIM_MIN_IDLE_MS = 60_000


class Consumer:
    def __init__(
        self,
        redis_client: Redis,
        stream_name: str,
        group_name: str,
        message_handler: Callable[[dict], None],
    ) -> None:
        self._redis = redis_client
        self._stream = stream_name
        self._group = group_name
        self._name = f"consumer-{uuid.uuid4().hex[:8]}"
        self._message_handler = message_handler
        self._create_group()

    def _create_group(self) -> None:
        try:
            self._redis.xgroup_create(self._stream, self._group, id="0", mkstream=True)
            logger.info("Consumer group created", extra={"group": self._group, "stream": self._stream})
        except Exception as e:
            if "BUSYGROUP" in str(e):
                logger.debug("Consumer group already exists", extra={"group": self._group})
            else:
                raise

    def _process_message(self, msg_id: str, fields: dict) -> None:
        self._message_handler(fields)

    def _dispatch(self, msg_id, msg_fields: dict) -> None:
        try:
            self._process_message(msg_id, msg_fields)
            self._redis.xack(self._stream, self._group, msg_id)
            logger.info("Message ACKed", extra={"msg_id": msg_id})
        except Exception as e:
            logger.error(
                "Failed to process message, not ACKing",
                extra={"msg_id": msg_id, "error": str(e)},
            )

    def _consume_once(self) -> None:
        messages = self._redis.xreadgroup(
            self._group,
            self._name,
            {self._stream: ">"},
            count=10,
            block=1000,
        )
        if not messages:
            return
        for _stream, msgs in messages:
            for msg_id, msg_fields in msgs:
                self._dispatch(msg_id, msg_fields)

    def _reclaim_stale_pending(self) -> None:
        """Claim messages left pending by a previous failure or a dead consumer
        and re-dispatch them.

        Consumer names are random per process start, so a message that was read
        but not ACKed (transient handler error) would otherwise sit in the group
        PEL forever — `>` only ever returns never-delivered messages. XAUTOCLAIM
        sweeps the PEL across all consumers, re-delivering anything idle past the
        reclaim window so a transient failure is retried instead of stranding the
        release. Messages that still fail are left pending for the next sweep.
        """
        cursor = "0-0"
        while True:
            result = self._redis.xautoclaim(
                self._stream,
                self._group,
                self._name,
                min_idle_time=_RECLAIM_MIN_IDLE_MS,
                start_id=cursor,
                count=10,
            )
            cursor, claimed = result[0], result[1]
            for msg_id, msg_fields in claimed:
                self._dispatch(msg_id, msg_fields)
            if cursor in (b"0-0", "0-0"):
                break

    def start(self) -> None:
        logger.info("Consumer starting", extra={"consumer_name": self._name, "stream": self._stream})
        while True:
            try:
                self._reclaim_stale_pending()
                self._consume_once()
            except Exception as e:
                logger.error("Consumer loop error", extra={"error": str(e)})
                if "NOGROUP" in str(e):
                    logger.warning("Consumer group lost, recreating", extra={"stream": self._stream})
                    self._create_group()
                time.sleep(3)
