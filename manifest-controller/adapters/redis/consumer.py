import logging
import time
import uuid
from collections.abc import Callable
from redis import Redis

logger = logging.getLogger(__name__)


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

    def start(self) -> None:
        logger.info("Consumer starting", extra={"consumer_name": self._name, "stream": self._stream})
        while True:
            try:
                messages = self._redis.xreadgroup(
                    self._group,
                    self._name,
                    {self._stream: ">"},
                    count=10,
                    block=1000,
                )
                if not messages:
                    continue
                for _stream, msgs in messages:
                    for msg_id, msg_fields in msgs:
                        try:
                            self._process_message(msg_id, msg_fields)
                            self._redis.xack(self._stream, self._group, msg_id)
                            logger.info("Message ACKed", extra={"msg_id": msg_id})
                        except Exception as e:
                            logger.error(
                                "Failed to process message, not ACKing",
                                extra={"msg_id": msg_id, "error": str(e)},
                            )
            except Exception as e:
                logger.error("Consumer loop error", extra={"error": str(e)})
                if "NOGROUP" in str(e):
                    logger.warning("Consumer group lost, recreating", extra={"stream": self._stream})
                    self._create_group()
                time.sleep(3)
