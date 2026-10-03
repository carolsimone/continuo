import logging
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone

from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from adapters.redis.dead_letter import as_text, build_fields, tenant_of
from adapters.redis.error_class import ErrorClass, classify
from domain.contract_vocabulary import DeadLetterKind
from streams_contract import CONSUMER_DEAD_LETTER_V1

logger = logging.getLogger(__name__)

# Only reclaim messages that have been pending longer than this. A live peer
# may legitimately hold a message for the duration of an S3 download + sqlglot
# resolve, so the window is wide enough that reclaim never steals in-flight work.
_RECLAIM_MIN_IDLE_MS = 60_000

# How long the initial consumer-group creation keeps waiting for a Redis it
# cannot reach yet, and how long it pauses between attempts. A process can win
# the race against its own Redis on a cold start — on Kubernetes the Service
# DNS name may not resolve, under compose the server may not be accepting
# connections — and that failure clears itself within seconds. Without this
# wait the exception escapes the constructor and ends the process, which costs
# far more than the wait: CrashLoopBackOff then holds the pod down on an
# exponential delay (10s, 20s, 40s, 80s...) long after Redis is ready.
#
# Bounded rather than infinite, because the two failure modes need different
# answers. main.py only starts the health server once the consumer is
# constructed, so waiting forever on a genuinely unreachable or misconfigured
# Redis would leave a process that never serves a probe and never consumes
# anything, with nothing to signal it. Giving up restores that signal.
_STARTUP_CONNECT_TIMEOUT_S = 60.0
_STARTUP_CONNECT_BACKOFF_S = 3.0

# A message whose handler still fails transiently on this delivery is
# dead-lettered. The count is the pending entry's delivery counter.
_MAX_DELIVERIES = 5
# After an infrastructure error the consumer waits 1s, doubling to 60s, and
# refreshes its heartbeat and the message's idle time every 5s while it waits.
_INFRA_BACKOFF_BASE_S = 1.0
_INFRA_BACKOFF_CAP_S = 60.0
_PAUSE_SLICE_S = 5.0
# How many of this consumer's pending entries one pause slice re-claims.
_HOLD_BATCH = 200
# Logged once per abandoned message, after its dead letter is written.
# scripts/bench/outage.sh counts abandoned messages by this text.
LOG_DEAD_LETTERED = "Message dead-lettered — ACKing to drop from PEL"


class Consumer:
    def __init__(
        self,
        redis_client: Redis,
        stream_name: str,
        group_name: str,
        message_handler: Callable[[dict], None],
        *,
        service_name: str,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._redis = redis_client
        self._stream = stream_name
        self._group = group_name
        self._name = f"consumer-{uuid.uuid4().hex[:8]}"
        self._message_handler = message_handler
        # Names this service as the producer of the dead letters it writes.
        self._service = service_name
        self._now = clock
        # Stamped at the end of every start() loop pass (success or handled
        # failure) so a health check can tell "retrying through a Redis
        # outage" (heartbeat keeps advancing) apart from "the loop stopped
        # running" (heartbeat goes and stays stale) — see adapters/health.
        self.last_heartbeat = time.monotonic()
        self._create_group_awaiting_redis()

    def _create_group_awaiting_redis(self) -> None:
        """Create the consumer group, waiting out a Redis that is not
        reachable yet.

        Only a failure to reach Redis is retried. Any other error — a bad
        argument, a key of the wrong type — is permanent, so it is raised on
        the first attempt instead of burning the whole startup window on
        something no amount of waiting will fix.
        """
        deadline = time.monotonic() + _STARTUP_CONNECT_TIMEOUT_S
        attempt = 0
        while True:
            attempt += 1
            try:
                self._create_group()
                return
            except (RedisConnectionError, RedisTimeoutError) as e:
                if time.monotonic() >= deadline:
                    logger.error(
                        "Redis still unreachable after %.0fs and %d attempts; giving up: %s",
                        _STARTUP_CONNECT_TIMEOUT_S, attempt, e,
                    )
                    raise
                logger.warning(
                    "Redis not reachable yet (attempt %d), retrying in %.0fs: %s",
                    attempt, _STARTUP_CONNECT_BACKOFF_S, e,
                )
                time.sleep(_STARTUP_CONNECT_BACKOFF_S)

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
        """Run the handler for one message and settle it. On success it is
        ACKed. On a permanent error, or a transient error on its
        _MAX_DELIVERIES-th delivery, it is dead-lettered and then ACKed.
        Otherwise it stays pending for the next sweep. An infrastructure error
        is retried in place, without counting a delivery (see _pause). A failed
        ACK leaves the message pending and never aborts the batch."""
        text_id = as_text(msg_id)
        pauses = 0
        while True:
            try:
                self._process_message(msg_id, msg_fields)
            except Exception as exc:
                cls = classify(exc)
                if cls is ErrorClass.INFRASTRUCTURE:
                    pauses += 1
                    delay = self._backoff(pauses)
                    logger.warning(
                        "Infrastructure error — pausing consumer: message_id=%s pause=%d retry_in=%.0fs: %s",
                        text_id, pauses, delay, exc,
                    )
                    self._pause(msg_id, delay)
                    continue
                deliveries = self._deliveries(msg_id)
                if cls is ErrorClass.PERMANENT:
                    self._dead_letter_and_ack(msg_id, msg_fields, DeadLetterKind.PERMANENT, exc, deliveries)
                elif deliveries >= _MAX_DELIVERIES:
                    self._dead_letter_and_ack(
                        msg_id, msg_fields, DeadLetterKind.TRANSIENT_EXHAUSTED, exc, deliveries,
                    )
                else:
                    # Render the cause into the message (and attach the
                    # traceback via exc_info) rather than only into `extra`:
                    # the process log format is plain `%(message)s`, so an
                    # `extra`-only detail is invisible and a fatal misconfig
                    # (e.g. an S3 SignatureDoesNotMatch) reads as an opaque
                    # failure that repeats on every redelivery until the
                    # message is dead-lettered.
                    logger.exception(
                        "Failed to process message %s, not ACKing (delivery %d of %d): %s",
                        text_id, deliveries, _MAX_DELIVERIES, exc,
                    )
                return
            if self._ack(msg_id):
                logger.info("Message ACKed", extra={"msg_id": text_id})
            return

    def _ack(self, msg_id) -> bool:
        """XACK msg_id. A failure is logged and leaves the message pending, so
        the reclaim sweep redelivers it instead of the failure aborting the
        batch the message came from."""
        try:
            self._redis.xack(self._stream, self._group, msg_id)
        except Exception as exc:
            logger.error("Could not ACK message %s — it stays pending for the reclaim sweep: %s",
                         as_text(msg_id), exc)
            return False
        return True

    @staticmethod
    def _backoff(attempt: int) -> float:
        """Seconds to wait after the attempt-th consecutive failure: the base
        delay, doubling each time, never above the cap."""
        delay = _INFRA_BACKOFF_BASE_S
        for _ in range(attempt - 1):
            delay *= 2
            if delay >= _INFRA_BACKOFF_CAP_S:
                break
        return min(delay, _INFRA_BACKOFF_CAP_S)

    def _deliveries(self, msg_id) -> int:
        """Times msg_id was delivered to this group. 0 when Redis cannot tell,
        which never dead-letters a transient failure."""
        try:
            entries = self._redis.xpending_range(self._stream, self._group, min=msg_id, max=msg_id, count=1)
        except Exception as exc:
            logger.warning("Could not read the delivery count of %s: %s", as_text(msg_id), exc)
            return 0
        return int(entries[0]["times_delivered"]) if entries else 0

    def _pause(self, msg_id, seconds: float) -> None:
        """Wait out an infrastructure error. Every _PAUSE_SLICE_S it refreshes
        the heartbeat and holds this consumer's pending messages (see _hold),
        so a peer's reclaim sweep takes none of them mid-wait."""
        remaining = seconds
        while remaining > 0:
            self.last_heartbeat = time.monotonic()
            self._hold(msg_id)
            step = min(remaining, _PAUSE_SLICE_S)
            time.sleep(step)
            remaining -= step

    def _hold(self, msg_id) -> None:
        """Re-claim for this consumer, with XCLAIM JUSTID, msg_id and every
        other entry this consumer has pending (up to _HOLD_BATCH). JUSTID resets
        each entry's idle time without counting a delivery, so while one message
        waits, neither it nor the rest of its read batch grows idle enough for a
        peer's reclaim sweep to take it. XCLAIM skips an id that is no longer
        pending. Failures only shorten the protection, so they are logged."""
        ids = [msg_id]
        seen = {as_text(msg_id)}
        try:
            pending = self._redis.xpending_range(
                self._stream, self._group, min="-", max="+", count=_HOLD_BATCH, consumername=self._name,
            )
            for entry in pending:
                sibling = entry.get("message_id")
                if sibling is not None and as_text(sibling) not in seen:
                    seen.add(as_text(sibling))
                    ids.append(sibling)
        except Exception as exc:
            logger.warning("Could not list this consumer's pending messages — holding only %s: %s",
                           as_text(msg_id), exc)
        try:
            self._redis.xclaim(self._stream, self._group, self._name, min_idle_time=0,
                               message_ids=ids, justid=True)
        except Exception as exc:
            logger.warning("Could not refresh the idle time of %d pending messages (paused on %s): %s",
                           len(ids), as_text(msg_id), exc)

    def _dead_letter_and_ack(self, msg_id, msg_fields: dict, kind: DeadLetterKind, exc: Exception,
                             deliveries: int) -> None:
        """Write the dead letter, then ACK. While the write fails the message
        stays pending and the consumer pauses, retrying the write, never the
        handler."""
        text_id = as_text(msg_id)
        # The exception type stays in the text: str(KeyError("s3_uri")) alone
        # is just "'s3_uri'".
        error = f"{type(exc).__name__}: {exc}"
        fields = build_fields(tenant_id=tenant_of(msg_fields), producer=self._service, occurred_at=self._now(),
                              stream=self._stream, group=self._group, message_id=text_id, fields=msg_fields,
                              failure_kind=kind, error=error, delivery_count=deliveries)
        writes = 0
        while True:
            try:
                self._redis.xadd(CONSUMER_DEAD_LETTER_V1, fields)
                break
            except Exception as write_exc:
                writes += 1
                delay = self._backoff(writes)
                logger.error("Dead-letter write failed — message stays pending: message_id=%s retry_in=%.0fs: %s",
                             text_id, delay, write_exc)
                self._pause(msg_id, delay)
        logger.error("%s: stream=%s group=%s message_id=%s failure_kind=%s deliveries=%d dead_letter_event_id=%s error=%s",
                     LOG_DEAD_LETTERED, self._stream, self._group, text_id, kind, deliveries, fields["event_id"], error)
        self._ack(msg_id)

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
                # Broad by design: a transient Redis outage (ConnectionError,
                # TimeoutError, or any other RedisError) must never escape
                # this loop and end it — that would leave the process
                # "1/1 Running" with a dead consumer and nothing to signal
                # it, exactly the failure mode this loop exists to avoid.
                # Match the Go stream consumer's read-loop idiom (see
                # pkg/redis/streamconsumer.go Start()): log, retry, flat
                # backoff, no distinction between error types at this level.
                logger.exception("Consumer loop error: %s", e)
                if "NOGROUP" in str(e):
                    logger.warning("Consumer group lost, recreating", extra={"stream": self._stream})
                    try:
                        self._create_group()
                    except Exception:
                        # Redis may still be unreachable while we're trying to
                        # recreate the group; don't let *this* raise escape
                        # the loop either — the next pass retries both the
                        # group recreation (via NOGROUP) and the read.
                        logger.exception("Failed to recreate consumer group; will retry next pass")
                time.sleep(3)
            finally:
                # Every pass through the loop — whether it succeeded or hit a
                # handled error above — proves the loop is still alive and
                # cycling. Only a pass that hangs inside a call without ever
                # returning or raising (not a handled Redis error) leaves
                # this stale.
                self.last_heartbeat = time.monotonic()
