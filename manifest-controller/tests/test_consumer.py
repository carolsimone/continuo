import logging
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import redis.exceptions as redis_exceptions

import adapters.redis.consumer as consumer_mod
from adapters.redis.consumer import Consumer
from streams_contract import RELEASE_REQUESTED_V1, MANIFEST_CONTROLLER_RELEASE_REQUESTED

_STREAM = RELEASE_REQUESTED_V1
_GROUP = MANIFEST_CONTROLLER_RELEASE_REQUESTED


_TEST_RETRY_SLEEP = 0.01  # real seconds; short enough to keep tests fast

# The real xreadgroup(..., block=1000) blocks on the socket for up to a
# second server-side, which is what paces the production loop on the
# no-error/no-message path. A MagicMock returns instantly, so without some
# pacing here start()'s `while True` spins as fast as the interpreter can
# manage — GIL-starving the test's own thread and burning CPU for the rest
# of the process, since these background threads are daemons with no stop
# mechanism and outlive the test. This tiny real sleep keeps every side
# effect below at a bounded, sane iteration rate instead.
_TEST_ITERATION_PACING = 0.01


def _defang_sleep(monkeypatch):
    """Rebind the `time` name inside adapters.redis.consumer's own module
    namespace to a fake whose sleep is short-but-real (not zero — see
    _TEST_ITERATION_PACING above), so start()'s retry loop doesn't block for
    the real production 3s but still yields the GIL each pass. This only
    shadows consumer.py's own binding (monkeypatch reverts it at teardown) —
    it never mutates the real stdlib `time` module, so it can't affect
    unrelated code or other tests."""
    monkeypatch.setattr(
        consumer_mod, "time",
        SimpleNamespace(sleep=lambda _s: time.sleep(_TEST_RETRY_SLEEP), monotonic=time.monotonic),
    )


def test_consumer_creates_group_on_init():
    mock_redis = MagicMock()
    Consumer(
        redis_client=mock_redis,
        stream_name=_STREAM,
        group_name=_GROUP,
        message_handler=MagicMock(),
    )
    mock_redis.xgroup_create.assert_called_once_with(
        _STREAM, _GROUP, id="0", mkstream=True
    )


def test_consumer_ignores_busygroup_error():
    mock_redis = MagicMock()
    mock_redis.xgroup_create.side_effect = Exception("BUSYGROUP Consumer Group name already exists")
    # Should not raise
    Consumer(
        redis_client=mock_redis,
        stream_name=_STREAM,
        group_name=_GROUP,
        message_handler=MagicMock(),
    )


def test_process_message_calls_message_handler_with_fields():
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = Exception("BUSYGROUP ...")
    handler = MagicMock()
    c = Consumer(
        redis_client=redis_mock, stream_name="s", group_name="g",
        message_handler=handler,
    )
    fields = {b"payload": b'{"release_id":"r"}'}
    c._process_message(msg_id="1-0", fields=fields)
    handler.assert_called_once_with(fields)


def test_process_message_propagates_handler_exception():
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = Exception("BUSYGROUP ...")
    def boom(_):
        raise RuntimeError("nope")
    c = Consumer(
        redis_client=redis_mock, stream_name="s", group_name="g",
        message_handler=boom,
    )
    with pytest.raises(RuntimeError, match="nope"):
        c._process_message(msg_id="1-0", fields={})


def test_dispatch_failure_logs_the_actual_exception_with_traceback(caplog):
    """When the handler raises, the failure log must render the real exception
    detail in the message AND capture a traceback (exc_info). A misconfig such
    as an S3 SignatureDoesNotMatch is otherwise invisible if the detail only
    lives in a `extra={}` field the log format does not render."""
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = Exception("BUSYGROUP")

    def boom(_fields):
        raise RuntimeError("SignatureDoesNotMatch calling GetObject")

    c = Consumer(
        redis_client=redis_mock, stream_name="s", group_name="g",
        message_handler=boom,
    )

    with caplog.at_level(logging.ERROR, logger="adapters.redis.consumer"):
        c._dispatch(msg_id="9-0", msg_fields={b"payload": b"x"})

    redis_mock.xack.assert_not_called()
    failures = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert failures, "expected an ERROR log on dispatch failure"
    rec = failures[-1]
    # The rendered message (what a plain `%(message)s` format prints) must carry
    # the underlying cause and the message id.
    rendered = rec.getMessage()
    assert "SignatureDoesNotMatch calling GetObject" in rendered
    assert "9-0" in rendered
    # And a traceback must be attached so the failure is fully diagnosable.
    assert rec.exc_info is not None and rec.exc_info[0] is RuntimeError


def _consumer(redis_mock, handler):
    redis_mock.xgroup_create.side_effect = Exception("BUSYGROUP")
    return Consumer(
        redis_client=redis_mock, stream_name="s", group_name="g",
        message_handler=handler,
    )


def test_reclaim_redispatches_pending_message_and_acks():
    """A message left pending by a previous (now-dead) consumer is claimed,
    re-dispatched to the handler, and ACKed."""
    redis_mock = MagicMock()
    redis_mock.xautoclaim.return_value = (
        b"0-0", [(b"5-0", {b"payload": b"x"})], [],
    )
    seen = []
    c = _consumer(redis_mock, lambda fields: seen.append(fields))

    c._reclaim_stale_pending()

    assert seen == [{b"payload": b"x"}]
    redis_mock.xack.assert_called_once_with("s", "g", b"5-0")


def test_reclaim_pages_until_cursor_returns_to_zero():
    """xautoclaim is paged: the consumer keeps claiming until the server
    returns the 0-0 cursor, and every claimed message is dispatched."""
    redis_mock = MagicMock()
    redis_mock.xautoclaim.side_effect = [
        (b"100-0", [(b"5-0", {b"payload": b"a"})], []),
        (b"0-0",   [(b"6-0", {b"payload": b"b"})], []),
    ]
    seen = []
    c = _consumer(redis_mock, lambda fields: seen.append(fields))

    c._reclaim_stale_pending()

    assert seen == [{b"payload": b"a"}, {b"payload": b"b"}]
    assert redis_mock.xautoclaim.call_count == 2
    first_kwargs = redis_mock.xautoclaim.call_args_list[0].kwargs
    second_kwargs = redis_mock.xautoclaim.call_args_list[1].kwargs
    assert first_kwargs["start_id"] == "0-0"
    assert second_kwargs["start_id"] == b"100-0"


def test_reclaim_does_not_ack_when_handler_still_failing():
    """A reclaimed message whose handler still raises (persistent transient
    error) is left pending — never ACKed — so a later reclaim retries it."""
    redis_mock = MagicMock()
    redis_mock.xautoclaim.return_value = (
        b"0-0", [(b"5-0", {b"payload": b"x"})], [],
    )

    def boom(_fields):
        raise RuntimeError("still down")

    c = _consumer(redis_mock, boom)

    c._reclaim_stale_pending()  # must not raise

    redis_mock.xack.assert_not_called()


def test_reclaim_uses_min_idle_so_it_never_steals_in_flight_work():
    """Reclaim must pass a non-trivial min-idle so a live peer's in-flight
    message is not stolen mid-processing."""
    redis_mock = MagicMock()
    redis_mock.xautoclaim.return_value = (b"0-0", [], [])
    c = _consumer(redis_mock, lambda _f: None)

    c._reclaim_stale_pending()

    kwargs = redis_mock.xautoclaim.call_args.kwargs
    assert kwargs["min_idle_time"] >= 60_000


# --- start() loop resilience ------------------------------------------------
#
# Reproduces the 2026-07-19 incident: Redis briefly went unreachable and
# manifest-controller's consumer loop was suspected to have died silently on
# the first redis.exceptions.ConnectionError, unlike the Go services' stream
# consumer (pkg/redis/streamconsumer.go Start()), which logs "Error in read
# loop" and keeps retrying with a flat 3s backoff until Redis recovers.
#
# These tests drive the *real* start() loop (in a background thread, against
# a mocked redis client) through that same failure shape and assert it
# matches the Go behavior: log, retry, keep consuming once the connection
# recovers — never let the exception escape and end the loop.

def _start_in_background(consumer: Consumer) -> threading.Thread:
    t = threading.Thread(target=consumer.start, daemon=True)
    t.start()
    return t


def test_start_survives_transient_connection_errors_and_keeps_consuming(monkeypatch):
    """A run of redis.exceptions.ConnectionError from _reclaim_stale_pending
    (the exact call site in the incident traceback) must not kill the
    consumer loop: it keeps retrying and successfully consumes a message once
    the connection recovers."""
    _defang_sleep(monkeypatch)
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = Exception("BUSYGROUP")

    attempts = {"n": 0}

    def xautoclaim_side_effect(*_a, **_kw):
        attempts["n"] += 1
        if attempts["n"] <= 3:
            raise redis_exceptions.ConnectionError(
                "Error 111 connecting to continuo-infra-redis-master:6379. Connection refused."
            )
        time.sleep(_TEST_ITERATION_PACING)  # simulates the real xreadgroup(block=...) pacing
        return (b"0-0", [], [])

    redis_mock.xautoclaim.side_effect = xautoclaim_side_effect

    delivered = threading.Event()

    def xreadgroup_side_effect(*_a, **_kw):
        if attempts["n"] > 3 and not delivered.is_set():
            delivered.set()
            return [("s", [(b"1-0", {b"payload": b"x"})])]
        time.sleep(_TEST_ITERATION_PACING)
        return []

    redis_mock.xreadgroup.side_effect = xreadgroup_side_effect

    seen = []
    c = Consumer(
        redis_client=redis_mock, stream_name="s", group_name="g",
        message_handler=lambda fields: seen.append(fields),
    )

    t = _start_in_background(c)

    assert delivered.wait(timeout=5), "consumer never recovered after transient ConnectionErrors"
    deadline = time.monotonic() + 2
    while not seen and time.monotonic() < deadline:
        time.sleep(0.01)

    assert seen == [{b"payload": b"x"}]
    assert t.is_alive(), "consumer thread must not exit on a transient connection error"
    assert attempts["n"] > 3, "must have actually retried xautoclaim after the failures"


def test_start_survives_group_recreate_failure_during_nogroup_recovery(monkeypatch):
    """If Redis is still unreachable when start() tries to recreate a lost
    consumer group (the NOGROUP branch), that failure must not escape the
    loop either — it should log and retry on the next pass."""
    _defang_sleep(monkeypatch)
    redis_mock = MagicMock()

    creates = {"n": 0}

    def xgroup_create_side_effect(*_a, **_kw):
        creates["n"] += 1
        if creates["n"] == 2:
            # The in-loop recreate attempt fails (Redis still down).
            raise redis_exceptions.ConnectionError("Error 111 connecting to host:6379.")
        return None

    redis_mock.xgroup_create.side_effect = xgroup_create_side_effect

    claims = {"n": 0}

    def xautoclaim_side_effect(*_a, **_kw):
        claims["n"] += 1
        if claims["n"] == 1:
            raise Exception("NOGROUP No such key 's' or consumer group 'g'")
        time.sleep(_TEST_ITERATION_PACING)
        return (b"0-0", [], [])

    redis_mock.xautoclaim.side_effect = xautoclaim_side_effect

    def xreadgroup_side_effect(*_a, **_kw):
        time.sleep(_TEST_ITERATION_PACING)
        return []

    redis_mock.xreadgroup.side_effect = xreadgroup_side_effect

    c = Consumer(redis_client=redis_mock, stream_name="s", group_name="g", message_handler=lambda f: None)
    t = _start_in_background(c)

    deadline = time.monotonic() + 2
    while claims["n"] < 3 and time.monotonic() < deadline:
        time.sleep(0.01)

    assert t.is_alive(), "a failure recreating the consumer group must not kill the loop"
    assert claims["n"] >= 3, "must keep attempting xautoclaim across passes"


def test_last_heartbeat_advances_while_loop_runs(monkeypatch):
    """The health server (adapters/health/server.py) treats a stale
    last_heartbeat as 'the loop stopped making progress'. It must advance on
    every pass, including passes that hit a handled Redis error."""
    _defang_sleep(monkeypatch)
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = Exception("BUSYGROUP")
    redis_mock.xautoclaim.side_effect = redis_exceptions.ConnectionError("Error 111 connecting.")
    redis_mock.xreadgroup.return_value = []

    c = Consumer(redis_client=redis_mock, stream_name="s", group_name="g", message_handler=lambda f: None)
    initial = c.last_heartbeat

    t = _start_in_background(c)

    deadline = time.monotonic() + 2
    while c.last_heartbeat == initial and time.monotonic() < deadline:
        time.sleep(0.01)

    assert c.last_heartbeat > initial
    assert t.is_alive()


def test_create_group_waits_out_a_redis_that_is_not_up_yet(monkeypatch):
    """A cold start can beat its own Redis: on Kubernetes the Service DNS name
    may not resolve yet, and under compose the server may not be accepting
    connections. That is transient and self-clearing, so construction waits it
    out instead of letting the error kill the process."""
    _defang_sleep(monkeypatch)
    redis_mock = MagicMock()
    unreachable = redis_exceptions.ConnectionError(
        "Error -2 connecting to continuo-redis:6379. Name or service not known."
    )
    redis_mock.xgroup_create.side_effect = [unreachable, unreachable, None]

    Consumer(
        redis_client=redis_mock,
        stream_name=_STREAM,
        group_name=_GROUP,
        message_handler=MagicMock(),
    )

    assert redis_mock.xgroup_create.call_count == 3


def test_create_group_gives_up_once_the_startup_window_expires(monkeypatch):
    """Bounded, not infinite. A Redis that is genuinely unreachable or
    misconfigured must still fail the process, rather than leave it alive and
    silent — main.py starts the health server only after this constructor
    returns, so an endless wait would report no health at all."""
    _defang_sleep(monkeypatch)
    monkeypatch.setattr(consumer_mod, "_STARTUP_CONNECT_TIMEOUT_S", 0.05)
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = redis_exceptions.ConnectionError(
        "Error -2 connecting to continuo-redis:6379. Name or service not known."
    )

    with pytest.raises(redis_exceptions.ConnectionError):
        Consumer(
            redis_client=redis_mock,
            stream_name=_STREAM,
            group_name=_GROUP,
            message_handler=MagicMock(),
        )

    assert redis_mock.xgroup_create.call_count > 1


def test_create_group_does_not_retry_a_permanent_error(monkeypatch):
    """Only a failure to reach Redis is worth waiting on. A permanent error
    surfaces immediately instead of burning the whole startup window on
    something no amount of waiting will fix."""
    _defang_sleep(monkeypatch)
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = redis_exceptions.ResponseError(
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    )

    with pytest.raises(redis_exceptions.ResponseError):
        Consumer(
            redis_client=redis_mock,
            stream_name=_STREAM,
            group_name=_GROUP,
            message_handler=MagicMock(),
        )

    assert redis_mock.xgroup_create.call_count == 1
