import json
import logging
import re
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import redis.exceptions as redis_exceptions

import adapters.redis.consumer as consumer_mod
from adapters.redis.consumer import LOG_DEAD_LETTERED, Consumer
from config.config import SERVICE_NAME
from domain.contract_vocabulary import DeadLetterKind
from service.errors import PermanentMessageError
from streams_contract import (
    CONSUMER_DEAD_LETTER_V1,
    RELEASE_REQUESTED_V1,
    TOPOLOGY_CONTROLLER_RELEASE_REQUESTED,
)

_SERVICE_NAME = SERVICE_NAME
_STREAM = RELEASE_REQUESTED_V1
_GROUP = TOPOLOGY_CONTROLLER_RELEASE_REQUESTED


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
        service_name=_SERVICE_NAME,
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
        service_name=_SERVICE_NAME,
    )


def test_process_message_calls_message_handler_with_fields():
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = Exception("BUSYGROUP ...")
    handler = MagicMock()
    c = Consumer(
        redis_client=redis_mock, stream_name="s", group_name="g",
        message_handler=handler,
        service_name=_SERVICE_NAME,
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
        service_name=_SERVICE_NAME,
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
        service_name=_SERVICE_NAME,
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
        service_name=_SERVICE_NAME,
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


def test_reclaim_only_claims_messages_idle_for_at_least_a_minute():
    """Reclaim passes a min-idle window of at least a minute, so a message a
    peer is still processing is not claimed before a normal handler finishes."""
    redis_mock = MagicMock()
    redis_mock.xautoclaim.return_value = (b"0-0", [], [])
    c = _consumer(redis_mock, lambda _f: None)

    c._reclaim_stale_pending()

    kwargs = redis_mock.xautoclaim.call_args.kwargs
    assert kwargs["min_idle_time"] >= 60_000


# --- start() loop resilience ------------------------------------------------
#
# Reproduces the 2026-07-19 incident: Redis briefly went unreachable and
# topology-controller's consumer loop was suspected to have died silently on
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
        service_name=_SERVICE_NAME,
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

    c = Consumer(redis_client=redis_mock, stream_name="s", group_name="g", message_handler=lambda f: None, service_name=_SERVICE_NAME)
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

    c = Consumer(redis_client=redis_mock, stream_name="s", group_name="g", message_handler=lambda f: None, service_name=_SERVICE_NAME)
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
        service_name=_SERVICE_NAME,
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
            service_name=_SERVICE_NAME,
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
            service_name=_SERVICE_NAME,
        )

    assert redis_mock.xgroup_create.call_count == 1


# --- error classes and dead letters -----------------------------------------


def _raising(exc):
    def handler(fields):
        raise exc
    return handler


def _dl_consumer(handler, redis_mock):
    redis_mock.xgroup_create.return_value = True
    return Consumer(redis_mock, _STREAM, _GROUP, handler, service_name=_SERVICE_NAME)


def _names(redis_mock):
    return [c[0] for c in redis_mock.method_calls if c[0] in ("xadd", "xack", "xclaim", "xpending_range")]


def test_a_consumer_without_a_service_name_refuses_to_start():
    """The service name is the producer of every dead letter the consumer
    writes, so an empty one is a configuration error caught at construction,
    before the consumer touches Redis."""
    r = MagicMock()
    with pytest.raises(ValueError, match="service_name"):
        Consumer(r, _STREAM, _GROUP, lambda fields: None, service_name="")
    r.xgroup_create.assert_not_called()


def test_permanent_error_dead_letters_then_acks(monkeypatch, caplog):
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xpending_range.return_value = [{"times_delivered": 1}]
    c = _dl_consumer(_raising(PermanentMessageError("no payload")), r)
    with caplog.at_level(logging.ERROR):
        c._dispatch(b"1-0", {b"x": b"y"})
    assert _names(r)[-2:] == ["xadd", "xack"]
    stream, fields = r.xadd.call_args[0]
    assert stream == CONSUMER_DEAD_LETTER_V1
    assert fields["producer"] == _SERVICE_NAME
    assert json.loads(fields["payload"])["failure_kind"] == DeadLetterKind.PERMANENT
    assert caplog.text.count(LOG_DEAD_LETTERED) == 1


def test_permanent_dead_letter_records_at_least_one_delivery_when_xpending_fails(monkeypatch):
    _defang_sleep(monkeypatch)
    for outcome in (redis_exceptions.ConnectionError("down"), []):
        r = MagicMock()
        if isinstance(outcome, Exception):
            r.xpending_range.side_effect = outcome
        else:
            r.xpending_range.return_value = outcome
        c = _dl_consumer(_raising(PermanentMessageError("bad")), r)
        c._dispatch(b"1-0", {})
        payload = json.loads(r.xadd.call_args[0][1]["payload"])
        assert payload["failure_kind"] == DeadLetterKind.PERMANENT
        assert payload["delivery_count"] == 1
        assert r.xack.called


def _bench_outage_script() -> Path:
    """Walk up from this file to scripts/bench/outage.sh. The topology-controller
    container mounts only pkg/, so there the script is absent and the test skips."""
    start = Path(__file__).resolve()
    for parent in start.parents:
        candidate = parent / "scripts" / "bench" / "outage.sh"
        if candidate.exists():
            return candidate
    pytest.skip("scripts/bench/outage.sh is not reachable from this checkout")


def test_dead_letter_log_line_matches_the_bench_outage_counter():
    """scripts/bench/outage.sh counts the messages consumers abandon by grepping
    service logs. This consumer must log exactly one line that pattern matches,
    and that line is the one written once per dead letter."""
    script = _bench_outage_script().read_text()
    found = re.search(r"grep -cE '([^']+)'", script)
    assert found, "outage.sh no longer counts abandoned messages with grep -cE '<pattern>'"
    pattern = re.compile(found.group(1))

    assert LOG_DEAD_LETTERED == "Message dead-lettered — ACKing to drop from PEL"
    assert pattern.search(LOG_DEAD_LETTERED)

    sources = sorted(Path(consumer_mod.__file__).parent.glob("*.py"))
    matches = sum(len(pattern.findall(src.read_text())) for src in sources)
    assert matches == 1, "only the LOG_DEAD_LETTERED constant may match the bench counter"


def test_dead_letter_write_failure_keeps_message_pending(monkeypatch, caplog):
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xpending_range.return_value = [{"times_delivered": 1}]
    r.xadd.side_effect = [redis_exceptions.ConnectionError("down"), redis_exceptions.ConnectionError("down"), b"9-0"]
    c = _dl_consumer(_raising(PermanentMessageError("bad")), r)
    with caplog.at_level(logging.ERROR):
        c._dispatch(b"1-0", {})
    calls = _names(r)
    assert calls.count("xadd") == 3
    assert calls[-1] == "xack" and calls.count("xack") == 1
    assert caplog.text.count(LOG_DEAD_LETTERED) == 1


def test_transient_error_dead_letters_on_the_fifth_delivery(monkeypatch):
    _defang_sleep(monkeypatch)
    for delivered, dead_lettered in ((4, False), (5, True)):
        r = MagicMock()
        r.xpending_range.return_value = [{"times_delivered": delivered}]
        c = _dl_consumer(_raising(RuntimeError("flaky")), r)
        c._dispatch(b"1-0", {})
        assert r.xadd.called is dead_lettered
        assert r.xack.called is dead_lettered
        if dead_lettered:
            assert json.loads(r.xadd.call_args[0][1]["payload"])["failure_kind"] == DeadLetterKind.TRANSIENT_EXHAUSTED


def test_infrastructure_error_pauses_without_counting(monkeypatch):
    _defang_sleep(monkeypatch)
    r = MagicMock()
    outcomes = [redis_exceptions.ConnectionError("down"), redis_exceptions.ConnectionError("down"), None]

    def handler(fields):
        outcome = outcomes.pop(0)
        if outcome:
            raise outcome

    c = _dl_consumer(handler, r)
    delays = []
    pause = c._pause

    def spy(msg_id, seconds):
        delays.append(seconds)
        pause(msg_id, seconds)

    c._pause = spy
    c._dispatch(b"1-0", {})
    assert delays == [1.0, 2.0], "each consecutive outage waits twice as long"
    delivery_count_reads = [
        call for call in r.xpending_range.call_args_list
        if call.kwargs.get("min") == call.kwargs.get("max") == b"1-0"
    ]
    assert not delivery_count_reads, "an outage never reads the delivery count"
    assert r.xclaim.call_args.kwargs["justid"] is True
    assert r.xack.call_count == 1 and not r.xadd.called


def test_dead_letter_write_retries_never_rerun_the_handler(monkeypatch):
    """A failing dead-letter write is retried on its own: the handler already
    gave its verdict and runs once."""
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xpending_range.return_value = [{"times_delivered": 1}]
    r.xadd.side_effect = [redis_exceptions.ConnectionError("down"), b"9-0"]
    calls = []

    def handler(fields):
        calls.append(fields)
        raise PermanentMessageError("bad")

    _dl_consumer(handler, r)._dispatch(b"1-0", {})
    assert len(calls) == 1
    assert r.xadd.call_count == 2


def test_dead_letter_write_failure_refreshes_the_pending_entry_while_waiting(monkeypatch):
    """While the dead-letter write fails the message is re-claimed with JUSTID,
    so a peer's reclaim sweep never takes it."""
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xpending_range.return_value = [{"times_delivered": 1}]
    r.xadd.side_effect = [redis_exceptions.ConnectionError("down"), b"9-0"]
    _dl_consumer(_raising(PermanentMessageError("bad")), r)._dispatch(b"1-0", {})
    assert r.xclaim.call_args.kwargs["justid"] is True
    assert r.xclaim.call_args.kwargs["message_ids"] == [b"1-0"]
    assert _names(r).index("xclaim") < _names(r).index("xack")


def test_unreadable_delivery_count_never_dead_letters_a_transient_failure(monkeypatch):
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xpending_range.side_effect = redis_exceptions.ConnectionError("down")
    _dl_consumer(_raising(RuntimeError("flaky")), r)._dispatch(b"1-0", {})
    assert not r.xadd.called and not r.xack.called


def test_dead_letter_carries_the_message_tenant_and_ids_as_text(monkeypatch):
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xpending_range.return_value = [{"times_delivered": 3}]
    fields = {b"tenant_id": b"acme", b"payload": b"{}"}
    _dl_consumer(_raising(PermanentMessageError("bad")), r)._dispatch(b"7-1", fields)
    _, written = r.xadd.call_args[0]
    payload = json.loads(written["payload"])
    assert written["tenant_id"] == "acme"
    assert payload["original_stream"] == _STREAM
    assert payload["original_group"] == _GROUP
    assert payload["original_message_id"] == "7-1"
    assert payload["fields"] == {"payload": "{}", "tenant_id": "acme"}
    assert payload["delivery_count"] == 3
    r.xack.assert_called_once_with(_STREAM, _GROUP, b"7-1")


def test_dead_letter_is_stamped_from_the_injected_clock(monkeypatch):
    from datetime import datetime, timezone
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xpending_range.return_value = [{"times_delivered": 1}]
    r.xgroup_create.return_value = True
    at = datetime(2026, 10, 3, 12, 0, 0, tzinfo=timezone.utc)
    c = Consumer(r, _STREAM, _GROUP, _raising(PermanentMessageError("bad")),
                 service_name=_SERVICE_NAME, clock=lambda: at)
    c._dispatch(b"1-0", {})
    assert r.xadd.call_args[0][1]["occurred_at"] == "2026-10-03T12:00:00.000000Z"


def test_reclaimed_message_that_fails_permanently_is_dead_lettered(monkeypatch):
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xautoclaim.return_value = (b"0-0", [(b"5-0", {b"payload": b"x"})], [])
    r.xpending_range.return_value = [{"times_delivered": 2}]
    _dl_consumer(_raising(PermanentMessageError("bad")), r)._reclaim_stale_pending()
    assert r.xadd.call_args[0][0] == CONSUMER_DEAD_LETTER_V1
    r.xack.assert_called_once_with(_STREAM, _GROUP, b"5-0")


def test_infrastructure_backoff_doubles_from_one_second_and_caps_at_sixty():
    delays = [Consumer._backoff(n) for n in range(1, 9)]
    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]


def test_infrastructure_pause_refreshes_heartbeat_and_idle_time_every_slice(monkeypatch):
    """A 12s pause makes three slices (5s, 5s, 2s); each one re-claims the
    message and stamps the heartbeat, so neither a peer nor the liveness probe
    mistakes the wait for a stall."""
    sleeps = []
    ticks = iter(range(100, 200))
    monkeypatch.setattr(
        consumer_mod, "time",
        SimpleNamespace(sleep=sleeps.append, monotonic=lambda: float(next(ticks))),
    )
    r = MagicMock()
    c = _dl_consumer(lambda f: None, r)
    before = c.last_heartbeat
    c._pause(b"1-0", 12.0)
    assert sleeps == [5.0, 5.0, 2.0]
    assert r.xclaim.call_count == 3
    assert c.last_heartbeat > before


def test_infrastructure_pause_survives_a_failing_idle_refresh(monkeypatch):
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xclaim.side_effect = redis_exceptions.ConnectionError("down")
    _dl_consumer(lambda f: None, r)._pause(b"1-0", 1.0)


def test_failed_ack_after_a_handled_message_leaves_it_pending_and_continues_the_batch():
    r = MagicMock()
    r.xgroup_create.return_value = True
    r.xreadgroup.return_value = [(_STREAM.encode(), [(b"1-0", {b"payload": b"a"}), (b"2-0", {b"payload": b"b"})])]
    r.xack.side_effect = [redis_exceptions.ConnectionError("down"), 1]
    seen = []
    c = Consumer(r, _STREAM, _GROUP, seen.append, service_name=_SERVICE_NAME)
    c._consume_once()  # must not raise
    assert seen == [{b"payload": b"a"}, {b"payload": b"b"}], "the second message is still handled"
    assert [call.args[2] for call in r.xack.call_args_list] == [b"1-0", b"2-0"]


def test_failed_ack_after_a_dead_letter_write_leaves_it_pending_and_continues_the_batch(monkeypatch, caplog):
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xgroup_create.return_value = True
    r.xpending_range.return_value = [{"times_delivered": 1}]
    r.xreadgroup.return_value = [(_STREAM.encode(), [(b"1-0", {b"payload": b"a"}), (b"2-0", {b"payload": b"b"})])]
    r.xack.side_effect = [redis_exceptions.ConnectionError("down"), 1]
    c = Consumer(r, _STREAM, _GROUP, _raising(PermanentMessageError("bad")), service_name=_SERVICE_NAME)
    with caplog.at_level(logging.ERROR):
        c._consume_once()  # must not raise
    assert r.xadd.call_count == 2, "both messages are dead-lettered"
    assert r.xack.call_count == 2
    assert "Could not ACK message 1-0" in caplog.text


def test_failed_ack_is_logged(caplog):
    r = MagicMock()
    r.xack.side_effect = redis_exceptions.ConnectionError("down")
    c = _dl_consumer(lambda f: None, r)
    with caplog.at_level(logging.ERROR):
        c._dispatch(b"1-0", {})
    assert "Could not ACK message 1-0" in caplog.text
    assert "Message ACKed" not in caplog.text


def test_pause_holds_every_entry_this_consumer_has_pending(monkeypatch):
    """A pause on one message re-claims, with JUSTID, the paused id and the
    batch siblings this consumer still holds, so a peer's reclaim sweep takes
    none of them during a long outage."""
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xpending_range.return_value = [
        {"message_id": b"1-0", "times_delivered": 1},
        {"message_id": b"2-0", "times_delivered": 1},
        {"message_id": b"3-0", "times_delivered": 1},
    ]
    c = _dl_consumer(lambda f: None, r)
    c._pause(b"2-0", 1.0)
    listing = r.xpending_range.call_args
    assert listing.kwargs["consumername"] == c._name
    assert listing.kwargs["count"] == 200
    claim = r.xclaim.call_args
    assert claim.kwargs["justid"] is True and claim.kwargs["min_idle_time"] == 0
    assert claim.args[2] == c._name
    assert claim.kwargs["message_ids"] == [b"2-0", b"1-0", b"3-0"], "the paused id once, then its siblings"


def test_pause_still_holds_the_paused_message_when_listing_pending_fails(monkeypatch):
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xpending_range.side_effect = redis_exceptions.ConnectionError("down")
    _dl_consumer(lambda f: None, r)._pause(b"2-0", 1.0)
    assert r.xclaim.call_args.kwargs["message_ids"] == [b"2-0"]


def test_infrastructure_backoff_never_overflows_on_a_long_outage():
    assert Consumer._backoff(2000) == 60.0
    assert Consumer._backoff(10**6) == 60.0


def test_dead_letter_error_keeps_the_exception_type(monkeypatch, caplog):
    """str(KeyError("s3_uri")) is just "'s3_uri'"; the dead letter and its log
    line name the exception type as well."""
    _defang_sleep(monkeypatch)
    r = MagicMock()
    r.xpending_range.return_value = [{"times_delivered": 5}]
    with caplog.at_level(logging.ERROR):
        _dl_consumer(_raising(KeyError("s3_uri")), r)._dispatch(b"1-0", {})
    assert json.loads(r.xadd.call_args[0][1]["payload"])["error"] == "KeyError: 's3_uri'"
    assert "error=KeyError: 's3_uri'" in caplog.text
