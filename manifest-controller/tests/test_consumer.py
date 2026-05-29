import pytest
from unittest.mock import MagicMock
from adapters.redis.consumer import Consumer


def test_consumer_creates_group_on_init():
    mock_redis = MagicMock()
    Consumer(
        redis_client=mock_redis,
        stream_name="update.graph:v1",
        group_name="manifest-controller",
        message_handler=MagicMock(),
    )
    mock_redis.xgroup_create.assert_called_once_with(
        "update.graph:v1", "manifest-controller", id="0", mkstream=True
    )


def test_consumer_ignores_busygroup_error():
    mock_redis = MagicMock()
    mock_redis.xgroup_create.side_effect = Exception("BUSYGROUP Consumer Group name already exists")
    # Should not raise
    Consumer(
        redis_client=mock_redis,
        stream_name="update.graph:v1",
        group_name="manifest-controller",
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
