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
