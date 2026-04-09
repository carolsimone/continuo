import pytest
from unittest.mock import MagicMock
from adapters.redis.consumer import Consumer


def _make_consumer(handler_factory=None):
    mock_redis = MagicMock()
    mock_redis.xgroup_create.side_effect = Exception("BUSYGROUP Consumer Group name already exists")
    mock_factory = handler_factory or MagicMock()
    return Consumer(
        redis_client=mock_redis,
        stream_name="update.graph:v1",
        group_name="manifest-controller",
        handler_factory=mock_factory,
    ), mock_redis, mock_factory


def test_consumer_creates_group_on_init():
    mock_redis = MagicMock()
    Consumer(
        redis_client=mock_redis,
        stream_name="update.graph:v1",
        group_name="manifest-controller",
        handler_factory=MagicMock(),
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
        handler_factory=MagicMock(),
    )


def test_process_message_calls_handler_factory_with_source_name():
    consumer, _, mock_factory = _make_consumer()
    consumer._process_message(msg_id="1-0", fields={b"source": b"local"})
    mock_factory.assert_called_once_with("local")


def test_process_message_raises_on_unknown_source():
    consumer, _, mock_factory = _make_consumer()
    with pytest.raises(ValueError, match="unknown source"):
        consumer._process_message(msg_id="1-0", fields={b"source": b"unknown_source"})
    mock_factory.assert_not_called()


def test_process_message_raises_on_missing_source():
    consumer, _, mock_factory = _make_consumer()
    with pytest.raises(ValueError, match="missing source"):
        consumer._process_message(msg_id="1-0", fields={})
    mock_factory.assert_not_called()


def test_process_message_accepts_s3_source():
    consumer, _, mock_factory = _make_consumer()
    consumer._process_message(msg_id="1-0", fields={b"source": b"s3"})
    mock_factory.assert_called_once_with("s3")
