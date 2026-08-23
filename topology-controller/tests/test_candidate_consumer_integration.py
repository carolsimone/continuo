import json
from unittest.mock import MagicMock
from adapters.redis.consumer import Consumer
from streams_contract import RELEASE_REQUESTED_V1, TOPOLOGY_CONTROLLER_RELEASE_REQUESTED


def test_consume_once_dispatches_payload_and_acks():
    """One XREADGROUP poll: a release.requested:v1 message is read by the
    production loop body, dispatched to the message_handler, then ACKed."""
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = Exception("BUSYGROUP")
    redis_mock.xreadgroup.return_value = [(
        RELEASE_REQUESTED_V1.encode(),
        [(b"1700000000-0", {b"payload": json.dumps({
            "release_id": "rel-int",
            "manifest_keys": [
                {"service": "service-1", "s3_uri": "s3://continuo/service-1/rel-int/manifest.json"},
            ],
        }).encode()})],
    )]

    captured = {}

    def handler(fields):
        captured["fields"] = fields

    consumer = Consumer(
        redis_client=redis_mock,
        stream_name=RELEASE_REQUESTED_V1,
        group_name=TOPOLOGY_CONTROLLER_RELEASE_REQUESTED,
        message_handler=handler,
    )

    consumer._consume_once()

    # The production loop body dispatched the raw fields to our handler...
    assert b"payload" in captured["fields"]
    payload = json.loads(captured["fields"][b"payload"])
    assert payload["release_id"] == "rel-int"
    # ...and the production code ACKed the message id it just processed.
    redis_mock.xack.assert_called_once_with(
        RELEASE_REQUESTED_V1, TOPOLOGY_CONTROLLER_RELEASE_REQUESTED, b"1700000000-0",
    )


def test_consume_once_does_not_ack_when_handler_raises():
    """If the message_handler raises, the production loop must NOT ACK the
    message (it stays in the PEL for redelivery)."""
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = Exception("BUSYGROUP")
    redis_mock.xreadgroup.return_value = [(
        RELEASE_REQUESTED_V1.encode(),
        [(b"1700000000-1", {b"payload": b"{}"})],
    )]

    def handler(_fields):
        raise RuntimeError("transient")

    consumer = Consumer(
        redis_client=redis_mock,
        stream_name=RELEASE_REQUESTED_V1,
        group_name=TOPOLOGY_CONTROLLER_RELEASE_REQUESTED,
        message_handler=handler,
    )

    consumer._consume_once()  # must not raise — the loop body swallows handler errors
    redis_mock.xack.assert_not_called()
