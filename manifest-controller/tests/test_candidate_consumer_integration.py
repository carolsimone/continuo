import json
from unittest.mock import MagicMock
from adapters.redis.consumer import Consumer
from streams_contract import RELEASE_REQUESTED_V1, MANIFEST_CONTROLLER_RELEASE_REQUESTED


def test_consumer_dispatches_payload_to_message_handler():
    """One full XREADGROUP cycle: a release.requested:v1 message
    is read, dispatched to the handler, then ACKed."""
    redis_mock = MagicMock()
    redis_mock.xgroup_create.side_effect = Exception("BUSYGROUP")
    redis_mock.xreadgroup.return_value = [(
        RELEASE_REQUESTED_V1.encode(),
        [(b"1700000000-0", {b"payload": json.dumps({
            "release_id": "rel-int",
            "manifests_uri": "s3://continuo/releases/rel-int/manifests/",
        }).encode()})],
    )]

    captured = {}

    def handler(fields):
        captured["fields"] = fields

    consumer = Consumer(
        redis_client=redis_mock,
        stream_name=RELEASE_REQUESTED_V1,
        group_name=MANIFEST_CONTROLLER_RELEASE_REQUESTED,
        message_handler=handler,
    )

    # Drive one XREADGROUP iteration without entering the infinite loop.
    messages = redis_mock.xreadgroup(
        MANIFEST_CONTROLLER_RELEASE_REQUESTED, consumer._name,
        {RELEASE_REQUESTED_V1: ">"}, count=10, block=1000,
    )
    for _stream, msgs in messages:
        for msg_id, fields in msgs:
            consumer._process_message(msg_id, fields)
            redis_mock.xack(RELEASE_REQUESTED_V1, MANIFEST_CONTROLLER_RELEASE_REQUESTED, msg_id)

    assert captured["fields"][b"payload"]
    payload = json.loads(captured["fields"][b"payload"])
    assert payload["release_id"] == "rel-int"
    redis_mock.xack.assert_called()
