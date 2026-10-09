import json
from datetime import datetime, timedelta, timezone

from adapters.redis.envelope import envelope_fields


def test_envelope_fields_render_the_header_and_a_compact_payload():
    """The same rendering as pkg/events.Envelope.Fields: occurred_at in UTC with
    microseconds, schema_version as text, the payload as compact UTF-8 JSON."""
    cest = timezone(timedelta(hours=2))

    got = envelope_fields(
        event_id="e-1", tenant_id="default",
        occurred_at=datetime(2026, 10, 3, 14, 0, 0, 1, tzinfo=cest),
        producer="topology-controller", schema_version=2,
        payload={"b": "zürich", "a": 1},
    )

    assert got == {
        "event_id": "e-1",
        "tenant_id": "default",
        "occurred_at": "2026-10-03T12:00:00.000001Z",
        "producer": "topology-controller",
        "schema_version": "2",
        "payload": '{"b":"zürich","a":1}',
    }
    assert json.loads(got["payload"]) == {"a": 1, "b": "zürich"}
