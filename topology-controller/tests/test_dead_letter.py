import json
from datetime import datetime
from pathlib import Path

from adapters.redis.dead_letter import build_fields, event_id
from domain.contract_vocabulary import DeadLetterKind

_FIXTURE = Path(__file__).parent / "fixtures" / "consumer_dead_letter_v1.json"


def test_build_fields_matches_the_golden_fixture():
    fixture = json.loads(_FIXTURE.read_text())
    given = fixture["input"]
    got = build_fields(
        tenant_id=given["tenant_id"],
        producer=given["producer"],
        occurred_at=datetime.fromisoformat(given["occurred_at"].replace("Z", "+00:00")),
        stream=given["original_stream"],
        group=given["original_group"],
        message_id=given["original_message_id"],
        fields={k.encode(): v.encode() for k, v in given["fields"].items()},
        failure_kind=DeadLetterKind(given["failure_kind"]),
        error=given["error"],
        delivery_count=given["delivery_count"],
    )
    want = fixture["fields"]
    assert set(got) == set(want)
    for key in ("event_id", "tenant_id", "occurred_at", "producer", "schema_version"):
        assert got[key] == want[key], key
    assert json.loads(got["payload"]) == want["payload"]


def test_event_id_includes_the_tenant():
    assert event_id("default", "g", "s", "1-0") == event_id("default", "g", "s", "1-0")
    assert event_id("default", "g", "s", "1-0") != event_id("acme", "g", "s", "1-0")
