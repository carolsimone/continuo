import json
from datetime import datetime
from pathlib import Path

import pytest

from adapters.redis.dead_letter import build_fields, dedup_message_id, event_id
from domain.contract_vocabulary import DeadLetterKind

_FIXTURES = Path(__file__).parent / "fixtures"


# A dead letter of an entry read from the stream's legacy key, and one of an
# entry read from its prefixed key.
@pytest.mark.parametrize("name", ["consumer_dead_letter_v1.json", "consumer_dead_letter_v1_prefixed_key.json"])
def test_build_fields_matches_the_golden_fixture(name):
    fixture = json.loads((_FIXTURES / name).read_text())
    given = fixture["input"]
    got = build_fields(
        tenant_id=given["tenant_id"],
        producer=given["producer"],
        occurred_at=datetime.fromisoformat(given["occurred_at"].replace("Z", "+00:00")),
        stream=given["original_stream"],
        group=given["original_group"],
        message_id=given["original_message_id"],
        key=given["original_key"],
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


@pytest.mark.parametrize(("key", "want"), [
    ("orders:v1", "1-0"),
    ("continuo:orders:v1", "1-0@continuo:orders:v1"),
])
def test_dedup_message_id_qualifies_the_id_by_any_key_but_the_legacy_one(key, want):
    assert dedup_message_id("orders:v1", key, "1-0") == want
