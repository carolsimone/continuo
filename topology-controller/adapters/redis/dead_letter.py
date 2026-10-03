"""consumer.dead_letter:v1 entries written by the release.requested consumer.

The fields match the Go consumers' (pkg/events.ConsumerDeadLetterFields);
tests/fixtures/consumer_dead_letter_v1.json pins both to the same entry.
"""

import json
import uuid
from datetime import datetime, timezone

from domain.contract_vocabulary import DeadLetterKind

EVENT_ID_NAMESPACE = uuid.UUID("2f0c5a7e-8d14-4b63-a1f9-6c3e5b2d7a40")
DEFAULT_TENANT_ID = "default"
SCHEMA_VERSION = 1


def event_id(tenant_id: str, group: str, stream: str, message_id: str) -> str:
    """Deterministic id of the dead letter for one message in one group."""
    name = "|".join(["consumer.dead_letter", tenant_id, group, stream, message_id])
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, name))


def as_text(value) -> str:
    """A Redis field name or value as text; bytes are decoded as UTF-8."""
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)


def tenant_of(fields: dict) -> str:
    raw = fields.get(b"tenant_id", fields.get("tenant_id"))
    return as_text(raw) if raw else DEFAULT_TENANT_ID


def build_fields(*, tenant_id: str, producer: str, occurred_at: datetime, stream: str, group: str,
                 message_id: str, fields: dict, failure_kind: DeadLetterKind, error: str,
                 delivery_count: int) -> dict[str, str]:
    original = {as_text(k): as_text(v) for k, v in fields.items()}
    payload = {
        "original_stream": stream,
        "original_group": group,
        "original_message_id": message_id,
        "fields": dict(sorted(original.items())),
        "failure_kind": str(failure_kind),
        "error": error,
        "delivery_count": delivery_count,
    }
    return {
        "event_id": event_id(tenant_id, group, stream, message_id),
        "tenant_id": tenant_id,
        "occurred_at": occurred_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "producer": producer,
        "schema_version": str(SCHEMA_VERSION),
        "payload": json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
    }
