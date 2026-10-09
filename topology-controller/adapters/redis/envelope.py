"""The header every envelope stream entry carries, rendered as
pkg/events.Envelope.Fields renders it: each header field a Redis stream field
of its own, next to a "payload" field holding the entry's payload as compact
JSON."""

import json
import uuid
from datetime import datetime, timezone

# The UUIDv5 namespace of every event id derived from a natural key; the same
# value as pkg/events.EventIDNamespace.
EVENT_ID_NAMESPACE = uuid.UUID("2f0c5a7e-8d14-4b63-a1f9-6c3e5b2d7a40")


def envelope_fields(*, event_id: str, tenant_id: str, occurred_at: datetime, producer: str,
                    schema_version: int, payload: dict) -> dict[str, str]:
    """The Redis fields of one envelope entry: occurred_at in UTC with
    microseconds, schema_version as text, the payload as compact UTF-8 JSON."""
    return {
        "event_id": event_id,
        "tenant_id": tenant_id,
        "occurred_at": occurred_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "producer": producer,
        "schema_version": str(schema_version),
        "payload": json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
    }
