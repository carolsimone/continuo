CREATE TABLE rejected_topology_messages (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id  text        NOT NULL,
    reason      text        NOT NULL,
    payload     jsonb       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_rejected_topology_messages_created_at
    ON rejected_topology_messages (created_at DESC);

COMMENT ON TABLE rejected_topology_messages IS
    'Forensic sink for permanently rejected manifest.loaded:v1 payloads. Written non-transactionally by the orchestrator ingest validator before ACKing the source Redis message. Intentionally has NO UNIQUE(message_id) — XCLAIM redelivery must not raise a unique violation that would re-classify a permanent error as transient. Intentionally has NO foreign keys — these rows are forensic, not authoritative. Retention sweep deferred (see image-tag-stuck-runs plan).';
COMMENT ON COLUMN rejected_topology_messages.message_id IS
    'Redis Stream message ID of the rejected payload (e.g. 1738756432123-0). Not unique — same ID may appear multiple times under XCLAIM redelivery.';
COMMENT ON COLUMN rejected_topology_messages.reason IS
    'Validator rejection reason, human-readable. Format starts with the offender summary (e.g. "image_tag empty for N node(s): svc/schema/table, ...").';
COMMENT ON COLUMN rejected_topology_messages.payload IS
    'Full original manifest.loaded:v1 payload as JSON, preserved verbatim for offline replay/debugging.';
