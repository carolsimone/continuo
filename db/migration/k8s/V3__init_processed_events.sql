-- Processed Events Table
-- Deduplication table to prevent duplicate processing of node.deployed:v1
-- and check.k8s:v1 messages (mirrors executor-controller's processed_events table).

CREATE TABLE IF NOT EXISTS processed_events (
    outbox_entry_id UUID        PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_k8s_processed_events_processed_at
    ON processed_events(processed_at);

COMMENT ON TABLE processed_events IS 'Tracks processed outbox entries to prevent duplicate k8s status processing';
COMMENT ON COLUMN processed_events.outbox_entry_id IS 'UUID of the upstream outbox entry (DeploymentOutboxEntry.ID or K8sStatusOutboxEntry.ID)';
