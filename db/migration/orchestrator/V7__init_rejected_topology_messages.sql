-- db/migration/orchestrator/V7__init_rejected_topology_messages.sql
CREATE TABLE rejected_topology_messages (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id  text        NOT NULL,
    reason      text        NOT NULL,
    payload     jsonb       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_rejected_topology_created_at
    ON rejected_topology_messages (created_at DESC);
