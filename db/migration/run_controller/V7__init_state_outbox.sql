CREATE TABLE IF NOT EXISTS state_outbox (
    id              UUID PRIMARY KEY,
    aggregate_type  TEXT NOT NULL,
    aggregate_id    UUID NOT NULL,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    stream_name     TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    max_retries     INT  NOT NULL DEFAULT 3,
    retry_count     INT  NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
