-- Dead letters of every source: stream consumers that gave up on a message,
-- outbox relays that gave up on a row, and stream entries the retention cap
-- removed before a consumer group finished them. One row per dead letter,
-- identified by dedup_key so a redelivered or re-quarantined entry is stored
-- once.
CREATE TABLE dead_letters (
    id                   UUID        PRIMARY KEY,
    dedup_key            TEXT        NOT NULL,
    source               TEXT        NOT NULL,
    failure_kind         TEXT        NOT NULL,
    stream               TEXT        NOT NULL,
    consumer_group       TEXT        NOT NULL DEFAULT '',
    original_message_id  TEXT        NOT NULL DEFAULT '',
    producer             TEXT        NOT NULL DEFAULT '',
    error                TEXT        NOT NULL DEFAULT '',
    delivery_count       BIGINT      NOT NULL DEFAULT 0,
    fields               JSONB       NOT NULL DEFAULT '{}'::jsonb,
    redrivable           BOOLEAN     NOT NULL,
    original_event_type  TEXT        NOT NULL DEFAULT '',
    failed_outbox_id     TEXT        NOT NULL DEFAULT '',
    original_at          TIMESTAMPTZ NOT NULL,
    recorded_at          TIMESTAMPTZ NOT NULL,
    status               TEXT        NOT NULL DEFAULT 'open',
    redriven_by          TEXT        NULL,
    redrive_reason       TEXT        NULL,
    redriven_at          TIMESTAMPTZ NULL,
    CONSTRAINT dead_letters_dedup_key_key UNIQUE (dedup_key),
    CONSTRAINT dead_letters_source_check CHECK (source IN ('consumer', 'outbox', 'quarantine')),
    CONSTRAINT dead_letters_failure_kind_check CHECK (failure_kind IN ('permanent', 'transient_exhausted', 'trimmed')),
    CONSTRAINT dead_letters_status_check CHECK (status IN ('open', 'redriven')),
    CONSTRAINT dead_letters_redrive_check CHECK (
        (status = 'open' AND redriven_at IS NULL) OR
        (status = 'redriven' AND redriven_at IS NOT NULL AND redriven_by IS NOT NULL AND redrive_reason IS NOT NULL))
);

CREATE INDEX idx_dead_letters_open ON dead_letters (recorded_at DESC) WHERE status = 'open';
CREATE INDEX idx_dead_letters_stream ON dead_letters (stream, recorded_at DESC);
CREATE INDEX idx_dead_letters_original_at ON dead_letters (original_at);

COMMENT ON TABLE dead_letters IS 'Dead letters from stream consumers, outbox relays and the stream trim quarantine';
COMMENT ON COLUMN dead_letters.fields IS 'Stream fields a redrive publishes; for an unreadable dead letter, its raw fields (redrivable = false)';
COMMENT ON COLUMN dead_letters.original_at IS 'When the original message was produced; a dead letter is redrivable until this is 30 days old';

-- The outbox of redrive events: one row per redriven dead letter, committed
-- with the dead letter's status change and published by the relay.
CREATE TABLE dead_letter_outbox (
    id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    message_processing_id  UUID        NULL,
    aggregate_type         TEXT        NOT NULL,
    aggregate_id           UUID        NOT NULL,
    event_type             TEXT        NOT NULL,
    payload                JSONB       NOT NULL,
    stream_name            TEXT        NOT NULL,
    status                 TEXT        NOT NULL DEFAULT 'pending',
    retry_count            INT         NOT NULL DEFAULT 0,
    max_retries            INT         NOT NULL DEFAULT 13,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at           TIMESTAMPTZ NULL,
    error_message          TEXT        NULL,
    next_attempt_at        TIMESTAMPTZ NULL,
    CONSTRAINT dead_letter_outbox_status_check
        CHECK (status IN ('pending', 'scheduled', 'processed', 'failed'))
);

-- The outbox relay claims due rows with
--   WHERE status IN ('pending', 'scheduled') ... ORDER BY created_at, id
-- and withholds a row while an older row of its aggregate is still open, so
-- both predicates get a partial index of their own. Dead-lettered rows
-- ('failed') are counted for metrics through a third.
CREATE INDEX IF NOT EXISTS idx_dead_letter_outbox_claimable
    ON dead_letter_outbox (created_at, id)
    WHERE status IN ('pending', 'scheduled');

CREATE INDEX IF NOT EXISTS idx_dead_letter_outbox_open_by_aggregate
    ON dead_letter_outbox (aggregate_type, aggregate_id, created_at)
    WHERE status IN ('pending', 'scheduled');

CREATE INDEX IF NOT EXISTS idx_dead_letter_outbox_failed
    ON dead_letter_outbox (created_at)
    WHERE status = 'failed';

-- Wakes the relay listening on the channel named after the table. Postgres
-- delivers the notification when the inserting transaction commits, once per
-- transaction however many rows it inserted.
CREATE OR REPLACE FUNCTION continuo_outbox_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(TG_TABLE_NAME, '');
    RETURN NULL;
END;
$$;

CREATE TRIGGER dead_letter_outbox_notify
    AFTER INSERT ON dead_letter_outbox
    FOR EACH STATEMENT
    EXECUTE FUNCTION continuo_outbox_notify();
