-- next_attempt_at gates when a transiently-failed pending row becomes eligible
-- for another publish attempt. NULL means "due now" (every existing pending row
-- stays immediately eligible, so no backfill is needed). The processor sets it
-- to NOW() + backoff(retry_count) on a transient failure and never selects a row
-- whose next_attempt_at is still in the future.
ALTER TABLE state_outbox
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NULL;

-- Partial index over scheduled (backed-off) rows keeps GetPendingBatch's
-- due-gate cheap: these are the rows with a meaningful next_attempt_at to
-- range-scan. Fresh pending rows all have NULL next_attempt_at and stay
-- covered by idx_state_outbox_pending (created_at, WHERE status='pending')
-- from the canonical migration.
CREATE INDEX IF NOT EXISTS idx_state_outbox_due
    ON state_outbox (next_attempt_at)
    WHERE status = 'scheduled';

-- Align the DB fallback default with the outbox processor's retry budget
-- (DefaultMaxRetries = 13), so a raw INSERT that omits max_retries still
-- gets the current retry budget instead of a stale value.
ALTER TABLE state_outbox ALTER COLUMN max_retries SET DEFAULT 13;

-- 'scheduled' marks a transiently-failed row awaiting its backoff deadline.
-- It is a distinct status from 'pending' specifically so a previous-version
-- replica's `status = 'pending'` reader (rolling deploy) does not see the row
-- and cannot reclaim/retry it before next_attempt_at elapses.
ALTER TABLE state_outbox
    DROP CONSTRAINT IF EXISTS state_outbox_status_check;
ALTER TABLE state_outbox
    ADD CONSTRAINT state_outbox_status_check
    CHECK (status IN ('pending', 'scheduled', 'processed', 'failed'));
