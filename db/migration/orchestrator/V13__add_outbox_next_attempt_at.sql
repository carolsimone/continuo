-- next_attempt_at gates when a transiently-failed pending row becomes eligible
-- for another publish attempt. NULL means "due now" (every existing pending row
-- stays immediately eligible, so no backfill is needed). The processor sets it
-- to NOW() + backoff(retry_count) on a transient failure and never selects a row
-- whose next_attempt_at is still in the future.
ALTER TABLE orchestrator_outbox
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NULL;

-- Partial index over due pending rows keeps GetPendingBatch's due-gate cheap.
CREATE INDEX IF NOT EXISTS idx_orchestrator_outbox_due
    ON orchestrator_outbox (next_attempt_at)
    WHERE status = 'pending';
