-- Stuck Entry Resolver Index
-- Creates partial index for efficient detection of stuck outbox entries

CREATE INDEX idx_k8s_status_outbox_stuck
ON k8s_status_outbox(created_at)
WHERE status = 'pending' AND outbox_retry_count >= max_retries;

COMMENT ON INDEX idx_k8s_status_outbox_stuck IS 'Index for detecting stuck entries that need manual intervention';
