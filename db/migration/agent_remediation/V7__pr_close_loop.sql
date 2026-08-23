-- Close-loop slice: a PR reaches a terminal outcome mirrored from GitHub.
-- pr_state machine: '' -> 'opening' -> 'open' -> 'merged' | 'rejected'
-- ('opening' -> 'failed' stays the retryable error path). 'merged' and
-- 'rejected' are terminal; a PR reopened on GitHub is not tracked.
ALTER TABLE proposal ADD COLUMN pr_closed_at TIMESTAMPTZ NULL;
