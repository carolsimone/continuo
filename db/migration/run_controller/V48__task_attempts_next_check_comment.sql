-- next_check_at is written by state: an attempt gets it when state dispatches
-- the attempt; each non-final report state records for the attempt moves it to
-- that status's window; each time state asks execution-controller about the
-- attempt moves it to the next window. It is empty once the attempt has ended
-- or was cancelled. Comment only; no schema change.

COMMENT ON COLUMN task_attempts.next_check_at IS 'When state next asks execution-controller about the attempt: set when the attempt is dispatched, moved by each non-final report state records for the attempt and each time state asks; NULL once the attempt has ended or was cancelled';
