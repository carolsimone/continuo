-- scheduler_tracker.last_heartbeat_at is a run's progress clock: state stamps
-- it when a task of the run is dispatched and whenever a task of the run moves.
-- state's attempt reconcile fails an active run that has no attempt in flight
-- and whose clock is more than five minutes old as stalled. Comment only; no
-- schema change.

COMMENT ON COLUMN scheduler_tracker.last_heartbeat_at IS 'The run''s progress clock: stamped when a task of the run is dispatched or moves; state fails an active run with nothing in flight whose clock is more than five minutes old as stalled';
