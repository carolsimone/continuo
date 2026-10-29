-- scheduler_tracker.last_heartbeat_at is a run's progress clock. The Run
-- aggregate stamps it when the run is dispatched and whenever a task status
-- change is applied; orchestrator's dispatch watchdog cancels an active run
-- whose clock (or created_at, before its first progress) is older than its
-- no-progress window and that has no task running.
--
-- Every active run gets a fresh stamp so each starts a full no-progress window
-- from this migration rather than from a stamp the aggregate never wrote.

COMMENT ON COLUMN scheduler_tracker.last_heartbeat_at IS
    'Last lifecycle progress: dispatch or an applied task status change; the dispatch watchdog''s stall clock.';

UPDATE scheduler_tracker
   SET last_heartbeat_at = now()
 WHERE status IN ('pending', 'running');
