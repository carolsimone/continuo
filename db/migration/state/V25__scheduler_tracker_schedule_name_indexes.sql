-- scheduler_tracker schedule_name indexes
--
-- Two indexes on the append-only run-history table, both keyed on schedule_name.
--
-- 1. uq_scheduler_tracker_active_per_schedule — partial UNIQUE index enforcing
--    the "at most one active (pending|running) run per schedule" invariant at
--    the database level. This is the correctness backstop for the
--    check-then-act activation path (ActivateSchedule / TriggerRerun /
--    TriggerRebase): two concurrent activations can both pass the
--    HasActiveSchedule pre-check on the autocommit connection and each attempt
--    an INSERT; the unique index lets exactly one win and rejects the other
--    with SQLSTATE 23505, which the repository maps to ErrScheduleHasActiveRun.
--
--    PRE-FLIGHT: existing data must contain no schedule_name with more than one
--    row in status IN ('pending','running'). Verify before applying:
--      SELECT schedule_name, count(*)
--        FROM scheduler_tracker
--       WHERE status IN ('pending','running')
--       GROUP BY schedule_name
--      HAVING count(*) > 1;
--    The migration fails if any double-active rows exist; resolve them first.
--
-- 2. idx_scheduler_tracker_schedule_name_created — supports the hot scheduler
--    read paths keyed on schedule_name:
--      * HasActiveSchedule / GetActiveScheduler:
--          WHERE schedule_name = $1 AND status IN ('pending','running')
--      * GetLastRunPerSchedule (DISTINCT ON ... ORDER BY schedule_name,
--        created_at DESC) — called by ListAllSchedules and the watchdog's
--        ListStuckCandidates server query.
--    The (schedule_name, created_at DESC) ordering lets DISTINCT ON walk one
--    row per schedule without a full-table sort.

CREATE UNIQUE INDEX uq_scheduler_tracker_active_per_schedule
    ON scheduler_tracker (schedule_name)
    WHERE status IN ('pending', 'running');

CREATE INDEX idx_scheduler_tracker_schedule_name_created
    ON scheduler_tracker (schedule_name, created_at DESC);

COMMENT ON INDEX uq_scheduler_tracker_active_per_schedule IS
    'Enforces at most one active (pending|running) run per schedule; the DB-level backstop for the activation TOCTOU race.';
COMMENT ON INDEX idx_scheduler_tracker_schedule_name_created IS
    'Supports schedule_name-keyed active-run lookups and the DISTINCT ON last-run-per-schedule scan.';
