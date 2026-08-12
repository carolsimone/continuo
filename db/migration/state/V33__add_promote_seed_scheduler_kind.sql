-- Allow 'promote_seed' as a run kind.
--
-- Promoting a release materialises the seeds that release changed into the
-- production schema. That work used to be dispatched without a run behind it,
-- which left it outside the run lifecycle: a failed seed build was never
-- observed, never retried despite carrying a retry budget, and never surfaced.
-- It now mints a run like every other dispatch, and this is the kind it carries.

ALTER TABLE scheduler_tracker
    DROP CONSTRAINT IF EXISTS scheduler_tracker_kind_check;

ALTER TABLE scheduler_tracker
    ADD CONSTRAINT scheduler_tracker_kind_check
    CHECK (kind IN ('cron', 'trigger', 'rerun', 'rebase', 'single_node_run', 'promote_seed'));

COMMENT ON COLUMN scheduler_tracker.kind IS
    'Run discriminator. Allowed: cron, trigger, rerun, rebase, single_node_run, promote_seed. Written once at activation time and immutable thereafter; a rerun/rebase mints a new row with its own kind rather than mutating the source.';
