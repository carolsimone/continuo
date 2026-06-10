-- V24__correct_scheduler_tracker_kind_comment.sql
-- Corrects the scheduler_tracker.kind column comment originally set in V15.
-- The V15 text claimed "the rerun handler may flip kind from 'cron' to 'rerun'
-- on TriggerRerun", which is false: a rerun or rebase mints a NEW scheduler_tracker
-- row (kind set at creation) and never mutates the source row's kind. V15 is
-- immutable (already applied), so the comment is corrected forward here.

COMMENT ON COLUMN scheduler_tracker.kind IS
    'Run discriminator. Allowed: cron, trigger, rerun, rebase, single_node_run. Written once at activation time and immutable thereafter; a rerun/rebase mints a new row with its own kind rather than mutating the source.';
