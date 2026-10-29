-- V15__add_scheduler_tracker_kind_and_source_run.sql
-- Adds run-level discriminator (kind) and lineage pointer (source_run_id)
-- to scheduler_tracker. Foundational for the four-feature run taxonomy
-- (umbrella: docs/superpowers/specs/2026-05-07-run-semantics-taxonomy-design.md).
--
-- All pre-V15 rows default to kind='cron'. Provenance for pre-V15 reruns
-- is unrecoverable; documented audit-fidelity loss in the design spec.

ALTER TABLE scheduler_tracker
    ADD COLUMN kind character varying(20) NOT NULL DEFAULT 'cron',
    ADD COLUMN source_run_id uuid NULL;

ALTER TABLE scheduler_tracker
    ADD CONSTRAINT scheduler_tracker_kind_check
    CHECK (kind IN ('cron', 'trigger', 'rerun', 'rebase', 'single_node_run'));

CREATE INDEX idx_scheduler_tracker_kind ON scheduler_tracker (kind);
CREATE INDEX idx_scheduler_tracker_source_run_id ON scheduler_tracker (source_run_id)
    WHERE source_run_id IS NOT NULL;

COMMENT ON COLUMN scheduler_tracker.kind IS
    'Run discriminator. Allowed: cron, trigger, rerun, rebase, single_node_run. Written at activation time. The rerun handler may flip kind from ''cron'' to ''rerun'' on TriggerRerun; all other paths treat it as immutable after write.';
COMMENT ON COLUMN scheduler_tracker.source_run_id IS
    'Lineage pointer to the run this one derives from (rerun, rebase, stale-mode single_node_run). NULL for cron/trigger. Not a foreign key — orphans are fine.';
