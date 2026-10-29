-- V17: add inherited_from_task_id marker to task_tracker.
-- NULL = real execution. Non-NULL = inherited from a rebase parent run; points
-- to the ROOT executed task_id (resolve-to-root semantics — chain depth ≤ 1
-- forever, including rebase-of-rebase). No FK: orphaned pointers are fine; same
-- rationale as scheduler_tracker.source_run_id (V15).
ALTER TABLE task_tracker
    ADD COLUMN inherited_from_task_id uuid NULL;

CREATE INDEX idx_task_tracker_inherited_from
    ON task_tracker (inherited_from_task_id)
    WHERE inherited_from_task_id IS NOT NULL;

COMMENT ON COLUMN task_tracker.inherited_from_task_id IS
    'Lineage marker for inherited tasks in rebase runs. NULL = task was actually executed in this run. Non-NULL = task projected as SUCCEEDED from a rebase parent; points to the ROOT executed task_id (resolve-to-root semantics — chain depth ≤ 1 even across rebase-of-rebase). Not a foreign key — orphaned pointers are fine.';
