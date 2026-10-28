-- A schedule's version counts the operator's edits from 1: a create is version 1
-- and each edit raises it by one. Saving an edit compares the version the operator
-- read with the stored row's, and creating a schedule saves it at expected
-- version 0, so a row stored at version 0 or below, such as one written by hand,
-- matches no edit and can only be deleted. The check refuses such a row.
--
-- A row already at version 0 or below is moved to version 1, so the constraint
-- can be added and the schedule can be edited again.
UPDATE schedules SET version = 1 WHERE version < 1;

ALTER TABLE schedules
    ADD CONSTRAINT schedules_version_positive CHECK (version >= 1);
COMMENT ON CONSTRAINT schedules_version_positive ON schedules IS
    'A schedule''s version counts the operator''s edits from 1';
