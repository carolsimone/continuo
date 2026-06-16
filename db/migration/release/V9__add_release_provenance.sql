-- Provenance (source repo + commit SHA) is required on releases going forward.
-- Existing release rows are transient CD history with no provenance, so they are
-- cleared before the NOT NULL columns are added. current_prod / service_prod have
-- no foreign key to releases and are intentionally left intact, preserving the
-- live production pointers.
DELETE FROM releases;

ALTER TABLE releases
    ADD COLUMN repo       text NOT NULL,
    ADD COLUMN commit_sha text NOT NULL;
