-- V36__drop_task_tracker_manifest_version.sql
-- task_tracker.manifest_version has been unused and empty since V35, and no
-- running replica names it any more, so it is dropped.
--
-- Safe only once every state pod runs code that never references the column.
-- An install upgrading straight from a release that still names it, through
-- this migration, would fail those pods mid-upgrade; the changelog directs
-- operators to upgrade through 0.8.0 first.

ALTER TABLE task_tracker
    DROP COLUMN IF EXISTS manifest_version;
