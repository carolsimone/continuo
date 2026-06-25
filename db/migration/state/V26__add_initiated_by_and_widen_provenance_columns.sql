-- V26__add_initiated_by_and_widen_provenance_columns.sql
-- Records the user who initiated each run (full provenance) and widens every
-- column that stores a user identifier to TEXT.
--
-- The stored value is the stable identifier minted at the HTTP edge
-- (ui-service: "issuer-host|sub"). An OIDC `sub` may itself be up to 255
-- characters, so "issuer-host|sub" can exceed the original
-- character varying(255) columns and would be rejected with "value too long".
-- TEXT removes the limit for every column that now carries this identifier:
-- the new scheduler_tracker.initiated_by, and the pre-existing cancelled_by
-- columns on scheduler_tracker, task_tracker, and task_execution (the cancel
-- cascade propagates the cancelling user's id from the run down into the task
-- rows).
--
-- Cron and other platform-initiated runs carry the 'system' sentinel rather
-- than a real account, so every row has a non-null initiator. Pre-V26 rows
-- default to 'system': their initiator predates provenance tracking and is not
-- recoverable.

ALTER TABLE scheduler_tracker
    ADD COLUMN initiated_by text NOT NULL DEFAULT 'system';

ALTER TABLE scheduler_tracker ALTER COLUMN cancelled_by TYPE text;
ALTER TABLE task_tracker      ALTER COLUMN cancelled_by TYPE text;
ALTER TABLE task_execution    ALTER COLUMN cancelled_by TYPE text;

CREATE INDEX idx_scheduler_tracker_initiated_by ON scheduler_tracker (initiated_by);

COMMENT ON COLUMN scheduler_tracker.initiated_by IS
    'User who initiated the run, or ''system'' for cron / platform-initiated runs. Stamped at creation from the gRPC initiating-user metadata header; immutable thereafter.';
