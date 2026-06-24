-- V26__add_scheduler_tracker_initiated_by.sql
-- Records the user who initiated each run (full provenance). The value is the
-- stable identifier minted at the HTTP edge (ui-service: "issuer-host|sub"),
-- carried into state as gRPC metadata and stamped at run-creation time.
--
-- Cron and other platform-initiated runs carry the 'system' sentinel rather
-- than a real account, so every row has a non-null initiator. Pre-V26 rows
-- default to 'system': their initiator predates provenance tracking and is not
-- recoverable.

ALTER TABLE scheduler_tracker
    ADD COLUMN initiated_by character varying(255) NOT NULL DEFAULT 'system';

CREATE INDEX idx_scheduler_tracker_initiated_by ON scheduler_tracker (initiated_by);

COMMENT ON COLUMN scheduler_tracker.initiated_by IS
    'User who initiated the run, or ''system'' for cron / platform-initiated runs. Stamped at creation from the gRPC initiating-user metadata header; immutable thereafter.';
