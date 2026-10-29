-- db/migration/state/V6__init_schedule_catalog.sql
-- Durable catalog of schedule names discovered from dbt manifests.
-- Populated by the state service consuming schedules.loaded:v1 events.
-- removed_at IS NULL = active; non-NULL = soft-deleted (removed from manifests).
-- Retained after removal so scheduler_tracker history remains joinable.

CREATE TABLE IF NOT EXISTS schedule_catalog (
    schedule_name VARCHAR(50)  PRIMARY KEY,
    first_seen_at TIMESTAMPTZ  NOT NULL,
    last_seen_at  TIMESTAMPTZ  NOT NULL,
    removed_at    TIMESTAMPTZ
);
