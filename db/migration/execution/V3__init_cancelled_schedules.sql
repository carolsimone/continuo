-- Guard table fed by schedule.cancelled:v1. A dispatch or a Job result for a
-- cancelled schedule is dropped. Rows are swept after a configurable TTL.
CREATE TABLE cancelled_schedules (
    schedule_id  UUID        PRIMARY KEY,
    cancelled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
