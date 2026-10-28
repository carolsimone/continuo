-- schedules holds the cron schedules state fires, one row per tenant and name.
-- A schedule carries its cron expression and the time zone that expression is
-- read in, whether it is paused, and when it fires next; version counts the
-- changes made to the row, and updated_by names who made the last one.
-- next_fire_at is NULL while the schedule is paused, and while it is set it is
-- the instant of the schedule's next slot.
CREATE TABLE schedules (
    tenant_id    text        NOT NULL,
    name         text        NOT NULL,
    cron         text        NOT NULL,
    timezone     text        NOT NULL,
    description  text        NOT NULL DEFAULT '',
    paused       boolean     NOT NULL DEFAULT false,
    next_fire_at timestamptz NULL,
    version      bigint      NOT NULL DEFAULT 1,
    updated_by   text        NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT schedules_pkey PRIMARY KEY (tenant_id, name)
);

COMMENT ON TABLE schedules IS 'The cron schedules state fires, one row per tenant and schedule name';
COMMENT ON COLUMN schedules.tenant_id IS 'Tenant that owns the schedule; leads the key, so another tenant may use the same name';
COMMENT ON COLUMN schedules.name IS 'Name of the schedule, the schedule_name of the runs it fires';
COMMENT ON COLUMN schedules.cron IS 'Cron expression that says when the schedule fires, read in timezone';
COMMENT ON COLUMN schedules.timezone IS 'IANA time zone name the cron expression is read in';
COMMENT ON COLUMN schedules.description IS 'Free text that says what the schedule is for; empty when none was given';
COMMENT ON COLUMN schedules.paused IS 'Whether the schedule is paused; a paused schedule fires nothing';
COMMENT ON COLUMN schedules.next_fire_at IS 'Instant of the next slot the schedule fires; NULL while the schedule is paused';
COMMENT ON COLUMN schedules.version IS 'Counts the changes to the schedule: 1 when it is created, raised by each change after';
COMMENT ON COLUMN schedules.updated_by IS 'Who made the last change to the schedule';
COMMENT ON COLUMN schedules.updated_at IS 'When the last change to the schedule was made';

-- A change to a schedule wakes the loop that fires them, so it re-reads the
-- next fire without waiting for its next pass. The trigger is statement-level,
-- so a statement that changes many rows sends one notification, delivered when
-- its transaction commits. A statement that matches no row notifies too; the
-- loop only re-reads, so the extra wake does no harm.
CREATE FUNCTION notify_schedules_changed() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('schedules', '');
    RETURN NULL;
END;
$$;
COMMENT ON FUNCTION notify_schedules_changed() IS
    'Notifies the schedules channel after a statement has inserted, updated or deleted schedules';

CREATE TRIGGER schedules_changed
    AFTER INSERT OR UPDATE OR DELETE ON schedules
    FOR EACH STATEMENT EXECUTE FUNCTION notify_schedules_changed();

-- A scheduled run records the slot it was fired for: the instant the schedule's
-- cron expression named, not the time the run was made. A run that no schedule
-- fired (a manual trigger, a rerun, a promote-seed run) has no slot. At most one
-- run exists per schedule slot, so a fire that is made twice, by a restarted
-- loop or by two replicas, creates one run.
ALTER TABLE runs ADD COLUMN scheduled_for timestamptz NULL;
COMMENT ON COLUMN runs.scheduled_for IS 'Slot of the schedule this run was fired for, the instant its cron expression named; NULL for a run no schedule fired';

CREATE UNIQUE INDEX uq_runs_scheduled_for
    ON runs (tenant_id, schedule_name, scheduled_for)
    WHERE scheduled_for IS NOT NULL;
COMMENT ON INDEX uq_runs_scheduled_for IS
    'At most one run per schedule slot within a tenant, so a fire made twice creates one run.';
