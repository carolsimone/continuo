-- state plans, dispatches and finalizes every run and execution-controller
-- drops the dispatches and Job results of a cancelled run, so orchestrator
-- consumes no run stream and keeps no record of cancelled runs. Nothing reads
-- or writes this table.
DROP TABLE IF EXISTS cancelled_schedules;
