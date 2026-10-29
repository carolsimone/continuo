-- tenant_id leads every key of the run tables and of the schedule catalog.
-- runs, tasks, task_attempts, schedule_catalog and schedule_catalog_state gain
-- a tenant_id column; node_leases already has one. Every existing row belongs
-- to the default tenant. The column is NOT NULL with no default, so an insert
-- that leaves the tenant out fails.
--
-- Each column is added with the default 'default' and the default is then
-- dropped: Postgres stores that value once for the rows that exist when the
-- column is added, rather than rewriting every row, and an insert after this
-- migration must name its tenant.
--
-- Keys: a run is (tenant_id, run_id), a task (tenant_id, task_id) and an
-- attempt (tenant_id, task_id, attempt). The foreign keys that tie a task to
-- its run, and an attempt and a node lease to its task, include the tenant, so
-- the database refuses a row whose tenant differs from its parent's. A
-- schedule has one active run per tenant, and a schedule's runs and the node
-- lease waiters are indexed per tenant. The constraints and indexes keep
-- their names.

ALTER TABLE runs ADD COLUMN tenant_id text NOT NULL DEFAULT 'default';
ALTER TABLE runs ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE tasks ADD COLUMN tenant_id text NOT NULL DEFAULT 'default';
ALTER TABLE tasks ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE task_attempts ADD COLUMN tenant_id text NOT NULL DEFAULT 'default';
ALTER TABLE task_attempts ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE schedule_catalog ADD COLUMN tenant_id text NOT NULL DEFAULT 'default';
ALTER TABLE schedule_catalog ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE schedule_catalog_state ADD COLUMN tenant_id text NOT NULL DEFAULT 'default';
ALTER TABLE schedule_catalog_state ALTER COLUMN tenant_id DROP DEFAULT;

COMMENT ON COLUMN runs.tenant_id IS 'Tenant of the run; leads the run''s key';
COMMENT ON COLUMN tasks.tenant_id IS 'Tenant of the task, the tenant of its run; leads the task''s key';
COMMENT ON COLUMN task_attempts.tenant_id IS 'Tenant of the attempt, the tenant of its task; leads the attempt''s key';
COMMENT ON COLUMN schedule_catalog.tenant_id IS 'Tenant whose releases hold the schedule';
COMMENT ON COLUMN schedule_catalog_state.tenant_id IS 'Tenant whose schedule catalog the row describes';

-- Every task belongs to the default tenant, so a node lease recorded under
-- another tenant names a task outside that tenant: it is released here, as the
-- lease's foreign key to its task now requires.
DELETE FROM node_leases WHERE tenant_id <> 'default';

-- The foreign keys go first: each depends on the key it references.
ALTER TABLE node_leases   DROP CONSTRAINT node_leases_task_id_fkey;
ALTER TABLE node_leases   DROP CONSTRAINT node_leases_task_id_key;
ALTER TABLE task_attempts DROP CONSTRAINT task_attempts_task_id_fkey;
ALTER TABLE tasks         DROP CONSTRAINT tasks_run_id_fkey;

ALTER TABLE runs DROP CONSTRAINT runs_pkey;
ALTER TABLE runs ADD CONSTRAINT runs_pkey PRIMARY KEY (tenant_id, run_id);
DROP INDEX uq_runs_active_per_schedule;
CREATE UNIQUE INDEX uq_runs_active_per_schedule
    ON runs (tenant_id, schedule_name)
    WHERE status IN ('pending', 'running', 'cancelling');
COMMENT ON INDEX uq_runs_active_per_schedule IS
    'Enforces at most one active (pending|running|cancelling) run per schedule within a tenant; the DB-level backstop for the activation TOCTOU race.';
-- Every read of a schedule's runs names its tenant: the latest run of each
-- schedule and a schedule's completed runs, newest first, walk this index.
DROP INDEX idx_runs_schedule_name_created;
CREATE INDEX idx_runs_schedule_name_created ON runs (tenant_id, schedule_name, created_at DESC);
COMMENT ON INDEX idx_runs_schedule_name_created IS
    'Serves the schedule_name-keyed active-run lookups and the DISTINCT ON last-run-per-schedule scan within a tenant.';

ALTER TABLE tasks DROP CONSTRAINT tasks_pkey;
ALTER TABLE tasks ADD CONSTRAINT tasks_pkey PRIMARY KEY (tenant_id, task_id);
DROP INDEX uq_tasks_run_node;
CREATE UNIQUE INDEX uq_tasks_run_node
    ON tasks (tenant_id, run_id, node_key)
    WHERE node_key IS NOT NULL;
ALTER TABLE tasks ADD CONSTRAINT tasks_run_id_fkey
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE;
DROP INDEX idx_tasks_lease_waiters;
CREATE INDEX idx_tasks_lease_waiters
    ON tasks (tenant_id, node_key, lease_wait_since)
    WHERE wait_reason = 'node_lease';
COMMENT ON INDEX idx_tasks_lease_waiters IS
    'The tasks waiting for a node lease, oldest first per tenant and node; the waker grants from it.';

ALTER TABLE task_attempts DROP CONSTRAINT task_attempts_pkey;
ALTER TABLE task_attempts ADD CONSTRAINT task_attempts_pkey PRIMARY KEY (tenant_id, task_id, attempt);
ALTER TABLE task_attempts ADD CONSTRAINT task_attempts_task_id_fkey
    FOREIGN KEY (tenant_id, task_id) REFERENCES tasks (tenant_id, task_id) ON DELETE CASCADE;

ALTER TABLE node_leases ADD CONSTRAINT node_leases_task_id_key UNIQUE (tenant_id, task_id);
ALTER TABLE node_leases ADD CONSTRAINT node_leases_task_id_fkey
    FOREIGN KEY (tenant_id, task_id) REFERENCES tasks (tenant_id, task_id) ON DELETE CASCADE;
COMMENT ON TABLE node_leases IS
    'The write lease on a node: at most one row per tenant and node, and one per task. Deleting the row (or the task) releases the node.';

ALTER TABLE schedule_catalog DROP CONSTRAINT schedule_catalog_pkey;
ALTER TABLE schedule_catalog ADD CONSTRAINT schedule_catalog_pkey PRIMARY KEY (tenant_id, schedule_name);

-- schedule_catalog_state holds one row per tenant: the promotion seq of the
-- last schedules.loaded:v1 payload that tenant's catalog applied.
ALTER TABLE schedule_catalog_state DROP CONSTRAINT schedule_catalog_state_pkey;
ALTER TABLE schedule_catalog_state DROP CONSTRAINT schedule_catalog_state_singleton;
ALTER TABLE schedule_catalog_state DROP COLUMN id;
ALTER TABLE schedule_catalog_state ADD CONSTRAINT schedule_catalog_state_pkey PRIMARY KEY (tenant_id);
COMMENT ON TABLE schedule_catalog_state IS
    'The promotion seq of the last schedules.loaded:v1 payload each tenant''s schedule catalog applied; one row per tenant';

-- A dispatch or cancel row written before this migration stores its payload
-- alone and belongs to the default tenant: it is wrapped here, so a row still
-- pending publishes as it would have. A row already so wrapped is left as it
-- is.
UPDATE state_outbox
   SET payload = jsonb_build_object('tenant_id', 'default', 'payload', payload)
 WHERE event_type IN ('task.dispatch:v1', 'run.cancelled:v1')
   AND NOT (payload ? 'tenant_id' AND payload ? 'payload');
