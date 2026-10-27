-- A deployment names the tenant of the task.dispatch:v1 it runs, and every
-- report written for it is published under that tenant. Every existing
-- deployment belongs to the default tenant; the column is NOT NULL with no
-- default, so an insert that leaves the tenant out fails.
--
-- The column is added with the default 'default' and the default is then
-- dropped: Postgres stores that value once for the rows that exist when the
-- column is added, rather than rewriting every row.
ALTER TABLE deployments ADD COLUMN tenant_id text NOT NULL DEFAULT 'default';
ALTER TABLE deployments ALTER COLUMN tenant_id DROP DEFAULT;

COMMENT ON COLUMN deployments.tenant_id IS
    'Tenant of the task.dispatch:v1 the deployment runs; its reports are published under it';

-- A task report row stores its tenant beside its payload, as
-- {"tenant_id": ..., "payload": ...}. A report row written before this
-- migration stores the report alone and belongs to the default tenant: it is
-- wrapped here, so it publishes as it would have. A row already so wrapped is
-- left as it is.
UPDATE execution_outbox
   SET payload = jsonb_build_object('tenant_id', 'default', 'payload', payload)
 WHERE event_type = 'task_status_report'
   AND NOT (payload ? 'tenant_id' AND payload ? 'payload');
