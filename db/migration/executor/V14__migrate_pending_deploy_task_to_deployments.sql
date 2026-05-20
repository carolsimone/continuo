-- Pre-#68 versions wrote deploy intents as executor_outbox rows with
-- event_type='deploy_task', whose JSONB payload carried the deploy fields. The
-- current architecture deploys from the executor_deployments command queue
-- instead, and the collapsed publisher has no 'deploy_task' branch — so a
-- pending legacy row would be routed to the unknown-event-type path and marked
-- failed, dropping an in-flight deploy.
--
-- To keep in-flight deploys across a rolling upgrade, migrate any pending
-- deploy_task rows into executor_deployments and neutralise the originals. The
-- legacy deploy_task payload is field-identical to the deployer DeployTask
-- command, so it is stored directly as job_params. On a fresh database (no
-- legacy rows) both statements are no-ops.

INSERT INTO executor_deployments (
    id, message_processing_id, task_id, schedule_id, job_params,
    status, retry_count, max_retries, next_attempt_at, created_at
)
SELECT
    gen_random_uuid(),
    message_processing_id,
    (payload->>'task_id')::uuid,
    (payload->>'schedule_id')::uuid,
    payload,
    'pending',
    0,
    3,
    NOW(),
    created_at
FROM executor_outbox
WHERE event_type = 'deploy_task'
  AND status = 'pending'
  AND payload->>'task_id' IS NOT NULL
  AND payload->>'schedule_id' IS NOT NULL;

-- Neutralise the migrated rows so the publisher never sees them.
UPDATE executor_outbox
SET status = 'processed', processed_at = NOW()
WHERE event_type = 'deploy_task' AND status = 'pending';
