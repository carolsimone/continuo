-- Adds a nullable, application-level idempotency key on deployment_outbox.
-- Sits beside the shared (msg.ID, stream_name) dedup row in
-- message_processing. The two layers protect against different failure
-- modes:
--
--   * message_processing dedups Redis-level redeliveries: same msg.ID
--     arriving twice on the same stream (PEL reclaim, consumer crash).
--
--   * deployment_outbox.outbox_entry_id dedups publisher-side retries:
--     when the orchestrator's outbox processor sees an ambiguous XADD
--     ("succeeded but error returned"), it re-publishes the same payload
--     and Redis assigns a new msg.ID. The shared dedup row treats the
--     two deliveries as distinct (different msg.IDs), so without this
--     column both would insert separate deployment_outbox rows and
--     produce duplicate K8s jobs.
--
-- The column is nullable because retry.task:v1 does not currently carry
-- an outbox_entry_id on the wire (k8s-controller's TaskRetry payload).
-- The partial unique index treats NULLs as distinct, so retry.task
-- messages never collide on this index.

ALTER TABLE deployment_outbox ADD COLUMN IF NOT EXISTS outbox_entry_id UUID;

CREATE UNIQUE INDEX IF NOT EXISTS deployment_outbox_outbox_entry_id_key
    ON deployment_outbox(outbox_entry_id)
    WHERE outbox_entry_id IS NOT NULL;

COMMENT ON COLUMN deployment_outbox.outbox_entry_id IS
    'Application-level idempotency key copied from the inbound message payload (orchestrator''s outbox row ID). Nullable when the publisher does not carry one.';
