-- V21 gives executor_deployments an alternative to one Kubernetes Job per task:
-- a pool of long-lived worker pods that CLAIM a task under an expiring lease.
--
-- A worker's lease is authenticated by a token whose SHA-256 digest alone is
-- stored here; the raw token is returned to the claiming worker once and never
-- persisted, so a database read cannot impersonate a worker.
--
-- slot_reserved_at/slot_released_at record a deployment's hold on one of the
-- executor's shared execution slots. Jobs-mode and workers-mode work draw on the
-- same pool, so a reserved-and-not-released row counts against the concurrency
-- cap whichever path created it.
ALTER TABLE executor_deployments
    DROP CONSTRAINT executor_deployments_status_check,
    ADD COLUMN execution_mode TEXT NOT NULL DEFAULT 'jobs'
        CHECK (execution_mode IN ('jobs', 'workers')),
    ADD COLUMN pool_key TEXT NULL,
    ADD COLUMN resolved_argv JSONB NULL,
    ADD COLUMN execution_path TEXT NULL
        CHECK (execution_path IN ('native', 'wrapper_required', 'wrapper_opaque') OR execution_path IS NULL),
    ADD COLUMN slot_reserved_at TIMESTAMPTZ NULL,
    ADD COLUMN slot_released_at TIMESTAMPTZ NULL,
    ADD COLUMN lease_id UUID NULL,
    ADD COLUMN lease_token_sha256 TEXT NULL,
    ADD COLUMN lease_owner TEXT NULL,
    ADD COLUMN lease_pod_name TEXT NULL,
    ADD COLUMN lease_pod_uid TEXT NULL,
    ADD COLUMN attempt INT NOT NULL DEFAULT 0,
    ADD COLUMN lease_expires_at TIMESTAMPTZ NULL,
    ADD COLUMN heartbeat_at TIMESTAMPTZ NULL,
    ADD COLUMN started_at TIMESTAMPTZ NULL,
    ADD COLUMN finished_at TIMESTAMPTZ NULL,
    ADD COLUMN terminal_result JSONB NULL,
    ADD CONSTRAINT executor_deployments_status_check CHECK (
        status IN (
            'pending', 'blocked', 'dispatching', 'deployed', 'leased', 'running',
            'retry_pending', 'succeeded', 'failed', 'skipped', 'cancelled'
        )
    ),
    ADD CONSTRAINT executor_deployments_worker_pool_check CHECK (
        execution_mode <> 'workers' OR pool_key IS NOT NULL
    ),
    ADD CONSTRAINT executor_deployments_slot_order_check CHECK (
        slot_released_at IS NULL OR slot_reserved_at IS NOT NULL
    );

-- Drives the per-pool claim query:
-- WHERE execution_mode='workers' AND status='pending' AND pool_key=$1
--   AND next_attempt_at <= NOW() ORDER BY created_at.
CREATE INDEX idx_executor_worker_due
    ON executor_deployments (pool_key, next_attempt_at, created_at)
    WHERE execution_mode = 'workers' AND status = 'pending';

-- One deployment at a time may hold a given lease ID.
CREATE UNIQUE INDEX uq_executor_active_lease_id
    ON executor_deployments (lease_id)
    WHERE lease_id IS NOT NULL;

-- executor_worker_pools registers the worker pools the reconciler sizes. A pool
-- serves exactly one (service, image, runtime manifest, credential) combination,
-- so every worker in it can execute any task routed to its pool_key.
CREATE TABLE executor_worker_pools (
    pool_key TEXT PRIMARY KEY,
    service_name TEXT NOT NULL,
    image_tag TEXT NOT NULL,
    runtime_manifest_uri TEXT NOT NULL,
    runtime_manifest_sha256 TEXT NOT NULL,
    runtime_manifest_dbt_version TEXT NOT NULL,
    runtime_manifest_parse_context_sha256 TEXT NOT NULL,
    credential_sha256 TEXT NOT NULL,
    desired_replicas INT NOT NULL DEFAULT 0 CHECK (desired_replicas >= 0),
    last_activity_at TIMESTAMPTZ NOT NULL,
    initialization_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE executor_worker_pools IS
    'Worker pools reconciled into Kubernetes Deployments (executor-controller).';
COMMENT ON COLUMN executor_deployments.lease_token_sha256 IS
    'SHA-256 of the worker lease token; the raw token is never stored.';
