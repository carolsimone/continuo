-- V16 widens the validation lifecycle. The continuo-orchestrated candidate build
-- gates each node on its in-set intra-service upstreams: a node starts 'blocked'
-- until those upstreams succeed (then 'pending'), and is driven to 'skipped'
-- when an in-set upstream fails so it can never be validated. 'skipped' is a
-- terminal per-node outcome that fails the release aggregate.

ALTER TABLE executor_deployments
    DROP CONSTRAINT executor_deployments_status_check,
    ADD  CONSTRAINT executor_deployments_status_check
        CHECK (status IN ('pending','blocked','deployed','failed','skipped'));

ALTER TABLE executor_deployments
    DROP CONSTRAINT IF EXISTS executor_deployments_outcome_check,
    ADD  CONSTRAINT executor_deployments_outcome_check
        CHECK (outcome IN ('ok','failed','skipped') OR outcome IS NULL);
