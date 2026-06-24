-- V17 adds the structured validation-result pointer to executor_deployments.
-- run_results_uri is the S3 key of the per-node structured result emitted by the
-- validation pod and uploaded by k8s-controller (parallel to dbt_log_uri). NULL
-- on production rows and on validation rows whose pod produced no structured
-- block (old image / truncated log) — the remediation classifier then falls back
-- to the text log.
ALTER TABLE executor_deployments
    ADD COLUMN run_results_uri TEXT NULL;
