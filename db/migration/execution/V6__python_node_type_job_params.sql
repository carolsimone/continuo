-- The python script node kind is stored as "python-node". A deploy command
-- queued in deployments.job_params (a production DeployTask or a candidate
-- ValidationDeployTask, both JSON objects with a top-level node_type field) can
-- sit pending or blocked across an upgrade; a row written under the retired
-- "python-model" value would then be rejected when the dispatcher parses its
-- node type, stranding the deployment with no Job ever created.
--
-- Rewrite only the node_type field of each affected row's job_params. The WHERE
-- extracts the node_type field alone, so a job_params whose node_type is some
-- other kind but that merely mentions "python-model" in another string field
-- (a service_name, a schema_name) is not matched, and re-running is a no-op.
UPDATE deployments
   SET job_params = jsonb_set(job_params, '{node_type}', '"python-node"')
 WHERE job_params->>'node_type' = 'python-model';
