-- The python script node kind is stored as "python-node". Rewrite every stored
-- "python-model" value in the JSONB columns that hold node lists or per-node
-- results. The match includes the surrounding double quotes, so only an exact
-- JSON string value changes; free text that mentions the old name inside a
-- longer string (a failure detail) is left as written. Re-running is a no-op.
UPDATE current_prod
   SET topology_snapshot = replace(topology_snapshot::text, '"python-model"', '"python-node"')::jsonb
 WHERE topology_snapshot::text LIKE '%"python-model"%';

UPDATE release_pipeline_runs
   SET candidate_topology = replace(candidate_topology::text, '"python-model"', '"python-node"')::jsonb
 WHERE candidate_topology::text LIKE '%"python-model"%';

UPDATE release_pipeline_runs
   SET per_node_results = replace(per_node_results::text, '"python-model"', '"python-node"')::jsonb
 WHERE per_node_results::text LIKE '%"python-model"%';

UPDATE release_pipeline_runs
   SET rejection_payload = replace(rejection_payload::text, '"python-model"', '"python-node"')::jsonb
 WHERE rejection_payload::text LIKE '%"python-model"%';
