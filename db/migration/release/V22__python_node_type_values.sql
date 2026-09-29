-- The python script node kind is stored as "python-node". Rewrite every stored
-- "python-model" node_type in the JSONB columns that hold node lists,
-- per-node results, and a rejection body. Only the node_type field of each
-- element is rewritten: a sibling string that merely equals "python-model"
-- (a service_name, a free-text detail) is left as written, so a service named
-- python-model keeps its name. Each statement is guarded by a containment
-- match on node_type, so re-running it is a no-op. Every array is rebuilt with
-- WITH ORDINALITY + jsonb_agg(... ORDER BY ord) so element order is preserved:
-- jsonb_agg has no inherent ordering, and a silently reordered topology or
-- results array would be a subtle regression.

-- current_prod.topology_snapshot and release_pipeline_runs.candidate_topology
-- are JSON arrays of node objects. Rebuild the array, rewriting node_type only
-- on the elements that carry the retired value.
UPDATE current_prod
   SET topology_snapshot = (
     SELECT jsonb_agg(
              CASE WHEN elem->>'node_type' = 'python-model'
                   THEN jsonb_set(elem, '{node_type}', '"python-node"')
                   ELSE elem END ORDER BY ord)
     FROM jsonb_array_elements(topology_snapshot) WITH ORDINALITY AS x(elem, ord))
 WHERE topology_snapshot @> '[{"node_type":"python-model"}]';

UPDATE release_pipeline_runs
   SET candidate_topology = (
     SELECT jsonb_agg(
              CASE WHEN elem->>'node_type' = 'python-model'
                   THEN jsonb_set(elem, '{node_type}', '"python-node"')
                   ELSE elem END ORDER BY ord)
     FROM jsonb_array_elements(candidate_topology) WITH ORDINALITY AS x(elem, ord))
 WHERE candidate_topology @> '[{"node_type":"python-model"}]';

-- release_pipeline_runs.per_node_results is a JSON array of per-node result
-- objects, each optionally carrying node_type.
UPDATE release_pipeline_runs
   SET per_node_results = (
     SELECT jsonb_agg(
              CASE WHEN elem->>'node_type' = 'python-model'
                   THEN jsonb_set(elem, '{node_type}', '"python-node"')
                   ELSE elem END ORDER BY ord)
     FROM jsonb_array_elements(per_node_results) WITH ORDINALITY AS x(elem, ord))
 WHERE per_node_results @> '[{"node_type":"python-model"}]';

-- release_pipeline_runs.rejection_payload is a single release.rejected:v1 body
-- object whose per_node array holds the failing nodes; some rejection shapes
-- carry node_type on each per_node element. Rewrite node_type only within that
-- nested array, leaving the rest of the body untouched.
UPDATE release_pipeline_runs
   SET rejection_payload = jsonb_set(
     rejection_payload, '{per_node}', (
       SELECT jsonb_agg(
                CASE WHEN elem->>'node_type' = 'python-model'
                     THEN jsonb_set(elem, '{node_type}', '"python-node"')
                     ELSE elem END ORDER BY ord)
       FROM jsonb_array_elements(rejection_payload->'per_node') WITH ORDINALITY AS x(elem, ord)))
 WHERE rejection_payload @> '{"per_node":[{"node_type":"python-model"}]}';
