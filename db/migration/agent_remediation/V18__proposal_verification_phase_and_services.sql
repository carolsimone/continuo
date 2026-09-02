-- verification_run_id names the pipeline run that verified this attempt's
-- fix (the first of its verifications). verifications carries, per edited
-- service, the run id and the durable summary of how that run went: its
-- phase (queued | running | passed | failed), when it left the queue, and the
-- errors it reported. services is every service the attempt touched — the
-- failing nodes' services plus the edited ones — so a listing can be
-- narrowed to one team's work.
ALTER TABLE proposal RENAME COLUMN shadow_release_id TO verification_run_id;

ALTER TABLE proposal ADD COLUMN services text[] NOT NULL DEFAULT '{}';
CREATE INDEX idx_proposal_services ON proposal USING GIN (services);

UPDATE proposal SET services = COALESCE((
    SELECT array_agg(DISTINCT s ORDER BY s)
      FROM (
        SELECT n->>'service' AS s
          FROM jsonb_array_elements(COALESCE(trigger_payload->'nodes', '[]'::jsonb)) AS n
         WHERE COALESCE(n->>'service', '') <> ''
        UNION
        SELECT v->>'service'
          FROM jsonb_array_elements(verifications) AS v
         WHERE COALESCE(v->>'service', '') <> ''
      ) AS x), '{}');

UPDATE proposal SET verifications = (
    SELECT jsonb_agg(
             (v - 'shadow_release_id') || jsonb_build_object(
               'run_id',       COALESCE(v->>'shadow_release_id', v->>'run_id', ''),
               'phase',        CASE WHEN status = 'proposed' THEN 'passed'
                                    WHEN status = 'failed' AND verify_error <> '' THEN 'failed'
                                    ELSE '' END,
               'activated_at', NULL,
               'error',        CASE WHEN status = 'failed' THEN verify_error ELSE '' END)
             ORDER BY ord)
      FROM jsonb_array_elements(verifications) WITH ORDINALITY AS x(v, ord))
 WHERE verifications <> '[]'::jsonb;
