-- One proposal per attempt of a rejected release. The attempt addresses the
-- release's whole failing set: resolved_node_ids lists the failing nodes it
-- covers, node_outcomes records how it ended for each of them, and
-- verifications names the shadow release that judged each edited service.
-- Rows written per node before this change are renumbered per release so the
-- new identity holds, and their batched fields are derived from node_id.
ALTER TABLE proposal
    ADD COLUMN resolved_node_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN node_outcomes    JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN verifications    JSONB NOT NULL DEFAULT '[]'::jsonb;

UPDATE proposal
   SET resolved_node_ids = jsonb_build_array(node_id)
 WHERE resolved_node_ids = '[]'::jsonb AND node_id <> '';

UPDATE proposal
   SET node_outcomes = jsonb_build_object(node_id, jsonb_build_object('status', status, 'reason', rationale))
 WHERE node_outcomes = '{}'::jsonb AND node_id <> '';

UPDATE proposal
   SET verifications = jsonb_build_array(jsonb_build_object('service', '', 'kind', 'python', 'shadow_release_id', shadow_release_id))
 WHERE verifications = '[]'::jsonb AND shadow_release_id <> '';

-- The old unique key (release_id, source, node_id, attempt) is dropped before
-- renumbering: Postgres checks a non-deferrable unique constraint per row, so
-- renumbering rows that share (release_id, source, node_id) under the OLD key
-- can transiently collide mid-statement.
ALTER TABLE proposal DROP CONSTRAINT proposal_uniq;

WITH ranked AS (
    SELECT id, row_number() OVER (PARTITION BY release_id ORDER BY created_at, attempt, node_id) AS rn
      FROM proposal
)
UPDATE proposal p SET attempt = ranked.rn FROM ranked WHERE p.id = ranked.id;

ALTER TABLE proposal ADD CONSTRAINT proposal_uniq UNIQUE (release_id, attempt);

CREATE INDEX idx_proposal_resolved_node_ids ON proposal USING GIN (resolved_node_ids);
CREATE INDEX idx_proposal_release_round ON proposal (release_id, remediation_round);
DROP INDEX IF EXISTS idx_proposal_round_node_signature;
