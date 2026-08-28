-- The attempt cap is a budget per remediation round of a release: a "try again"
-- on a rejected release starts round 2 with a fresh count. Rows written before
-- the column existed belong to round 1.
ALTER TABLE proposal ADD COLUMN remediation_round INT NOT NULL DEFAULT 1;
CREATE INDEX idx_proposal_round_node_signature
    ON proposal (release_id, remediation_round, source, node_id, error_signature);
DROP INDEX idx_proposal_release_node_signature;
