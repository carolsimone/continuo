-- The attempt cap is a per-release budget: CountAttempts looks up
-- (release_id, source, node_id, error_signature), so the same node failing
-- with the same signature under a later release starts its own count instead
-- of inheriting an earlier release's exhausted one. Index that lookup key and
-- drop the release-less index it replaces, which nothing else queries.
CREATE INDEX idx_proposal_release_node_signature
    ON proposal (release_id, source, node_id, error_signature);
DROP INDEX idx_proposal_node_signature;
