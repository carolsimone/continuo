-- A release can now fail at compile, seed_build, or validation independently;
-- keep their proposals from colliding by widening the natural key with source.
ALTER TABLE proposal DROP CONSTRAINT proposal_uniq;
ALTER TABLE proposal ADD CONSTRAINT proposal_uniq UNIQUE (release_id, source, node_id, attempt);
