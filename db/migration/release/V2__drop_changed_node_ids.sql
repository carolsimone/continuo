-- release-controller no longer stores a CI-supplied changed-node set.
-- HandleParsedManifest derives the validation seed set itself from the
-- content_hash diff between the candidate topology and the current prod
-- topology_snapshot, so the changed_node_ids column is now vestigial.
ALTER TABLE releases DROP COLUMN changed_node_ids;
