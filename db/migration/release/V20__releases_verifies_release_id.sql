-- A shadow release names the rejected release it verifies a fix for. The fix
-- may edit a different service than the one whose release was rejected, so the
-- rejected release's own changed service is assembled from ITS candidate
-- manifest rather than from the live production pointer. Empty for every other
-- release, and never updated after the row is inserted.
ALTER TABLE releases ADD COLUMN verifies_release_id TEXT NOT NULL DEFAULT '';
