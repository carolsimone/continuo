-- Add the seed_building status: a release building new/changed seeds into its
-- candidate schema before validation. It is an ACTIVE status (a release in
-- seed_building blocks queue advancement just like parsing/validating).
ALTER TABLE releases DROP CONSTRAINT releases_status_check;
ALTER TABLE releases ADD CONSTRAINT releases_status_check
    CHECK (status IN ('received','parsing','seed_building','validating',
                      'promoted','rejected','superseded'));

-- Keep the active-release index aligned with ActiveRelease's WHERE clause.
DROP INDEX IF EXISTS idx_releases_active_status;
CREATE INDEX idx_releases_active_status ON releases (created_at)
    WHERE status IN ('received','parsing','seed_building','validating');
