-- compiling: a release running the changed service's dbt compile before parsing.
-- ACTIVE (blocks queue advancement like parsing/seed_building/validating).
ALTER TABLE releases DROP CONSTRAINT releases_status_check;
ALTER TABLE releases ADD CONSTRAINT releases_status_check
    CHECK (status IN ('received','compiling','parsing','seed_building','validating',
                      'promoted','rejected','superseded'));

-- Keep the active-release index aligned with ActiveRelease's WHERE clause.
DROP INDEX IF EXISTS idx_releases_active_status;
CREATE INDEX idx_releases_active_status ON releases (created_at)
    WHERE status IN ('received','compiling','parsing','seed_building','validating');
