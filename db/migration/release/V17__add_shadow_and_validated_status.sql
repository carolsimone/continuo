-- shadow: fix-verification releases posted by remediation-agent. They run
-- the normal parse+validation flow but stop at 'validated' instead of
-- promoting, and are pruned like any resolved release.
ALTER TABLE releases ADD COLUMN shadow boolean NOT NULL DEFAULT false;
ALTER TABLE releases DROP CONSTRAINT releases_status_check;
ALTER TABLE releases ADD CONSTRAINT releases_status_check CHECK (status IN
  ('received','compiling','parsing','seed_building','validating',
   'promoted','rejected','superseded','validated'));
