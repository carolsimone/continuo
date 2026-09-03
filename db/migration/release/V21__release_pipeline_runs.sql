-- One table for every run of the release pipeline. A run is a candidate
-- release (posted by a team's CI, promotable) or a fix-verification run
-- (posted by agent-remediation, never promotable); run_kind tells them apart.
-- The queue, the active-run slice, and the advisory lock are unchanged: one
-- FIFO over this table by status, whatever the kind.

ALTER TABLE releases RENAME TO release_pipeline_runs;
ALTER TABLE release_pipeline_runs RENAME COLUMN release_id TO run_id;
-- `kind` is the manifest kind (dbt | python); the run kind takes its own column.
ALTER TABLE release_pipeline_runs RENAME COLUMN kind TO manifest_kind;
ALTER TABLE release_pipeline_runs RENAME COLUMN reject_reason TO fail_reason;
ALTER TABLE release_pipeline_runs RENAME COLUMN reject_detail TO fail_detail;

ALTER TABLE release_pipeline_runs ADD COLUMN run_kind text NOT NULL DEFAULT 'candidate';
UPDATE release_pipeline_runs SET run_kind = 'verification' WHERE shadow;
ALTER TABLE release_pipeline_runs ALTER COLUMN run_kind DROP DEFAULT;
ALTER TABLE release_pipeline_runs ADD CONSTRAINT release_pipeline_runs_run_kind_check
    CHECK (run_kind IN ('candidate', 'verification'));

-- attempt: which attempt of the verified release's remediation a verification
-- belongs to. Existing verification ids end in "-a<n>".
ALTER TABLE release_pipeline_runs ADD COLUMN attempt integer NOT NULL DEFAULT 0;
UPDATE release_pipeline_runs
   SET attempt = COALESCE((substring(run_id from '-a([0-9]+)$'))::integer, 1)
 WHERE run_kind = 'verification';

-- Verification terminals are passed/failed. Map the rows and their history.
ALTER TABLE release_pipeline_runs DROP CONSTRAINT releases_status_check;
UPDATE release_pipeline_runs
   SET status = CASE status WHEN 'validated' THEN 'passed' WHEN 'rejected' THEN 'failed' ELSE status END
 WHERE run_kind = 'verification';
UPDATE release_pipeline_runs
   SET transitions = COALESCE((
         SELECT jsonb_agg(
                  CASE t->>'to'
                    WHEN 'validated' THEN jsonb_set(t, '{to}', '"passed"')
                    WHEN 'rejected'  THEN jsonb_set(t, '{to}', '"failed"')
                    ELSE t
                  END ORDER BY ord)
           FROM jsonb_array_elements(transitions) WITH ORDINALITY AS x(t, ord)),
         '[]'::jsonb)
 WHERE run_kind = 'verification';
-- A verification carries no candidate facts.
UPDATE release_pipeline_runs
   SET rejection_payload = NULL, repo = '', commit_sha = '', remediation_round = 1, bootstrap = false
 WHERE run_kind = 'verification';

ALTER TABLE release_pipeline_runs ADD CONSTRAINT release_pipeline_runs_status_check CHECK (status IN
  ('received','compiling','parsing','seed_building','validating',
   'promoted','rejected','superseded','passed','failed'));
ALTER TABLE release_pipeline_runs ADD CONSTRAINT release_pipeline_runs_kind_status_check CHECK (
  (run_kind = 'candidate'    AND status NOT IN ('passed','failed')) OR
  (run_kind = 'verification' AND status NOT IN ('promoted','rejected','superseded')));

ALTER TABLE release_pipeline_runs DROP COLUMN shadow;

-- Indexes: the active-run predicate is unchanged; the list indexes lead with
-- run_kind because every listing is per kind; verifies_release_id serves the
-- release page's "verification runs" section.
ALTER INDEX idx_releases_active_status RENAME TO idx_release_pipeline_runs_active;
DROP INDEX IF EXISTS idx_releases_created_at;
DROP INDEX IF EXISTS idx_releases_status_created_at;
CREATE INDEX idx_release_pipeline_runs_kind_created
    ON release_pipeline_runs (run_kind, created_at DESC, run_id DESC);
CREATE INDEX idx_release_pipeline_runs_kind_status_created
    ON release_pipeline_runs (run_kind, status, created_at DESC, run_id DESC);
CREATE INDEX idx_release_pipeline_runs_verifies
    ON release_pipeline_runs (verifies_release_id) WHERE run_kind = 'verification';
