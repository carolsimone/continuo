-- PR-creation slice: persist the source-location the agent already computes
-- (repo/commit_sha/file_path) so the UI can open a PR, and track the opened PR.
ALTER TABLE proposal ADD COLUMN repo          TEXT        NOT NULL DEFAULT '';
ALTER TABLE proposal ADD COLUMN commit_sha    TEXT        NOT NULL DEFAULT '';
ALTER TABLE proposal ADD COLUMN file_path     TEXT        NOT NULL DEFAULT '';
ALTER TABLE proposal ADD COLUMN pr_url        TEXT        NOT NULL DEFAULT '';
ALTER TABLE proposal ADD COLUMN pr_number     INT         NOT NULL DEFAULT 0;
-- pr_state: '' (none) -> 'opening' -> 'open'; or 'failed' (retryable)
ALTER TABLE proposal ADD COLUMN pr_state      TEXT        NOT NULL DEFAULT '';
ALTER TABLE proposal ADD COLUMN pr_opened_at  TIMESTAMPTZ NULL;
ALTER TABLE proposal ADD COLUMN pr_opened_by  TEXT        NOT NULL DEFAULT '';
