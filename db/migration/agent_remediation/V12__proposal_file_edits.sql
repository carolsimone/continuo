-- Each proposal carries a list of file changes as
-- [{"path","content_uri","diff_uri"}, ...]. A row written before this column
-- existed has an empty array; readers fall back to the single-file scalar
-- columns (file_path, proposed_sql_uri, diff_uri) to synthesize one edit.
ALTER TABLE proposal ADD COLUMN file_edits JSONB NOT NULL DEFAULT '[]'::jsonb;
