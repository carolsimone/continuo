ALTER TABLE proposal ADD COLUMN candidate_fix_sql_uri  TEXT    NOT NULL DEFAULT '';
ALTER TABLE proposal ADD COLUMN candidate_fix_diff_uri TEXT    NOT NULL DEFAULT '';
ALTER TABLE proposal ADD COLUMN source_resolved        BOOLEAN NOT NULL DEFAULT false;
