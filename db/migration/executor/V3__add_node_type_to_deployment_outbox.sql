-- Add node_type column to deployment_outbox.
-- DEFAULT 'dbt-model' covers in-flight rows written by the pre-migration binary;
-- those rows are all models. The old binary can coexist with this schema
-- (it does not reference node_type, so inserts hit the default with no error).
ALTER TABLE deployment_outbox
    ADD COLUMN node_type TEXT NOT NULL DEFAULT 'dbt-model';
