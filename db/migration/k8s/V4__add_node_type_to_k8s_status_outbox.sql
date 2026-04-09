-- Add node_type column to k8s_status_outbox.
-- DEFAULT 'dbt-model' covers in-flight rows written before the migration;
-- those rows are all models. Old binaries (without node_type) can coexist
-- because inserts that omit the column hit the default with no error.
ALTER TABLE k8s_status_outbox
    ADD COLUMN node_type TEXT NOT NULL DEFAULT 'dbt-model';
