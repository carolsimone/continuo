-- db/migration/k8s/V5__add_log_fields_to_k8s_status_outbox.sql
ALTER TABLE k8s_status_outbox
    ADD COLUMN execution_id UUID,
    ADD COLUMN log_s3_key   TEXT;
