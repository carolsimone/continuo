-- deployments.message_processing_id names the inbound message that created the
-- row, and the dispatcher copies it onto the outbox rows it writes. It is
-- provenance only, so deleting an aged message_processing row sets it to NULL
-- instead of being refused: the retention sweeper prunes dedup rows that
-- deployments still name.
ALTER TABLE deployments DROP CONSTRAINT deployments_message_processing_id_fkey;
ALTER TABLE deployments
    ADD CONSTRAINT deployments_message_processing_id_fkey
    FOREIGN KEY (message_processing_id) REFERENCES message_processing (id) ON DELETE SET NULL;

-- The retention sweeper deletes message_processing rows in batches, and each
-- batch looks rows up by message_processing_id in both referencing tables: the
-- pruner's NOT EXISTS probe skips a candidate an execution_outbox row still
-- names, and for every row it deletes Postgres runs the foreign-key action on
-- deployments (SET NULL) and the foreign-key check on execution_outbox. Without
-- these indexes each lookup scans the whole referencing table, and deployments
-- is never pruned. Most rows carry no message_processing_id (job-status outbox
-- rows have none), so the indexes cover only the rows that do.
CREATE INDEX idx_deployments_message_processing_id
    ON deployments (message_processing_id) WHERE message_processing_id IS NOT NULL;
CREATE INDEX idx_execution_outbox_message_processing_id
    ON execution_outbox (message_processing_id) WHERE message_processing_id IS NOT NULL;
