-- A rejected release can be re-classified once per remediation round (a human's
-- "try again"), so the idempotency key that makes a redelivered rejection a
-- no-op is scoped to the round. Rows written before the column existed are
-- round 1.
ALTER TABLE classification_decision ADD COLUMN remediation_round INT NOT NULL DEFAULT 1;
ALTER TABLE classification_decision DROP CONSTRAINT classification_decision_uniq;
ALTER TABLE classification_decision
    ADD CONSTRAINT classification_decision_uniq UNIQUE (source, release_id, remediation_round, node_id);
