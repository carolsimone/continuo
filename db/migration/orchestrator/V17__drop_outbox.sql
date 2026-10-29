-- The orchestrator writes no event: its handlers consume streams and update the
-- graph, and nothing inserts into orchestrator_outbox. Dropping the table drops
-- its indexes and its commit trigger. A row still in it, whatever its status,
-- is an event no process of this version can publish, so it goes with the table.
DROP TABLE IF EXISTS orchestrator_outbox;

-- Wakes the relay listening on the channel named after an outbox table; the
-- trigger of the table above was its only caller in this database.
DROP FUNCTION IF EXISTS continuo_outbox_notify();
