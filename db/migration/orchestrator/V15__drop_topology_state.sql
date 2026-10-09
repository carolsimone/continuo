-- The live topology's promotion seq is recorded on the Neo4j :Meta and
-- :TopologyRoot singletons, in the transaction that swaps the topology, and is
-- allocated by release-controller with each promotion. Nothing reads or writes
-- this table.
DROP TABLE IF EXISTS topology_state;
