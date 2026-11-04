-- run-controller signs in as its own login, which owns no table, and Postgres
-- skips an ANALYZE by a non-owner with only a warning. analyze_topology()
-- runs as its owner, the migration login that owns the topology tables, so
-- the service can refresh their planner statistics in the transaction that
-- loads a release. It analyses only those two tables and takes no argument.
CREATE FUNCTION analyze_topology() RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
    SET search_path = public, pg_temp
AS $$
    ANALYZE topology_nodes, topology_edges
$$;

COMMENT ON FUNCTION analyze_topology() IS 'Analyzes topology_nodes and topology_edges as the tables'' owner, for a service login that does not own them';
