package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// schemaStatements are the idempotent Neo4j DDL statements that back the
// orchestrator's hot lookup paths. Each uses IF NOT EXISTS so running them on
// every startup is safe and order-independent.
//
//   - run_id_unique: backs every `MATCH (:Run {run_id: …})` lookup (rehydrate,
//     snapshot writer, all query-repository reads) and guarantees a single :Run
//     node per run_id.
//   - table_uid_unique: backs the `MERGE (:Table {unique_id: …})` upsert in the
//     release-promotion swap and prevents concurrent promotions from minting
//     duplicate :Table nodes for the same unique_id.
//   - table_fqn: backs the composite (service_name, schema_name, table_name)
//     lookups used by the snapshot writer's :EXECUTES match and every
//     fully-qualified descendant/single-table reader.
//   - table_schedule: backs the `MATCH (:Table {schedule_name: …})` scans in
//     LoadLatestSourceDAG and the schedule-graph query.
//   - run_schedule: backs the `MATCH (:Run {schedule_name: …})` lookup in
//     ListRuns.
var schemaStatements = []string{
	"CREATE CONSTRAINT run_id_unique IF NOT EXISTS FOR (r:Run) REQUIRE r.run_id IS UNIQUE",
	"CREATE CONSTRAINT table_uid_unique IF NOT EXISTS FOR (t:Table) REQUIRE t.unique_id IS UNIQUE",
	"CREATE INDEX table_fqn IF NOT EXISTS FOR (t:Table) ON (t.service_name, t.schema_name, t.table_name)",
	"CREATE INDEX table_schedule IF NOT EXISTS FOR (t:Table) ON (t.schedule_name)",
	"CREATE INDEX run_schedule IF NOT EXISTS FOR (r:Run) ON (r.schedule_name)",
}

// InitSchema applies the orchestrator's Neo4j constraints and indexes. It is
// idempotent (every statement uses IF NOT EXISTS) and must run at startup,
// before any consumer or gRPC server begins serving, so the first message does
// not race a full label scan. Any DDL failure is returned so main can fail
// startup loudly rather than serve traffic against an unindexed graph.
//
// The table_uid_unique constraint refuses to create if duplicate
// :Table {unique_id} nodes already exist. See the orchestrator storage-ownership
// architecture doc for the one-off dedup query to run before first rollout.
func InitSchema(ctx context.Context, client Neo4jClient, logger *slog.Logger) error {
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	for _, stmt := range schemaStatements {
		if _, err := session.Run(ctx, stmt, nil); err != nil {
			return fmt.Errorf("InitSchema: %q: %w", stmt, err)
		}
	}

	logger.Info("Neo4j schema initialized", "statements", len(schemaStatements))
	return nil
}
