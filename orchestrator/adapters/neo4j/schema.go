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
//   - node_version_unique: makes (unique_id, content_hash) a code version's
//     identity, so a redelivered release MERGEs onto the version it already
//     wrote instead of minting a duplicate.
//   - node_version_uid: backs chain and history lookups that start from a
//     node's unique_id rather than from its :Table — the path that still works
//     after a retired node's :Table is deleted.
//   - code_unit_unique / code_unit_version_unique: the same identity guarantees
//     for shared-code units (dbt macros today, Python modules later), whose ids
//     are service-namespaced so two services' copies never collide.
//   - code_unit_version_unit_id: backs the `unit_id`-only lookups the unit
//     history RPCs issue (UnitVersions, and once per unit on the
//     node-selector path) — code_unit_version_unique only covers the
//     composite (unit_id, checksum), so without this index Neo4j plans a full
//     :CodeUnitVersion label scan for every one of those reads.
//   - rejection_unique: a rejection's identity is (release_id, node_id),
//     matching the classifier's dedup, so redeliveries MERGE onto the row
//     they already wrote.
//   - rejection_node_id: backs `[:RESOLVED_BY]` linking from both the
//     versions consumer and the late-arrival back-link.
//   - error_signature_unique: the global precedent hub's MERGE key.
//   - error_signature_category_reason: backs the category+reason precedent
//     lookup.
//   - proposal_unique: a proposal's identity.
var schemaStatements = []string{
	"CREATE CONSTRAINT run_id_unique IF NOT EXISTS FOR (r:Run) REQUIRE r.run_id IS UNIQUE",
	"CREATE CONSTRAINT table_uid_unique IF NOT EXISTS FOR (t:Table) REQUIRE t.unique_id IS UNIQUE",
	"CREATE INDEX table_fqn IF NOT EXISTS FOR (t:Table) ON (t.service_name, t.schema_name, t.table_name)",
	"CREATE INDEX table_schedule IF NOT EXISTS FOR (t:Table) ON (t.schedule_name)",
	"CREATE INDEX run_schedule IF NOT EXISTS FOR (r:Run) ON (r.schedule_name)",
	"CREATE CONSTRAINT node_version_unique IF NOT EXISTS FOR (v:NodeVersion) REQUIRE (v.unique_id, v.content_hash) IS UNIQUE",
	"CREATE INDEX node_version_uid IF NOT EXISTS FOR (v:NodeVersion) ON (v.unique_id)",
	"CREATE CONSTRAINT code_unit_unique IF NOT EXISTS FOR (c:CodeUnit) REQUIRE c.unit_id IS UNIQUE",
	"CREATE CONSTRAINT code_unit_version_unique IF NOT EXISTS FOR (v:CodeUnitVersion) REQUIRE (v.unit_id, v.checksum) IS UNIQUE",
	"CREATE INDEX code_unit_version_unit_id IF NOT EXISTS FOR (v:CodeUnitVersion) ON (v.unit_id)",
	"CREATE CONSTRAINT rejection_unique IF NOT EXISTS FOR (r:Rejection) REQUIRE (r.release_id, r.node_id) IS UNIQUE",
	"CREATE INDEX rejection_node_id IF NOT EXISTS FOR (r:Rejection) ON (r.node_id)",
	"CREATE CONSTRAINT error_signature_unique IF NOT EXISTS FOR (s:ErrorSignature) REQUIRE s.signature IS UNIQUE",
	"CREATE INDEX error_signature_category_reason IF NOT EXISTS FOR (s:ErrorSignature) ON (s.category, s.reason)",
	"CREATE CONSTRAINT proposal_unique IF NOT EXISTS FOR (p:Proposal) REQUIRE p.proposal_id IS UNIQUE",
}

// dataMigrations are idempotent DML statements applied once startup DDL is in
// place. Unlike schemaStatements they touch data, so they run after the indexes
// are online (the migrations themselves benefit from those indexes).
//
//   - normalize_terminal_status: folds the legacy uppercase casing the run
//     aggregate used to stamp ("SUCCEEDED"/"FAILED") down to the canonical
//     lowercase form every writer now produces, so the UI never sees a mixed
//     casing. Re-running it is a no-op once all rows are lowercase.
//   - delete_previous_chain: removes the retired :PREVIOUS version chain (node
//     and unit versions alike). Ordering is promoted_at; the chain could
//     neither order nor enumerate correctly, and nothing writes it any more.
//     Re-running finds no edges and is a no-op.
var dataMigrations = []string{
	"MATCH (r:Run) WHERE r.terminal_status IN ['SUCCEEDED', 'FAILED'] SET r.terminal_status = toLower(r.terminal_status)",
	"MATCH ()-[p:PREVIOUS]->() DELETE p",
}

// awaitIndexTimeoutSeconds bounds how long InitSchema waits for the freshly
// created indexes to come online. On a cold graph the indexes are populated
// instantly; on a populated graph this is the ceiling before startup fails
// loudly rather than serving queries against a still-building index.
const awaitIndexTimeoutSeconds = 300

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
	defer func() { _ = session.Close(ctx) }()

	// Bolt surfaces a DDL failure either from Run or only when the result
	// summary is pulled, so every statement is consumed and checked — a
	// discarded result could let a failed statement slip past this gate.
	for _, stmt := range schemaStatements {
		result, err := session.Run(ctx, stmt, nil)
		if err != nil {
			return fmt.Errorf("InitSchema: %q: %w", stmt, err)
		}
		if _, err := result.Consume(ctx); err != nil {
			return fmt.Errorf("InitSchema: %q: %w", stmt, err)
		}
	}

	// CREATE INDEX only registers the index; on a non-empty graph Neo4j
	// populates it asynchronously. Block until every index is online so the
	// first consumer query seeks an index instead of falling back to the full
	// label scan this startup gate exists to prevent.
	awaitResult, err := session.Run(ctx,
		"CALL db.awaitIndexes($timeout)",
		map[string]any{"timeout": awaitIndexTimeoutSeconds},
	)
	if err != nil {
		return fmt.Errorf("InitSchema: await indexes online: %w", err)
	}
	if _, err := awaitResult.Consume(ctx); err != nil {
		return fmt.Errorf("InitSchema: await indexes online: %w", err)
	}

	// Apply idempotent data migrations once the indexes backing them are online.
	for _, stmt := range dataMigrations {
		result, err := session.Run(ctx, stmt, nil)
		if err != nil {
			return fmt.Errorf("InitSchema: data migration %q: %w", stmt, err)
		}
		if _, err := result.Consume(ctx); err != nil {
			return fmt.Errorf("InitSchema: data migration %q: %w", stmt, err)
		}
	}

	logger.Info("Neo4j schema initialized",
		"statements", len(schemaStatements),
		"data_migrations", len(dataMigrations))
	return nil
}
