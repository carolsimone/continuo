package e2e

import (
	"context"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// topoNode is one node of the fixed e2e DAG seeded directly into Neo4j.
type topoNode struct {
	table    string   // table_name; unique_id is "e2e_schema."+table
	service  string   // service-1|2|3
	schedule string   // schedule_name (tag)
	upstream []string // upstream table names (same schema)
}

// e2eDAG is the full topology the legacy graph-load path used to ingest. It is
// the single source of truth for e2e topology now that the ingest path is gone.
var e2eDAG = []topoNode{
	{"seed_table_1", "service-1", "seed", nil},
	{"seed_table_2", "service-1", "seed", nil},
	{"seed_table_3", "service-1", "seed", nil},
	{"table_a", "service-1", "e2e-schedule", []string{"seed_table_1"}},
	{"table_b", "service-1", "e2e-schedule", []string{"seed_table_2"}},
	{"table_c", "service-1", "e2e-schedule", []string{"seed_table_3"}},
	{"table_d", "service-3", "e2e-schedule", []string{"table_a", "table_b"}},
	{"table_e", "service-3", "e2e-schedule", []string{"table_b", "table_c"}},
	{"table_f", "service-3", "e2e-schedule", []string{"table_a", "table_c"}},
	{"table_g", "service-2", "e2e-schedule", []string{"table_d", "table_e"}},
	{"table_h", "service-2", "e2e-schedule", []string{"table_e", "table_f"}},
	{"table_i", "service-3", "e2e-schedule", []string{"table_g", "table_h"}},
	{"table_j", "service-3", "e2e-schedule", []string{"table_g", "table_h"}},
	{"ftable_a", "service-1", "e2e-schedule-failure", nil},
	{"ftable_b", "service-1", "e2e-schedule-failure", nil},
	{"ftable_c", "service-3", "e2e-schedule-failure", []string{"ftable_a", "ftable_b"}},
	{"ftable_d", "service-2", "e2e-schedule-failure", []string{"ftable_c"}},
	{"ftable_e", "service-2", "e2e-schedule-failure", []string{"ftable_c"}},
	{"ftable_f", "service-3", "e2e-schedule-failure", []string{"ftable_d", "ftable_e"}},
	{"ftable_g", "service-3", "e2e-schedule-failure", []string{"ftable_a"}},
	{"ftable_h", "service-2", "e2e-schedule-failure", []string{"ftable_g"}},
	{"rel_probe", "service-1", "rel-probe", nil},
}

const seedSchemaName = "e2e_schema"

// seedTopology writes the full e2e DAG into Neo4j (:Table + :DEPENDS_ON) and
// the schedule names into schedule_catalog, replacing the legacy
// update.graph:v1 → manifest.loaded:v1 → IngestTopology path. Per-service
// image_tag is read from the service_metadata.json sidecars in S3 so the
// seeded nodes carry the content-addressed tag that kind actually has.
func seedTopology(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()

	imageTags := map[string]string{}
	for _, svc := range []string{"service-1", "service-2", "service-3"} {
		imageTags[svc] = readServiceImageTag(t, ctx, clients, svc)
	}

	// Build the node payload for a single UNWIND MERGE.
	nodes := make([]map[string]any, 0, len(e2eDAG))
	for _, n := range e2eDAG {
		ups := make([]string, 0, len(n.upstream))
		for _, u := range n.upstream {
			ups = append(ups, seedSchemaName+"."+u)
		}
		nodes = append(nodes, map[string]any{
			"unique_id":     seedSchemaName + "." + n.table,
			"schema_name":   seedSchemaName,
			"table_name":    n.table,
			"service_name":  n.service,
			"image_tag":     imageTags[n.service],
			"schedule_name": n.schedule,
			"upstreams":     ups,
		})
	}

	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{AccessMode: neo4jdriver.AccessModeWrite})
	defer session.Close(ctx)

	// MERGE nodes (mirrors release_promotion_repository PromoteRelease shape).
	_, err := session.Run(ctx, `
		UNWIND $nodes AS n
		MERGE (t:Table {unique_id: n.unique_id})
		SET t.schema_name = n.schema_name,
		    t.table_name = n.table_name,
		    t.service_name = n.service_name,
		    t.image_tag = n.image_tag,
		    t.schedule_name = n.schedule_name,
		    t.active = true,
		    t.retired_at = null
	`, map[string]any{"nodes": nodes})
	require.NoError(t, err, "seedTopology: MERGE :Table nodes")

	// Rebuild :DEPENDS_ON edges.
	_, err = session.Run(ctx, `
		UNWIND $nodes AS n
		UNWIND n.upstreams AS up
		MATCH (a:Table {unique_id: n.unique_id}), (b:Table {unique_id: up})
		MERGE (a)-[:DEPENDS_ON]->(b)
	`, map[string]any{"nodes": nodes})
	require.NoError(t, err, "seedTopology: MERGE :DEPENDS_ON edges")

	// Insert schedule_catalog rows for every distinct schedule (idempotent).
	seen := map[string]bool{}
	now := time.Now().UTC()
	for _, n := range e2eDAG {
		if seen[n.schedule] {
			continue
		}
		seen[n.schedule] = true
		_, err := clients.stateDB.ExecContext(ctx,
			`INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at, service_metadata)
			 VALUES ($1, $2, $2, '{}'::jsonb)
			 ON CONFLICT (schedule_name) DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at, removed_at = NULL`,
			n.schedule, now)
		require.NoError(t, err, "seedTopology: upsert schedule_catalog %s", n.schedule)
	}
}
