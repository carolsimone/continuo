package e2e

import (
	"context"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// dagNode represents a node in the test DAG (used only by failure test)
type dagNode struct {
	Name         string
	Dependencies []string
	ServiceName  string
	NodeType     string // empty string defaults to "dbt-model" in CreateNodeRequest
}

// getDiamondDAG returns the 13-node diamond DAG structure (3 seeds + 10 models) with per-node service names
func getDiamondDAG() []dagNode {
	return []dagNode{
		{Name: "seed_table_1", Dependencies: nil, ServiceName: "service-1", NodeType: "dbt-seed"},
		{Name: "seed_table_2", Dependencies: nil, ServiceName: "service-1", NodeType: "dbt-seed"},
		{Name: "seed_table_3", Dependencies: nil, ServiceName: "service-1", NodeType: "dbt-seed"},
		{Name: "table_a", Dependencies: []string{"seed_table_1"}, ServiceName: "service-1"},
		{Name: "table_b", Dependencies: []string{"seed_table_2"}, ServiceName: "service-1"},
		{Name: "table_c", Dependencies: []string{"seed_table_3"}, ServiceName: "service-1"},
		{Name: "table_d", Dependencies: []string{"table_a", "table_b"}, ServiceName: "service-3"},
		{Name: "table_e", Dependencies: []string{"table_b", "table_c"}, ServiceName: "service-3"},
		{Name: "table_f", Dependencies: []string{"table_a", "table_c"}, ServiceName: "service-3"},
		{Name: "table_g", Dependencies: []string{"table_d", "table_e"}, ServiceName: "service-2"},
		{Name: "table_h", Dependencies: []string{"table_e", "table_f"}, ServiceName: "service-2"},
		{Name: "table_i", Dependencies: []string{"table_g", "table_h"}, ServiceName: "service-3"},
		{Name: "table_j", Dependencies: []string{"table_g", "table_h"}, ServiceName: "service-3"},
	}
}

// getDAGLevels returns nodes grouped by execution level (used by verify.go)
func getDAGLevels() [][]string {
	return [][]string{
		{"seed_table_1", "seed_table_2", "seed_table_3"},
		{"table_a", "table_b", "table_c"},
		{"table_d", "table_e", "table_f"},
		{"table_g", "table_h"},
		{"table_i", "table_j"},
	}
}

// getFailureDAG returns the same 10-node diamond DAG with table_e using service-3-broken
func getFailureDAG() []dagNode {
	nodes := getDiamondDAG()
	for i, n := range nodes {
		if n.Name == "table_e" {
			nodes[i].ServiceName = "service-3-broken"
		}
	}
	return nodes
}

// seedNodes creates the given DAG nodes directly in Neo4j.
// This bypasses the orchestrator event path (manifest.loaded:v1) and writes
// Table nodes + DEPENDS_ON edges directly for test setup.
func seedNodes(t *testing.T, ctx context.Context, clients *testClients, nodes []dagNode, scheduleName, schemaName string) {
	session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode: neo4jdriver.AccessModeWrite,
	})
	defer session.Close(ctx)

	for _, node := range nodes {
		nodeType := node.NodeType
		if nodeType == "" {
			nodeType = "dbt-model"
		}
		nodeScheduleName := scheduleName
		if nodeType == "dbt-seed" {
			nodeScheduleName = "seed"
		}

		_, err := session.Run(ctx, `
			MERGE (t:Table {table_name: $table_name, schema: $schema})
			SET t.service_name = $service_name,
			    t.owner = $owner,
			    t.schedule_name = $schedule_name,
			    t.criticality = $criticality,
			    t.node_type = $node_type,
			    t.created_at = datetime()
		`, map[string]interface{}{
			"table_name":    node.Name,
			"schema":        schemaName,
			"service_name":  node.ServiceName,
			"owner":         failureTestOwner,
			"schedule_name": nodeScheduleName,
			"criticality":   "CORE",
			"node_type":     nodeType,
		})
		require.NoError(t, err, "Failed to create node: %s", node.Name)

		// Create DEPENDS_ON edges
		for _, dep := range node.Dependencies {
			_, err := session.Run(ctx, `
				MERGE (upstream:Table {table_name: $upstream_name, schema: $schema})
				WITH upstream
				MATCH (downstream:Table {table_name: $downstream_name, schema: $schema})
				MERGE (downstream)-[:DEPENDS_ON]->(upstream)
			`, map[string]interface{}{
				"upstream_name":   dep,
				"downstream_name": node.Name,
				"schema":          schemaName,
			})
			require.NoError(t, err, "Failed to create DEPENDS_ON edge: %s -> %s", node.Name, dep)
		}
	}
	t.Logf("Seeded %d nodes in Neo4j for schedule %s", len(nodes), scheduleName)
}

// seedFailureDAG seeds the failure DAG (table_e uses service-3-broken) and
// inserts the schedule into schedule_catalog so ActivateSchedule can proceed.
// The failure tests bypass manifest-controller (direct Neo4j seeding), so the
// catalog must be populated manually.
func seedFailureDAG(t *testing.T, ctx context.Context, clients *testClients) {
	seedNodes(t, ctx, clients, getFailureDAG(), failureTestScheduleName, failureTestSchemaName)
	insertScheduleCatalog(t, ctx, clients, failureTestScheduleName)
}

// insertScheduleCatalog inserts or re-activates a schedule name in schedule_catalog
// on the state service database. Used by tests that bypass manifest-controller.
func insertScheduleCatalog(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) {
	t.Helper()
	_, err := clients.stateDB.ExecContext(ctx, `
		INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at, removed_at, manifest_versions)
		VALUES ($1, NOW(), NOW(), NULL, '{}')
		ON CONFLICT (schedule_name) DO UPDATE
		  SET last_seen_at = NOW(),
		      removed_at = NULL,
		      manifest_versions = '{}'
	`, scheduleName)
	require.NoError(t, err, "Failed to insert %q into schedule_catalog", scheduleName)
	t.Logf("Inserted %q into schedule_catalog", scheduleName)
}

const (
	failureTestScheduleName = "e2e-schedule-failure"
	failureTestSchemaName   = "e2e_schema"
	failureTestOwner        = "test"
)

// failureTableServiceMap maps each table to its owning service for the failure DAG
var failureTableServiceMap = func() map[string]string {
	m := make(map[string]string)
	for _, n := range getFailureDAG() {
		m[n.Name] = n.ServiceName
	}
	return m
}()

func getFailureServiceNameForTable(tableName string) string {
	svc, ok := failureTableServiceMap[tableName]
	if !ok {
		panic(fmt.Sprintf("no service mapping for table %q", tableName))
	}
	return svc
}
