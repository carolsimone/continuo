package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const (
	testScheduleName = "e2e-schedule"
	testSchemaName   = "e2e_schema"
	testOwner        = "test"
)

// expectedTables is the full set of table names the manifest-controller loads
var expectedTables = []string{
	"table_a", "table_b", "table_c",
	"table_d", "table_e", "table_f",
	"table_g", "table_h",
	"table_i", "table_j",
}

// tableServiceMap maps each happy-path table to its owning service
var tableServiceMap = map[string]string{
	"seed_table_1": "service-1",
	"seed_table_2": "service-1",
	"seed_table_3": "service-1",
	"table_a":      "service-1",
	"table_b":      "service-1",
	"table_c":      "service-1",
	"table_d":      "service-3",
	"table_e":      "service-3",
	"table_f":      "service-3",
	"table_g":      "service-2",
	"table_h":      "service-2",
	"table_i":      "service-3",
	"table_j":      "service-3",
}

// getServiceNameForTable returns the service name for a happy-path table
func getServiceNameForTable(tableName string) string {
	svc, ok := tableServiceMap[tableName]
	if !ok {
		panic(fmt.Sprintf("no service mapping for table %q", tableName))
	}
	return svc
}

// triggerGraphLoad publishes an update.graph:v1 event and waits until all
// expected nodes are visible in the Neo4j graph (30s timeout).
func triggerGraphLoad(t *testing.T, ctx context.Context, clients *testClients) {
	t.Helper()

	err := clients.redisClient.XAdd(ctx, &goredis.XAddArgs{
		Stream: "update.graph:v1",
		Values: map[string]interface{}{"source": "local"},
	}).Err()
	require.NoError(t, err, "Failed to publish update.graph:v1 event")

	t.Log("Published update.graph:v1 — waiting for manifest-controller to load nodes...")

	pollUntil(t, ctx, 60*time.Second, 1*time.Second, func() (bool, error) {
		session := clients.neo4jDriver.NewSession(ctx, neo4jdriver.SessionConfig{
			AccessMode: neo4jdriver.AccessModeRead,
		})
		defer session.Close(ctx)

		result, err := session.Run(ctx,
			"MATCH (t:Table) WHERE t.table_name IN $tables AND t.schedule_name = $schedule_name RETURN count(t) AS cnt",
			map[string]interface{}{
				"tables":        expectedTables,
				"schedule_name": testScheduleName,
			},
		)
		if err != nil {
			return false, nil
		}
		record, err := result.Single(ctx)
		if err != nil {
			return false, nil
		}
		cnt, _ := record.Get("cnt")
		count, ok := cnt.(int64)
		if !ok {
			return false, nil
		}
		return count >= int64(len(expectedTables)), nil
	}, fmt.Sprintf("Timeout waiting for manifest-controller to load %d nodes", len(expectedTables)))

	t.Logf("manifest-controller loaded %d nodes into graph", len(expectedTables))

	// Wait for state service to consume schedules.loaded:v1 and populate schedule_catalog.
	// Neo4j nodes appearing does not guarantee the catalog is ready — they are populated
	// by two separate async steps (graph load → Redis publish → state consumer).
	// Without this wait, ActivateSchedule bypasses the catalog check and the UI sees
	// an empty catalog until the event is eventually processed.
	pollUntil(t, ctx, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		var count int
		if err := clients.stateDB.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM schedule_catalog WHERE removed_at IS NULL`,
		).Scan(&count); err != nil {
			return false, err
		}
		return count > 0, nil
	}, "Timeout waiting for schedule_catalog to be populated by state service")

	t.Log("schedule_catalog populated — catalog and graph are in sync")
}
