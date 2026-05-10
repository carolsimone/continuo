package snapshot_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// addDependency adds a DEPENDS_ON edge between two :Table nodes.
func addDependency(t *testing.T, driver neo4j.DriverWithContext, fromSched, fromSchema, fromTable, toSched, toSchema, toTable string) {
	t.Helper()
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, err := tx.Run(context.Background(), `
			MATCH (a:Table {schedule_name: $fs, schema_name: $fSchema, table_name: $fTable})
			MATCH (b:Table {schedule_name: $ts, schema_name: $tSchema, table_name: $tTable})
			MERGE (a)-[:DEPENDS_ON]->(b)`,
			map[string]interface{}{
				"fs": fromSched, "fSchema": fromSchema, "fTable": fromTable,
				"ts": toSched, "tSchema": toSchema, "tTable": toTable,
			})
		return nil, err
	})
	require.NoError(t, err)
}

// seedSeedTable seeds a :Table with node_type='dbt-seed' (vs default 'dbt-model').
func seedSeedTable(t *testing.T, driver neo4j.DriverWithContext, scheduleName, service, schema, table, image, manifest string) {
	t.Helper()
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, err := tx.Run(context.Background(), `
			MERGE (t:Table {service_name: $svc, schema_name: $schema, table_name: $tbl, schedule_name: $sched})
			ON CREATE SET t.active = true, t.image_tag = $img, t.manifest_version = $mv, t.node_type = 'dbt-seed'
			ON MATCH  SET t.active = true, t.image_tag = $img, t.manifest_version = $mv, t.node_type = 'dbt-seed'`,
			map[string]interface{}{
				"svc": service, "schema": schema, "tbl": table, "sched": scheduleName,
				"img": image, "mv": manifest,
			})
		return nil, err
	})
	require.NoError(t, err)
}

func TestLatestFullDAG_ReturnsActiveTablesAndUpstreamSeeds(t *testing.T) {
	driver := newDriver(t)
	sched := "test-lfd-" + uuid.New().String()[:8]
	seedSched := "seed-" + uuid.New().String()[:8] // separate seed schedule_name

	t.Cleanup(func() {
		cleanupRunAndTables(t, driver, "", "test-lfd-")
		cleanupRunAndTables(t, driver, "", "seed-")
	})

	// Active models in the target schedule.
	seedTable(t, driver, sched, "svc", "s", "a", "img:1", "v1")
	seedTable(t, driver, sched, "svc", "s", "b", "img:1", "v1")
	// Upstream seed in a different schedule_name.
	seedSeedTable(t, driver, seedSched, "svc", "s", "seed1", "img:0", "v0")
	addDependency(t, driver, sched, "s", "a", seedSched, "s", "seed1")
	addDependency(t, driver, sched, "s", "b", seedSched, "s", "seed1")

	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(context.Background())
	var got []snapshot.TaskProjection
	_, err := session.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		var ierr error
		got, ierr = snapshot.LatestFullDAG{}.SelectTasks(context.Background(), tx, snapshot.Params{ScheduleName: sched, Kind: "cron"})
		return nil, ierr
	})
	require.NoError(t, err)

	// Expect 3 entries: a, b, seed1.
	require.Len(t, got, 3)

	byTable := map[string]snapshot.TaskProjection{}
	for _, p := range got {
		byTable[p.TableName] = p
	}
	require.Contains(t, byTable, "a")
	require.Contains(t, byTable, "b")
	require.Contains(t, byTable, "seed1")

	// All entries: PENDING, no inherit pointer, fresh task_id.
	for _, p := range got {
		require.Equal(t, "PENDING", p.InitialStatus, "table %s", p.TableName)
		require.Nil(t, p.InheritedFromTaskID, "table %s", p.TableName)
		require.NotEqual(t, uuid.Nil, p.TaskID, "table %s should have fresh task_id", p.TableName)
	}

	// 'a' and 'b' carry the model schedule's metadata; 'seed1' carries the seed's.
	require.Equal(t, "img:1", byTable["a"].ImageTag)
	require.Equal(t, "v1", byTable["a"].ManifestVersion)
	require.Equal(t, "dbt-model", byTable["a"].NodeType)
	require.Equal(t, sched, byTable["a"].ScheduleName)

	require.Equal(t, "img:0", byTable["seed1"].ImageTag)
	require.Equal(t, "v0", byTable["seed1"].ManifestVersion)
	require.Equal(t, "dbt-seed", byTable["seed1"].NodeType)
	require.Equal(t, seedSched, byTable["seed1"].ScheduleName, "seed retains its own schedule_name (cross-schedule dependency)")
}

func TestLatestFullDAG_NoActiveTablesReturnsEmpty(t *testing.T) {
	driver := newDriver(t)
	sched := "test-lfd-empty-" + uuid.New().String()[:8]

	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(context.Background())
	var got []snapshot.TaskProjection
	_, err := session.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		var ierr error
		got, ierr = snapshot.LatestFullDAG{}.SelectTasks(context.Background(), tx, snapshot.Params{ScheduleName: sched, Kind: "cron"})
		return nil, ierr
	})
	require.NoError(t, err)
	require.Empty(t, got)
}
