package neo4jinfra_test

import (
	"context"
	"strings"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// newDriver returns a Neo4j driver for tests; skips if Neo4j is unreachable.
func newDriver(t *testing.T) neo4j.DriverWithContext {
	t.Helper()
	driver, err := neo4j.NewDriverWithContext(neo4jURI(), neo4j.BasicAuth(neo4jUser(), neo4jPassword(), ""))
	if err != nil {
		t.Skipf("neo4j driver: %v", err)
	}
	if err := driver.VerifyConnectivity(context.Background()); err != nil {
		t.Skipf("neo4j unreachable: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	return driver
}

// seedTable creates a :Table node with active=true and the given metadata.
func seedTable(t *testing.T, driver neo4j.DriverWithContext, scheduleName, service, schema, table, image, manifest string) {
	t.Helper()
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, err := tx.Run(context.Background(), `
			MERGE (t:Table {service_name: $svc, schema_name: $schema, table_name: $tbl, schedule_name: $sched})
			ON CREATE SET t.active = true, t.image_tag = $img, t.manifest_version = $mv, t.node_type = 'dbt-model'
			ON MATCH  SET t.active = true, t.image_tag = $img, t.manifest_version = $mv`,
			map[string]interface{}{
				"svc": service, "schema": schema, "tbl": table, "sched": scheduleName,
				"img": image, "mv": manifest,
			})
		return nil, err
	})
	require.NoError(t, err)
}

// cleanupRunAndTables removes the :Run and seeded :Table nodes after a test.
func cleanupRunAndTables(t *testing.T, driver neo4j.DriverWithContext, runID, scheduleNamePrefix string) {
	t.Helper()
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, _ = session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, _ = tx.Run(context.Background(), `MATCH (r:Run {run_id: $run_id}) DETACH DELETE r`, map[string]interface{}{"run_id": runID})
		_, _ = tx.Run(context.Background(), `MATCH (t:Table) WHERE t.schedule_name STARTS WITH $prefix DETACH DELETE t`, map[string]interface{}{"prefix": scheduleNamePrefix})
		return nil, nil
	})
}

func TestSnapshotWriter_CreatesRunAndEdges(t *testing.T) {
	driver := newDriver(t)
	scheduleName := "test-mat-" + uuid.New().String()[:8]

	seedTable(t, driver, scheduleName, "svc", "s", "a", "img:1", "v1")
	seedTable(t, driver, scheduleName, "svc", "s", "b", "img:0", "v0")

	runID := uuid.New().String()
	taskA := uuid.New()
	taskB := uuid.New()
	rootB := uuid.New()
	srcRun := uuid.New()

	t.Cleanup(func() { cleanupRunAndTables(t, driver, runID, "test-mat-") })

	projection := []snapshot.TaskProjection{
		{
			TaskID:          taskA,
			ServiceName:     "svc",
			SchemaName:      "s",
			TableName:       "a",
			ScheduleName:    scheduleName,
			NodeType:        "dbt-model",
			InitialStatus:   "PENDING",
			ImageTag:        "img:1",
			ManifestVersion: "v1",
			MaxRetries:      2,
		},
		{
			TaskID:              taskB,
			ServiceName:         "svc",
			SchemaName:          "s",
			TableName:           "b",
			ScheduleName:        scheduleName,
			NodeType:            "dbt-model",
			InitialStatus:       "SUCCEEDED",
			ImageTag:            "img:0",
			ManifestVersion:     "v0",
			InheritedFromTaskID: &rootB,
		},
	}
	params := snapshot.Params{RunID: runID, ScheduleName: scheduleName, Kind: "rebase", SourceRunID: &srcRun}

	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		return nil, neo4jinfra.NewSnapshotWriterForTest(tx).WriteRunAndExecutesEdges(context.Background(), params, projection)
	})
	require.NoError(t, err)

	// Assert :Run properties.
	read := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer read.Close(context.Background())
	rec, err := read.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		r, err := tx.Run(context.Background(), `
			MATCH (run:Run {run_id: $run_id})
			RETURN run.kind AS kind, run.source_run_id AS source_run_id, run.schedule_name AS schedule_name`,
			map[string]interface{}{"run_id": runID})
		if err != nil {
			return nil, err
		}
		if !r.Next(context.Background()) {
			return nil, nil
		}
		m := map[string]interface{}{}
		for _, k := range r.Record().Keys {
			v, _ := r.Record().Get(k)
			m[k] = v
		}
		return m, nil
	})
	require.NoError(t, err)
	runRec := rec.(map[string]interface{})
	require.Equal(t, "rebase", runRec["kind"])
	require.Equal(t, srcRun.String(), runRec["source_run_id"])
	require.Equal(t, scheduleName, runRec["schedule_name"])

	// Assert :EXECUTES edge properties for each task.
	rec2, err := read.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		r, err := tx.Run(context.Background(), `
			MATCH (run:Run {run_id: $run_id})-[e:EXECUTES]->(t:Table)
			WHERE t.schedule_name = $sched
			RETURN t.table_name AS tbl, e.status AS status, e.image_tag AS img,
			       e.manifest_version AS mv, e.task_id AS tid, e.inherited_from_task_id AS inh
			ORDER BY tbl`,
			map[string]interface{}{"run_id": runID, "sched": scheduleName})
		if err != nil {
			return nil, err
		}
		var rows []map[string]interface{}
		for r.Next(context.Background()) {
			row := map[string]interface{}{}
			for _, k := range r.Record().Keys {
				v, _ := r.Record().Get(k)
				row[k] = v
			}
			rows = append(rows, row)
		}
		return rows, r.Err()
	})
	require.NoError(t, err)
	rows := rec2.([]map[string]interface{})
	require.Len(t, rows, 2)

	// Row "a" — rebased PENDING.
	require.Equal(t, "a", rows[0]["tbl"])
	require.Equal(t, "PENDING", rows[0]["status"])
	require.Equal(t, "img:1", rows[0]["img"])
	require.Equal(t, "v1", rows[0]["mv"])
	require.Equal(t, taskA.String(), rows[0]["tid"])
	require.Nil(t, rows[0]["inh"])

	// Row "b" — inherited SUCCEEDED with root pointer.
	require.Equal(t, "b", rows[1]["tbl"])
	require.Equal(t, "SUCCEEDED", rows[1]["status"])
	require.Equal(t, "img:0", rows[1]["img"])
	require.Equal(t, "v0", rows[1]["mv"])
	require.Equal(t, taskB.String(), rows[1]["tid"])
	require.Equal(t, rootB.String(), rows[1]["inh"])
}

func TestSnapshotWriter_EmptyProjectionReturnsErr(t *testing.T) {
	driver := newDriver(t)
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())

	_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		return nil, neo4jinfra.NewSnapshotWriterForTest(tx).WriteRunAndExecutesEdges(context.Background(), snapshot.Params{
			RunID: uuid.New().String(), ScheduleName: "x", Kind: "rebase",
		}, nil)
	})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "empty projection"), "expected empty projection error, got %v", err)
}
