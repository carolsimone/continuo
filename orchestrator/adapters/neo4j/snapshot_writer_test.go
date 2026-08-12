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
			TestCount:       3,
			TestCountKnown:  true,
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
	params := snapshot.Params{RunID: runID, ScheduleName: scheduleName, Kind: "rebase", SourceRunID: &srcRun, InitiatedBy: "okta|alice"}

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
			RETURN run.kind AS kind, run.source_run_id AS source_run_id, run.schedule_name AS schedule_name, run.initiated_by AS initiated_by`,
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
	require.Equal(t, "okta|alice", runRec["initiated_by"])

	// Assert :EXECUTES edge properties for each task.
	rec2, err := read.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		r, err := tx.Run(context.Background(), `
			MATCH (run:Run {run_id: $run_id})-[e:EXECUTES]->(t:Table)
			WHERE t.schedule_name = $sched
			RETURN t.table_name AS tbl, e.status AS status, e.image_tag AS img,
			       e.manifest_version AS mv, e.task_id AS tid, e.inherited_from_task_id AS inh,
			       e.test_count AS test_count
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

	// Row "a" — rebased PENDING, known test_count stamped on the edge.
	require.Equal(t, "a", rows[0]["tbl"])
	require.Equal(t, "PENDING", rows[0]["status"])
	require.Equal(t, "img:1", rows[0]["img"])
	require.Equal(t, "v1", rows[0]["mv"])
	require.Equal(t, taskA.String(), rows[0]["tid"])
	require.Nil(t, rows[0]["inh"])
	require.Equal(t, int64(3), rows[0]["test_count"], "TestCountKnown=true must stamp e.test_count")

	// Row "b" — inherited SUCCEEDED with root pointer; TestCountKnown=false must
	// leave e.test_count unset (nil), not stamp a zero.
	require.Equal(t, "b", rows[1]["tbl"])
	require.Equal(t, "SUCCEEDED", rows[1]["status"])
	require.Equal(t, "img:0", rows[1]["img"])
	require.Equal(t, "v0", rows[1]["mv"])
	require.Equal(t, taskB.String(), rows[1]["tid"])
	require.Equal(t, rootB.String(), rows[1]["inh"])
	require.Nil(t, rows[1]["test_count"], "TestCountKnown=false must not stamp e.test_count")
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

// TestSnapshotWriter_CancelledStampsTerminalOnCreate covers the cancel-before-snapshot
// race: when the schedule was already cancelled at snapshot time, the :Run must be
// created already-terminal (completed_at + terminal_status='cancelled') so it never
// enters the active set, even if the run.finalized:v1 projection raced ahead and missed.
func TestSnapshotWriter_CancelledStampsTerminalOnCreate(t *testing.T) {
	driver := newDriver(t)
	scheduleName := "test-mat-" + uuid.New().String()[:8]

	seedTable(t, driver, scheduleName, "svc", "s", "a", "img:1", "v1")

	runID := uuid.New().String()
	t.Cleanup(func() { cleanupRunAndTables(t, driver, runID, "test-mat-") })

	projection := []snapshot.TaskProjection{
		{
			TaskID:          uuid.New(),
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
	}
	params := snapshot.Params{RunID: runID, ScheduleName: scheduleName, Kind: "trigger", Cancelled: true}

	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		return nil, neo4jinfra.NewSnapshotWriterForTest(tx).WriteRunAndExecutesEdges(context.Background(), params, projection)
	})
	require.NoError(t, err)

	read := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer read.Close(context.Background())
	rec, err := read.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		r, err := tx.Run(context.Background(), `
			MATCH (run:Run {run_id: $run_id})
			RETURN run.terminal_status AS terminal_status, run.completed_at AS completed_at`,
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
	require.NotNil(t, rec, "expected the :Run to be created")
	runRec := rec.(map[string]interface{})
	require.Equal(t, "cancelled", runRec["terminal_status"], "cancelled snapshot must stamp terminal_status")
	require.NotNil(t, runRec["completed_at"], "cancelled snapshot must stamp completed_at so the run leaves the active set")
}

// TestSnapshotWriter_BackfillsInitiatedByOnMatch verifies that re-snapshotting a
// :Run that predates provenance tracking (a node created without initiated_by)
// backfills the property from the event, while an already-recorded initiator is
// preserved. This mirrors how `kind` is backfilled via COALESCE on ON MATCH.
func TestSnapshotWriter_BackfillsInitiatedByOnMatch(t *testing.T) {
	driver := newDriver(t)
	scheduleName := "test-mat-" + uuid.New().String()[:8]
	seedTable(t, driver, scheduleName, "svc", "s", "a", "img:1", "v1")

	runID := uuid.New().String()
	t.Cleanup(func() { cleanupRunAndTables(t, driver, runID, "test-mat-") })

	// Simulate a pre-provenance :Run: created by an older orchestrator with no
	// initiated_by property at all.
	write := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer write.Close(context.Background())
	_, err := write.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, err := tx.Run(context.Background(),
			`CREATE (r:Run {run_id: $run_id, schedule_name: $sched})`,
			map[string]interface{}{"run_id": runID, "sched": scheduleName})
		return nil, err
	})
	require.NoError(t, err)

	projection := []snapshot.TaskProjection{{
		TaskID: uuid.New(), ServiceName: "svc", SchemaName: "s", TableName: "a",
		ScheduleName: scheduleName, NodeType: "dbt-model", InitialStatus: "PENDING",
		ImageTag: "img:1", ManifestVersion: "v1", MaxRetries: 2,
	}}
	params := snapshot.Params{RunID: runID, ScheduleName: scheduleName, Kind: "rerun", InitiatedBy: "okta|carol"}

	_, err = write.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		return nil, neo4jinfra.NewSnapshotWriterForTest(tx).WriteRunAndExecutesEdges(context.Background(), params, projection)
	})
	require.NoError(t, err)

	got := readRunInitiatedBy(t, driver, runID)
	require.Equal(t, "okta|carol", got, "ON MATCH must backfill initiated_by on a pre-provenance node")

	// A second re-snapshot with a different user must NOT overwrite the now-set value.
	params2 := params
	params2.InitiatedBy = "okta|dave"
	_, err = write.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		return nil, neo4jinfra.NewSnapshotWriterForTest(tx).WriteRunAndExecutesEdges(context.Background(), params2, projection)
	})
	require.NoError(t, err)
	require.Equal(t, "okta|carol", readRunInitiatedBy(t, driver, runID), "initiated_by must be immutable once set")
}

// readRunInitiatedBy returns the initiated_by property of the :Run with runID.
func readRunInitiatedBy(t *testing.T, driver neo4j.DriverWithContext, runID string) string {
	t.Helper()
	read := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer read.Close(context.Background())
	v, err := read.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		r, err := tx.Run(context.Background(),
			`MATCH (run:Run {run_id: $run_id}) RETURN run.initiated_by AS initiated_by`,
			map[string]interface{}{"run_id": runID})
		if err != nil {
			return nil, err
		}
		if !r.Next(context.Background()) {
			return "", nil
		}
		val, _ := r.Record().Get("initiated_by")
		s, _ := val.(string)
		return s, nil
	})
	require.NoError(t, err)
	return v.(string)
}

// TestSnapshotWriter_StampsContentHashOnExecutes pins the property that ties a
// run to the exact code it executed: the edge copies the content_hash off the
// :Table it matched, so a later release changing the node cannot rewrite what
// this run is recorded as having run.
func TestSnapshotWriter_StampsContentHashOnExecutes(t *testing.T) {
	driver := newDriver(t)
	scheduleName := "test-mat-" + uuid.New().String()[:8]

	seedTable(t, driver, scheduleName, "svc", "s", "hashed", "img:1", "v1")
	seedTableContentHash(t, driver, scheduleName, "svc", "s", "hashed", "sha256:exec")

	runID := uuid.New().String()
	t.Cleanup(func() { cleanupRunAndTables(t, driver, runID, "test-mat-") })

	projection := []snapshot.TaskProjection{{
		TaskID:          uuid.New(),
		ServiceName:     "svc",
		SchemaName:      "s",
		TableName:       "hashed",
		ScheduleName:    scheduleName,
		NodeType:        "dbt-model",
		InitialStatus:   "PENDING",
		ImageTag:        "img:1",
		ManifestVersion: "v1",
	}}
	params := snapshot.Params{RunID: runID, ScheduleName: scheduleName, Kind: "scheduled"}

	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		return nil, neo4jinfra.NewSnapshotWriterForTest(tx).WriteRunAndExecutesEdges(context.Background(), params, projection)
	})
	require.NoError(t, err)

	read := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer read.Close(context.Background())
	got, err := read.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		r, err := tx.Run(context.Background(), `
			MATCH (:Run {run_id: $run_id})-[e:EXECUTES]->(t:Table {schedule_name: $sched})
			RETURN e.content_hash AS ch`,
			map[string]interface{}{"run_id": runID, "sched": scheduleName})
		if err != nil {
			return nil, err
		}
		if !r.Next(context.Background()) {
			return nil, r.Err()
		}
		v, _ := r.Record().Get("ch")
		return v, r.Err()
	})
	require.NoError(t, err)
	require.Equal(t, "sha256:exec", got)
}

// seedTableContentHash stamps a content_hash on an already-seeded :Table, the
// way a topology swap does when it applies a promoted release.
func seedTableContentHash(t *testing.T, driver neo4j.DriverWithContext, scheduleName, service, schema, table, hash string) {
	t.Helper()
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, err := tx.Run(context.Background(), `
			MATCH (t:Table {service_name: $svc, schema_name: $schema, table_name: $tbl, schedule_name: $sched})
			SET t.content_hash = $hash`,
			map[string]interface{}{"svc": service, "schema": schema, "tbl": table, "sched": scheduleName, "hash": hash})
		return nil, err
	})
	require.NoError(t, err)
}
