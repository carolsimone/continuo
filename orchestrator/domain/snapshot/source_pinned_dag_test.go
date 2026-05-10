package snapshot_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seededEdge describes one :EXECUTES edge to seed onto a source :Run for tests.
type seededEdge struct {
	Service, Schema, Table    string
	Status                    string // "PENDING" | "SUCCEEDED" | "FAILED" | "SKIPPED" | "CANCELLED"
	TaskID                    string
	ImageTag, ManifestVersion string
	InheritedFromTaskID       string // empty for non-inherited
	ScheduleName              string // schedule of the :Table being matched (defaults to outer scheduleName)
}

// seedSourceRunWithEdges creates a :Run + per-task :EXECUTES edges with the
// given (status, task_id, image_tag, manifest_version, optional inherited_from).
// This is needed because Materialise can't seed multi-status :EXECUTES sets
// directly — it always produces a homogeneous projection.
func seedSourceRunWithEdges(t *testing.T, driver neo4j.DriverWithContext, runID, scheduleName string, edges []seededEdge) {
	t.Helper()
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		// Create the source :Run node with topology metadata.
		if _, err := tx.Run(context.Background(), `
			MERGE (r:Run {run_id: $run_id})
			ON CREATE SET r.schedule_name = $sched,
			              r.created_at = datetime(),
			              r.kind = 'cron',
			              r.topology_generation = 1,
			              r.service_metadata = '{}'`,
			map[string]interface{}{"run_id": runID, "sched": scheduleName}); err != nil {
			return nil, err
		}
		for _, e := range edges {
			schedFor := e.ScheduleName
			if schedFor == "" {
				schedFor = scheduleName
			}
			params := map[string]interface{}{
				"run_id":  runID,
				"svc":     e.Service,
				"schema":  e.Schema,
				"tbl":     e.Table,
				"sched":   schedFor,
				"status":  e.Status,
				"task_id": e.TaskID,
				"img":     e.ImageTag,
				"mv":      e.ManifestVersion,
			}
			var inheritedFrom interface{}
			if e.InheritedFromTaskID != "" {
				inheritedFrom = e.InheritedFromTaskID
			}
			params["inherited_from"] = inheritedFrom

			q := `
				MATCH (r:Run {run_id: $run_id})
				MATCH (t:Table {service_name: $svc, schema_name: $schema, table_name: $tbl, schedule_name: $sched})
				MERGE (r)-[e:EXECUTES]->(t)
				ON CREATE SET e.status = $status, e.task_id = $task_id,
				              e.image_tag = $img, e.manifest_version = $mv
				FOREACH (_ IN CASE WHEN $inherited_from IS NULL THEN [] ELSE [1] END |
				    SET e.inherited_from_task_id = $inherited_from
				)`
			if _, err := tx.Run(context.Background(), q, params); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
}

// runSelector is a small helper that opens a read session, calls the selector,
// and returns the projection.
func runSelector(t *testing.T, driver neo4j.DriverWithContext, sel snapshot.TaskSetSelector, params snapshot.Params) []snapshot.TaskProjection {
	t.Helper()
	sess := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer sess.Close(context.Background())
	var out []snapshot.TaskProjection
	_, err := sess.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		var ierr error
		out, ierr = sel.SelectTasks(context.Background(), tx, params)
		return nil, ierr
	})
	require.NoError(t, err)
	return out
}

// indexProjectionByTable maps a projection by table_name for easy lookups.
func indexProjectionByTable(p []snapshot.TaskProjection) map[string]snapshot.TaskProjection {
	m := make(map[string]snapshot.TaskProjection, len(p))
	for _, e := range p {
		m[e.TableName] = e
	}
	return m
}

func TestSourcePinnedDAG_TargetOnlyNoDescendants(t *testing.T) {
	driver := newDriver(t)
	sched := "test-spd-1-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-spd-") })

	// Seed :Tables for the source's pinned topology.
	seedTable(t, driver, sched, "svc", "s", "a", "img:OLD", "vOLD")
	seedTable(t, driver, sched, "svc", "s", "b", "img:OLD", "vOLD") // target (failed)

	srcRunID := uuid.New()
	srcA := uuid.New()
	srcB := uuid.New()
	seedSourceRunWithEdges(t, driver, srcRunID.String(), sched, []seededEdge{
		{Service: "svc", Schema: "s", Table: "a", Status: "SUCCEEDED", TaskID: srcA.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
		{Service: "svc", Schema: "s", Table: "b", Status: "FAILED", TaskID: srcB.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
	})

	got := runSelector(t, driver, snapshot.SourcePinnedDAG{
		TargetService: "svc", TargetSchema: "s", TargetTable: "b",
	}, snapshot.Params{SourceRunID: &srcRunID})

	require.Len(t, got, 2)
	by := indexProjectionByTable(got)

	// All entries carry source's pinned metadata (uniform OLD).
	for _, p := range got {
		assert.Equal(t, "img:OLD", p.ImageTag, "table %s", p.TableName)
		assert.Equal(t, "vOLD", p.ManifestVersion, "table %s", p.TableName)
	}
	// Target b: rebased PENDING.
	assert.Equal(t, "PENDING", by["b"].InitialStatus)
	assert.Nil(t, by["b"].InheritedFromTaskID)
	// a: inherited SUCCEEDED with root pointer to source's task_id.
	assert.Equal(t, "SUCCEEDED", by["a"].InitialStatus)
	require.NotNil(t, by["a"].InheritedFromTaskID)
	assert.Equal(t, srcA, *by["a"].InheritedFromTaskID)
}

func TestSourcePinnedDAG_RebasesCascadeSkippedDescendants(t *testing.T) {
	driver := newDriver(t)
	sched := "test-spd-cascade-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-spd-cascade-") })

	seedTable(t, driver, sched, "svc", "s", "a", "img:OLD", "vOLD")
	seedTable(t, driver, sched, "svc", "s", "b", "img:OLD", "vOLD") // target (failed)
	seedTable(t, driver, sched, "svc", "s", "c", "img:OLD", "vOLD") // depends on b — was SKIPPED
	addDependency(t, driver, sched, "s", "c", sched, "s", "b")

	srcRunID := uuid.New()
	srcA := uuid.New()
	srcB := uuid.New()
	srcC := uuid.New()
	seedSourceRunWithEdges(t, driver, srcRunID.String(), sched, []seededEdge{
		{Service: "svc", Schema: "s", Table: "a", Status: "SUCCEEDED", TaskID: srcA.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
		{Service: "svc", Schema: "s", Table: "b", Status: "FAILED", TaskID: srcB.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
		{Service: "svc", Schema: "s", Table: "c", Status: "SKIPPED", TaskID: srcC.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
	})

	got := runSelector(t, driver, snapshot.SourcePinnedDAG{
		TargetService: "svc", TargetSchema: "s", TargetTable: "b",
	}, snapshot.Params{SourceRunID: &srcRunID})

	require.Len(t, got, 3)
	by := indexProjectionByTable(got)

	// a: inherited
	assert.Equal(t, "SUCCEEDED", by["a"].InitialStatus)
	require.NotNil(t, by["a"].InheritedFromTaskID)
	// b: target → rebased
	assert.Equal(t, "PENDING", by["b"].InitialStatus)
	assert.Nil(t, by["b"].InheritedFromTaskID)
	// c: cascade-skipped descendant → rebased
	assert.Equal(t, "PENDING", by["c"].InitialStatus)
	assert.Nil(t, by["c"].InheritedFromTaskID)
}

func TestSourcePinnedDAG_InheritsSucceededDescendants(t *testing.T) {
	// If a descendant was already SUCCEEDED in source (unusual but possible if
	// it ran from a different upstream), it should remain inherited — not
	// flipped to PENDING just because the target failed.
	driver := newDriver(t)
	sched := "test-spd-succ-desc-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-spd-succ-desc-") })

	seedTable(t, driver, sched, "svc", "s", "a", "img:OLD", "vOLD") // failed (target)
	seedTable(t, driver, sched, "svc", "s", "b", "img:OLD", "vOLD") // depends on a, but somehow SUCCEEDED
	addDependency(t, driver, sched, "s", "b", sched, "s", "a")

	srcRunID := uuid.New()
	srcA := uuid.New()
	srcB := uuid.New()
	seedSourceRunWithEdges(t, driver, srcRunID.String(), sched, []seededEdge{
		{Service: "svc", Schema: "s", Table: "a", Status: "FAILED", TaskID: srcA.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
		{Service: "svc", Schema: "s", Table: "b", Status: "SUCCEEDED", TaskID: srcB.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
	})

	got := runSelector(t, driver, snapshot.SourcePinnedDAG{
		TargetService: "svc", TargetSchema: "s", TargetTable: "a",
	}, snapshot.Params{SourceRunID: &srcRunID})
	by := indexProjectionByTable(got)

	assert.Equal(t, "PENDING", by["a"].InitialStatus, "target a → rebased")
	assert.Equal(t, "SUCCEEDED", by["b"].InitialStatus, "succeeded descendant should stay inherited")
	require.NotNil(t, by["b"].InheritedFromTaskID, "succeeded descendant should carry inherit pointer")
}

func TestSourcePinnedDAG_TargetNotInSource(t *testing.T) {
	driver := newDriver(t)
	srcRunID := uuid.New()
	// Empty source :Run (no edges).
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-spd-tnf-") })
	seedSourceRunWithEdges(t, driver, srcRunID.String(), "test-spd-tnf-x", nil)

	sess := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer sess.Close(context.Background())
	_, err := sess.ExecuteRead(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, ierr := snapshot.SourcePinnedDAG{
			TargetService: "svc", TargetSchema: "s", TargetTable: "missing",
		}.SelectTasks(context.Background(), tx, snapshot.Params{SourceRunID: &srcRunID})
		return nil, ierr
	})
	require.ErrorIs(t, err, snapshot.ErrTargetNotFound)
}

func TestSourcePinnedDAG_RootForwarding(t *testing.T) {
	// Rebase-of-rerun-style chain: source row for inherited 'a' has its own
	// inherited_from_task_id. New projection's a should point to that root,
	// not at the intermediate task_id.
	driver := newDriver(t)
	sched := "test-spd-root-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-spd-root-") })

	seedTable(t, driver, sched, "svc", "s", "a", "img:OLD", "vOLD")
	seedTable(t, driver, sched, "svc", "s", "b", "img:OLD", "vOLD")

	srcRunID := uuid.New()
	rootA := uuid.New()         // the original execution
	intermediateA := uuid.New() // source row's task_id (inherited)
	srcB := uuid.New()
	seedSourceRunWithEdges(t, driver, srcRunID.String(), sched, []seededEdge{
		{Service: "svc", Schema: "s", Table: "a", Status: "SUCCEEDED", TaskID: intermediateA.String(),
			ImageTag: "img:OLD", ManifestVersion: "vOLD", InheritedFromTaskID: rootA.String()},
		{Service: "svc", Schema: "s", Table: "b", Status: "FAILED", TaskID: srcB.String(),
			ImageTag: "img:OLD", ManifestVersion: "vOLD"},
	})

	got := runSelector(t, driver, snapshot.SourcePinnedDAG{
		TargetService: "svc", TargetSchema: "s", TargetTable: "b",
	}, snapshot.Params{SourceRunID: &srcRunID})
	by := indexProjectionByTable(got)

	require.NotNil(t, by["a"].InheritedFromTaskID)
	assert.Equal(t, rootA, *by["a"].InheritedFromTaskID,
		"resolve-to-root: pointer should jump past the intermediate inherited row")
}
