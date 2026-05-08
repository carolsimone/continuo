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

// ── Task 8.1: happy path — one failure, no descendants ──

func TestRebasePartition_OneFailureNoDescendants(t *testing.T) {
	driver := newDriver(t)
	sched := "test-rb-1-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-rb-1-") })

	// Latest topology: a, b (both active).
	seedTable(t, driver, sched, "svc", "s", "a", "img:NEW", "vNEW")
	seedTable(t, driver, sched, "svc", "s", "b", "img:NEW", "vNEW")

	// Source :Run: a SUCCEEDED, b FAILED. Source's edge metadata is OLD.
	srcRunID := uuid.New()
	srcA := uuid.New()
	srcB := uuid.New()
	seedSourceRunWithEdges(t, driver, srcRunID.String(), sched, []seededEdge{
		{Service: "svc", Schema: "s", Table: "a", Status: "SUCCEEDED",
			TaskID: srcA.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
		{Service: "svc", Schema: "s", Table: "b", Status: "FAILED",
			TaskID: srcB.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
	})

	got := runSelector(t, driver, snapshot.RebasePartition{}, snapshot.Params{SourceRunID: &srcRunID})
	require.Len(t, got, 2)
	by := indexProjectionByTable(got)

	// b: rebased — PENDING with LATEST metadata.
	require.Equal(t, "PENDING", by["b"].InitialStatus)
	require.Nil(t, by["b"].InheritedFromTaskID)
	require.Equal(t, "img:NEW", by["b"].ImageTag, "rebased task uses latest topology metadata")
	require.Equal(t, "vNEW", by["b"].ManifestVersion)

	// a: inherited — SUCCEEDED with SOURCE's pinned metadata.
	require.Equal(t, "SUCCEEDED", by["a"].InitialStatus)
	require.NotNil(t, by["a"].InheritedFromTaskID)
	assert.Equal(t, srcA, *by["a"].InheritedFromTaskID, "inherit pointer = source task_id")
	require.Equal(t, "img:OLD", by["a"].ImageTag, "inherited task carries source's pinned metadata")
	require.Equal(t, "vOLD", by["a"].ManifestVersion)
}

// ── Task 8.2: cascade test ──

func TestRebasePartition_FailureCascadesToLatestDescendants(t *testing.T) {
	driver := newDriver(t)
	sched := "test-rb-cascade-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-rb-cascade-") })

	// Latest topology: a, b, c (c depends on b in latest).
	seedTable(t, driver, sched, "svc", "s", "a", "img:NEW", "vNEW")
	seedTable(t, driver, sched, "svc", "s", "b", "img:NEW", "vNEW")
	seedTable(t, driver, sched, "svc", "s", "c", "img:NEW", "vNEW")
	addDependency(t, driver, sched, "s", "c", sched, "s", "b")

	// Source: a SUCCEEDED, b FAILED, c SUCCEEDED.
	// Even though c was SUCCEEDED in source, its upstream b is being rebased
	// in latest topology → c must rebase too (cascade).
	srcRunID := uuid.New()
	srcA := uuid.New()
	srcB := uuid.New()
	srcC := uuid.New()
	seedSourceRunWithEdges(t, driver, srcRunID.String(), sched, []seededEdge{
		{Service: "svc", Schema: "s", Table: "a", Status: "SUCCEEDED",
			TaskID: srcA.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
		{Service: "svc", Schema: "s", Table: "b", Status: "FAILED",
			TaskID: srcB.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
		{Service: "svc", Schema: "s", Table: "c", Status: "SUCCEEDED",
			TaskID: srcC.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
	})

	got := runSelector(t, driver, snapshot.RebasePartition{}, snapshot.Params{SourceRunID: &srcRunID})
	require.Len(t, got, 3)
	by := indexProjectionByTable(got)

	require.Equal(t, "SUCCEEDED", by["a"].InitialStatus, "a inherits (no upstream changes)")
	require.NotNil(t, by["a"].InheritedFromTaskID)

	require.Equal(t, "PENDING", by["b"].InitialStatus, "b is the failed source task → rebased")
	require.Nil(t, by["b"].InheritedFromTaskID)

	require.Equal(t, "PENDING", by["c"].InitialStatus, "c rebases because its upstream b is rebased (cascade)")
	require.Nil(t, by["c"].InheritedFromTaskID)
}

// ── Task 8.3: new arrivals + drop set + root forwarding ──

func TestRebasePartition_NewArrival_RebasedPending(t *testing.T) {
	driver := newDriver(t)
	sched := "test-rb-new-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-rb-new-") })

	// Latest topology has 'd' (new arrival not in source).
	seedTable(t, driver, sched, "svc", "s", "a", "img:NEW", "vNEW")
	seedTable(t, driver, sched, "svc", "s", "b", "img:NEW", "vNEW")
	seedTable(t, driver, sched, "svc", "s", "d", "img:NEW", "vNEW")

	srcRunID := uuid.New()
	seedSourceRunWithEdges(t, driver, srcRunID.String(), sched, []seededEdge{
		{Service: "svc", Schema: "s", Table: "a", Status: "SUCCEEDED",
			TaskID: uuid.New().String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
		{Service: "svc", Schema: "s", Table: "b", Status: "FAILED",
			TaskID: uuid.New().String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
	})

	got := runSelector(t, driver, snapshot.RebasePartition{}, snapshot.Params{SourceRunID: &srcRunID})
	by := indexProjectionByTable(got)

	require.Contains(t, by, "d", "new arrival 'd' must appear in projection")
	require.Equal(t, "PENDING", by["d"].InitialStatus, "new arrival is rebased")
	require.Nil(t, by["d"].InheritedFromTaskID, "new arrival has no inherit pointer")
	require.Equal(t, "img:NEW", by["d"].ImageTag, "new arrival uses latest metadata")
}

func TestRebasePartition_DropSet_SilentlyExcluded(t *testing.T) {
	driver := newDriver(t)
	sched := "test-rb-drop-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-rb-drop-") })

	// Latest topology: a, b only. 'x' NOT in latest (was dropped from manifest).
	seedTable(t, driver, sched, "svc", "s", "a", "img:NEW", "vNEW")
	seedTable(t, driver, sched, "svc", "s", "b", "img:NEW", "vNEW")
	// 'x' exists as an inactive :Table so we can attach a source :EXECUTES edge,
	// but readLatestTables will filter it out via active=true.
	seedInactiveTable(t, driver, sched, "svc", "s", "x", "img:OLD", "vOLD")

	// Source had 'a', 'b', and 'x' — 'x' must be dropped from the projection.
	srcRunID := uuid.New()
	seedSourceRunWithEdges(t, driver, srcRunID.String(), sched, []seededEdge{
		{Service: "svc", Schema: "s", Table: "a", Status: "SUCCEEDED",
			TaskID: uuid.New().String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
		{Service: "svc", Schema: "s", Table: "b", Status: "FAILED",
			TaskID: uuid.New().String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
		{Service: "svc", Schema: "s", Table: "x", Status: "SUCCEEDED",
			TaskID: uuid.New().String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
	})

	got := runSelector(t, driver, snapshot.RebasePartition{}, snapshot.Params{SourceRunID: &srcRunID})
	by := indexProjectionByTable(got)

	require.NotContains(t, by, "x", "dropped table x must NOT appear in projection")
	require.Contains(t, by, "a")
	require.Contains(t, by, "b")
}

func TestRebasePartition_RebaseOfRebase_RootForwarding(t *testing.T) {
	driver := newDriver(t)
	sched := "test-rb-root-" + uuid.New().String()[:8]
	t.Cleanup(func() { cleanupRunAndTables(t, driver, "", "test-rb-root-") })

	seedTable(t, driver, sched, "svc", "s", "a", "img:NEW", "vNEW")
	seedTable(t, driver, sched, "svc", "s", "b", "img:NEW", "vNEW")

	// Source row for 'a' is itself inherited (chain depth 1: rebase-of-rebase).
	// New projection for 'a' should point to the ROOT, not the intermediate.
	rootA := uuid.New()
	intermediateA := uuid.New()
	srcRunID := uuid.New()
	seedSourceRunWithEdges(t, driver, srcRunID.String(), sched, []seededEdge{
		{Service: "svc", Schema: "s", Table: "a", Status: "SUCCEEDED",
			TaskID: intermediateA.String(), ImageTag: "img:OLD", ManifestVersion: "vOLD",
			InheritedFromTaskID: rootA.String()},
		{Service: "svc", Schema: "s", Table: "b", Status: "FAILED",
			TaskID: uuid.New().String(), ImageTag: "img:OLD", ManifestVersion: "vOLD"},
	})

	got := runSelector(t, driver, snapshot.RebasePartition{}, snapshot.Params{SourceRunID: &srcRunID})
	by := indexProjectionByTable(got)

	require.NotNil(t, by["a"].InheritedFromTaskID)
	assert.Equal(t, rootA, *by["a"].InheritedFromTaskID,
		"resolve-to-root: inherit pointer must skip the intermediate task_id")
}

// ── seedInactiveTable helper (mirrors seedTable but sets active=false) ──

func seedInactiveTable(t *testing.T, driver neo4j.DriverWithContext, scheduleName, service, schema, table, image, manifest string) {
	t.Helper()
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(context.Background())
	_, err := session.ExecuteWrite(context.Background(), func(tx neo4j.ManagedTransaction) (interface{}, error) {
		_, err := tx.Run(context.Background(), `
			MERGE (t:Table {service_name: $svc, schema_name: $schema, table_name: $tbl, schedule_name: $sched})
			ON CREATE SET t.active = false, t.image_tag = $img, t.manifest_version = $mv, t.node_type = 'dbt-model'
			ON MATCH  SET t.active = false, t.image_tag = $img, t.manifest_version = $mv`,
			map[string]interface{}{
				"svc": service, "schema": schema, "tbl": table, "sched": scheduleName,
				"img": image, "mv": manifest,
			})
		return nil, err
	})
	require.NoError(t, err)
}
