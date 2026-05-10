package neo4jinfra_test

import (
	"context"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/orchestrator/adapters/neo4j"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withTopologyReader is the test helper used by all TopologyReader tests.
// It:
//  1. Skips if Neo4j is unreachable (newTestClient skips on connectivity error).
//  2. Opens a write session, deletes all nodes (fresh scope).
//  3. Runs session.ExecuteWrite and inside the tx: calls fixture to seed data,
//     then calls exercise with the bound TopologyReader.
//  4. Fails the test on any error from fixture or exercise.
func withTopologyReader(
	t *testing.T,
	fixture func(ctx context.Context, tx neo4j.ManagedTransaction) error,
	exercise func(ctx context.Context, r snapshot.TopologyReader) error,
) {
	t.Helper()
	client := newTestClient(t) // skips if neo4j unreachable

	// newTestClient returns a Neo4jClient interface; cast to get NewSession.
	type sessioner interface {
		NewSession(context.Context, neo4j.AccessMode) neo4j.SessionWithContext
	}
	d, ok := client.(sessioner)
	require.True(t, ok, "neo4jClient must implement NewSession")

	ctx := context.Background()
	session := d.NewSession(ctx, neo4j.AccessModeWrite)
	defer session.Close(ctx)

	// Wipe all nodes for a fresh scope.
	r, err := session.Run(ctx, "MATCH (n) DETACH DELETE n", nil)
	require.NoError(t, err)
	_, err = r.Consume(ctx)
	require.NoError(t, err)

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (interface{}, error) {
		if fixtureErr := fixture(ctx, tx); fixtureErr != nil {
			return nil, fixtureErr
		}
		reader := neo4jinfra.NewTopologyReaderForTest(tx)
		return nil, exercise(ctx, reader)
	})
	require.NoError(t, err)
}

// ── helpers used by fixtures ──────────────────────────────────────────────────

func txMergeTable(ctx context.Context, tx neo4j.ManagedTransaction, sched, svc, schema, tbl, img, mv string, active bool) error {
	_, err := tx.Run(ctx, `
		MERGE (t:Table {service_name: $svc, schema_name: $schema, table_name: $tbl, schedule_name: $sched})
		ON CREATE SET t.active = $active, t.image_tag = $img, t.manifest_version = $mv, t.node_type = 'dbt-model'
		ON MATCH  SET t.active = $active, t.image_tag = $img, t.manifest_version = $mv`,
		map[string]interface{}{
			"svc": svc, "schema": schema, "tbl": tbl, "sched": sched,
			"img": img, "mv": mv, "active": active,
		})
	return err
}

func txAddDependency(ctx context.Context, tx neo4j.ManagedTransaction, fromSched, fromSchema, fromTable, toSched, toSchema, toTable string) error {
	_, err := tx.Run(ctx, `
		MATCH (a:Table {schedule_name: $fs, schema_name: $fSchema, table_name: $fTable})
		MATCH (b:Table {schedule_name: $ts, schema_name: $tSchema, table_name: $tTable})
		MERGE (a)-[:DEPENDS_ON]->(b)`,
		map[string]interface{}{
			"fs": fromSched, "fSchema": fromSchema, "fTable": fromTable,
			"ts": toSched, "tSchema": toSchema, "tTable": toTable,
		})
	return err
}

func txSeedRun(ctx context.Context, tx neo4j.ManagedTransaction, runID, sched string) error {
	_, err := tx.Run(ctx, `
		MERGE (r:Run {run_id: $run_id})
		ON CREATE SET r.schedule_name = $sched,
		              r.created_at = datetime(),
		              r.kind = 'cron',
		              r.topology_generation = 1,
		              r.service_metadata = '{}'`,
		map[string]interface{}{"run_id": runID, "sched": sched})
	return err
}

type txEdge struct {
	Svc, Schema, Tbl    string
	Status, TaskID      string
	Img, Mv             string
	InheritedFromTaskID string // empty = not inherited
	Sched               string // override schedule; empty = use outer sched
}

func txSeedExecEdges(ctx context.Context, tx neo4j.ManagedTransaction, runID, defaultSched string, edges []txEdge) error {
	for _, e := range edges {
		sched := e.Sched
		if sched == "" {
			sched = defaultSched
		}
		var inheritedFrom interface{}
		if e.InheritedFromTaskID != "" {
			inheritedFrom = e.InheritedFromTaskID
		}
		_, err := tx.Run(ctx, `
			MATCH (r:Run {run_id: $run_id})
			MATCH (t:Table {service_name: $svc, schema_name: $schema, table_name: $tbl, schedule_name: $sched})
			MERGE (r)-[ex:EXECUTES]->(t)
			ON CREATE SET ex.status = $status, ex.task_id = $task_id,
			              ex.image_tag = $img, ex.manifest_version = $mv
			FOREACH (_ IN CASE WHEN $inherited_from IS NULL THEN [] ELSE [1] END |
			    SET ex.inherited_from_task_id = $inherited_from
			)`,
			map[string]interface{}{
				"run_id": runID, "svc": e.Svc, "schema": e.Schema, "tbl": e.Tbl,
				"sched": sched, "status": e.Status, "task_id": e.TaskID,
				"img": e.Img, "mv": e.Mv, "inherited_from": inheritedFrom,
			})
		if err != nil {
			return err
		}
	}
	return nil
}

// ── LoadLatestSourceDAG ───────────────────────────────────────────────────────

func TestTopologyReader_LoadLatestSourceDAG(t *testing.T) {
	sched := "test-tr-lsd-" + uuid.New().String()[:8]

	withTopologyReader(t,
		func(ctx context.Context, tx neo4j.ManagedTransaction) error {
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "a", "img:1", "v1", true); err != nil {
				return err
			}
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "b", "img:2", "v2", true); err != nil {
				return err
			}
			return txAddDependency(ctx, tx, sched, "s", "a", sched, "s", "b")
		},
		func(ctx context.Context, r snapshot.TopologyReader) error {
			got, err := r.LoadLatestSourceDAG(ctx, sched)
			if err != nil {
				return err
			}
			if len(got) != 2 {
				t.Errorf("expected 2 rows, got %d", len(got))
				return nil
			}
			fqnA := snapshot.FQN{Service: "svc", Schema: "s", Table: "a"}
			fqnB := snapshot.FQN{Service: "svc", Schema: "s", Table: "b"}
			rowA, ok := got[fqnA]
			if !ok {
				t.Error("missing row for table a")
				return nil
			}
			assert.Equal(t, "img:1", rowA.ImageTag)
			assert.Equal(t, "v1", rowA.ManifestVersion)
			assert.Equal(t, sched, rowA.ScheduleName)
			assert.Equal(t, "dbt-model", rowA.NodeType)
			_, ok = got[fqnB]
			assert.True(t, ok, "missing row for table b")
			return nil
		},
	)
}

func TestTopologyReader_LoadLatestSourceDAG_EmptySchedule_ReturnsEmpty(t *testing.T) {
	withTopologyReader(t,
		func(ctx context.Context, tx neo4j.ManagedTransaction) error {
			return nil // no seed needed
		},
		func(ctx context.Context, r snapshot.TopologyReader) error {
			got, err := r.LoadLatestSourceDAG(ctx, "")
			require.NoError(t, err)
			assert.Empty(t, got, "empty schedule should return empty map")
			return nil
		},
	)
}

func TestTopologyReader_LoadLatestSourceDAG_InactiveTablesExcluded(t *testing.T) {
	sched := "test-tr-lsd-inactive-" + uuid.New().String()[:8]

	withTopologyReader(t,
		func(ctx context.Context, tx neo4j.ManagedTransaction) error {
			// active table
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "active", "img:1", "v1", true); err != nil {
				return err
			}
			// inactive table — should be excluded
			return txMergeTable(ctx, tx, sched, "svc", "s", "inactive", "img:2", "v2", false)
		},
		func(ctx context.Context, r snapshot.TopologyReader) error {
			got, err := r.LoadLatestSourceDAG(ctx, sched)
			require.NoError(t, err)
			assert.Len(t, got, 1, "inactive table must be excluded")
			fqnActive := snapshot.FQN{Service: "svc", Schema: "s", Table: "active"}
			_, ok := got[fqnActive]
			assert.True(t, ok, "active table must be present")
			fqnInactive := snapshot.FQN{Service: "svc", Schema: "s", Table: "inactive"}
			_, ok = got[fqnInactive]
			assert.False(t, ok, "inactive table must not be present")
			return nil
		},
	)
}

// ── LoadSourceTasks ───────────────────────────────────────────────────────────

func TestTopologyReader_LoadSourceTasks_RoundTripsInheritedFromTaskID(t *testing.T) {
	sched := "test-tr-lst-" + uuid.New().String()[:8]
	runID := uuid.New().String()
	taskA := uuid.New().String()
	rootA := uuid.New().String() // inherited_from for task a
	taskB := uuid.New().String()

	withTopologyReader(t,
		func(ctx context.Context, tx neo4j.ManagedTransaction) error {
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "a", "img:1", "v1", true); err != nil {
				return err
			}
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "b", "img:2", "v2", true); err != nil {
				return err
			}
			if err := txSeedRun(ctx, tx, runID, sched); err != nil {
				return err
			}
			return txSeedExecEdges(ctx, tx, runID, sched, []txEdge{
				{Svc: "svc", Schema: "s", Tbl: "a", Status: "SUCCEEDED", TaskID: taskA,
					Img: "img:1", Mv: "v1", InheritedFromTaskID: rootA},
				{Svc: "svc", Schema: "s", Tbl: "b", Status: "FAILED", TaskID: taskB,
					Img: "img:2", Mv: "v2"},
			})
		},
		func(ctx context.Context, r snapshot.TopologyReader) error {
			got, err := r.LoadSourceTasks(ctx, runID)
			require.NoError(t, err)
			require.Len(t, got, 2, "expected 2 source tasks")

			fqnA := snapshot.FQN{Service: "svc", Schema: "s", Table: "a"}
			rowA, ok := got[fqnA]
			require.True(t, ok, "missing row for a")
			assert.Equal(t, "SUCCEEDED", rowA.Status)
			assert.Equal(t, "img:1", rowA.ImageTag)
			require.NotNil(t, rowA.InheritedFromRoot, "inherited_from_task_id must be populated")
			assert.Equal(t, rootA, rowA.InheritedFromRoot.String())

			fqnB := snapshot.FQN{Service: "svc", Schema: "s", Table: "b"}
			rowB, ok := got[fqnB]
			require.True(t, ok, "missing row for b")
			assert.Equal(t, "FAILED", rowB.Status)
			assert.Nil(t, rowB.InheritedFromRoot, "b has no inherited_from")
			return nil
		},
	)
}

// ── DescendantsInLatestTopology ───────────────────────────────────────────────

func TestTopologyReader_DescendantsInLatestTopology_ActiveFilter(t *testing.T) {
	// Topology: c --DEPENDS_ON--> b --DEPENDS_ON--> a
	// d --DEPENDS_ON--> a but d is inactive.
	// DescendantsInLatestTopology(a) should return [b, c] but NOT d.
	sched := "test-tr-dlt-" + uuid.New().String()[:8]

	withTopologyReader(t,
		func(ctx context.Context, tx neo4j.ManagedTransaction) error {
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "a", "img:1", "v1", true); err != nil {
				return err
			}
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "b", "img:1", "v1", true); err != nil {
				return err
			}
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "c", "img:1", "v1", true); err != nil {
				return err
			}
			// inactive descendant
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "d", "img:1", "v1", false); err != nil {
				return err
			}
			if err := txAddDependency(ctx, tx, sched, "s", "b", sched, "s", "a"); err != nil {
				return err
			}
			if err := txAddDependency(ctx, tx, sched, "s", "c", sched, "s", "b"); err != nil {
				return err
			}
			return txAddDependency(ctx, tx, sched, "s", "d", sched, "s", "a")
		},
		func(ctx context.Context, r snapshot.TopologyReader) error {
			start := snapshot.FQN{Service: "svc", Schema: "s", Table: "a"}
			got, err := r.DescendantsInLatestTopology(ctx, start)
			require.NoError(t, err)

			tables := make(map[string]bool)
			for _, f := range got {
				tables[f.Table] = true
			}
			assert.True(t, tables["b"], "b should be a descendant")
			assert.True(t, tables["c"], "c should be a descendant")
			assert.False(t, tables["d"], "inactive d must not appear")
			assert.False(t, tables["a"], "start node must not be in descendants")
			return nil
		},
	)
}

// ── DescendantsInSourceRun ────────────────────────────────────────────────────

func TestTopologyReader_DescendantsInSourceRun_FilteredToSourceExecutes(t *testing.T) {
	// Topology: c --DEPENDS_ON--> b --DEPENDS_ON--> a
	// Source run has edges for a and b but NOT c.
	// DescendantsInSourceRun(a) should return only [b].
	sched := "test-tr-dsr-" + uuid.New().String()[:8]
	runID := uuid.New().String()

	withTopologyReader(t,
		func(ctx context.Context, tx neo4j.ManagedTransaction) error {
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "a", "img:1", "v1", true); err != nil {
				return err
			}
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "b", "img:1", "v1", true); err != nil {
				return err
			}
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "c", "img:1", "v1", true); err != nil {
				return err
			}
			if err := txAddDependency(ctx, tx, sched, "s", "b", sched, "s", "a"); err != nil {
				return err
			}
			if err := txAddDependency(ctx, tx, sched, "s", "c", sched, "s", "b"); err != nil {
				return err
			}
			if err := txSeedRun(ctx, tx, runID, sched); err != nil {
				return err
			}
			// Source run only covers a and b; c is NOT in the source run.
			return txSeedExecEdges(ctx, tx, runID, sched, []txEdge{
				{Svc: "svc", Schema: "s", Tbl: "a", Status: "FAILED", TaskID: uuid.New().String(), Img: "img:1", Mv: "v1"},
				{Svc: "svc", Schema: "s", Tbl: "b", Status: "PENDING", TaskID: uuid.New().String(), Img: "img:1", Mv: "v1"},
			})
		},
		func(ctx context.Context, r snapshot.TopologyReader) error {
			start := snapshot.FQN{Service: "svc", Schema: "s", Table: "a"}
			got, err := r.DescendantsInSourceRun(ctx, runID, start)
			require.NoError(t, err)

			tables := make(map[string]bool)
			for _, f := range got {
				tables[f.Table] = true
			}
			assert.True(t, tables["b"], "b is in source run and is a descendant")
			assert.False(t, tables["c"], "c is NOT in source run — must be excluded")
			assert.False(t, tables["a"], "start node must not be in descendants")
			return nil
		},
	)
}

// ── LoadSingleLatestTable ─────────────────────────────────────────────────────

func TestTopologyReader_LoadSingleLatestTable_HitAndMiss(t *testing.T) {
	sched := "test-tr-slt-" + uuid.New().String()[:8]

	withTopologyReader(t,
		func(ctx context.Context, tx neo4j.ManagedTransaction) error {
			return txMergeTable(ctx, tx, sched, "svc", "s", "x", "img:7", "v7", true)
		},
		func(ctx context.Context, r snapshot.TopologyReader) error {
			// Hit
			row, found, err := r.LoadSingleLatestTable(ctx, snapshot.FQN{Service: "svc", Schema: "s", Table: "x"})
			require.NoError(t, err)
			require.True(t, found, "existing active table must be found")
			assert.Equal(t, "img:7", row.ImageTag)
			assert.Equal(t, "v7", row.ManifestVersion)
			assert.Equal(t, sched, row.ScheduleName)
			assert.Equal(t, "dbt-model", row.NodeType)

			// Miss — returns (zero, false, nil)
			zero, found, err := r.LoadSingleLatestTable(ctx, snapshot.FQN{Service: "svc", Schema: "s", Table: "does-not-exist"})
			require.NoError(t, err)
			assert.False(t, found, "missing table must return found=false")
			assert.Equal(t, snapshot.LatestTableRow{}, zero, "miss must return zero value")
			return nil
		},
	)
}

func TestTopologyReader_LoadSingleLatestTable_InactiveExcluded(t *testing.T) {
	sched := "test-tr-slt-inactive-" + uuid.New().String()[:8]

	withTopologyReader(t,
		func(ctx context.Context, tx neo4j.ManagedTransaction) error {
			return txMergeTable(ctx, tx, sched, "svc", "s", "y", "img:1", "v1", false)
		},
		func(ctx context.Context, r snapshot.TopologyReader) error {
			_, found, err := r.LoadSingleLatestTable(ctx, snapshot.FQN{Service: "svc", Schema: "s", Table: "y"})
			require.NoError(t, err)
			assert.False(t, found, "inactive table must not be found")
			return nil
		},
	)
}

// ── LoadSingleTableFromSourceRun ──────────────────────────────────────────────

func TestTopologyReader_LoadSingleTableFromSourceRun_HitAndMiss(t *testing.T) {
	sched := "test-tr-sfr-" + uuid.New().String()[:8]
	runID := uuid.New().String()
	taskID := uuid.New().String()

	withTopologyReader(t,
		func(ctx context.Context, tx neo4j.ManagedTransaction) error {
			if err := txMergeTable(ctx, tx, sched, "svc", "s", "z", "img:NEW", "vNEW", true); err != nil {
				return err
			}
			if err := txSeedRun(ctx, tx, runID, sched); err != nil {
				return err
			}
			// Source edge pins OLD metadata (different from latest table).
			return txSeedExecEdges(ctx, tx, runID, sched, []txEdge{
				{Svc: "svc", Schema: "s", Tbl: "z", Status: "SUCCEEDED", TaskID: taskID,
					Img: "img:OLD", Mv: "vOLD"},
			})
		},
		func(ctx context.Context, r snapshot.TopologyReader) error {
			// Hit — must return source edge metadata (OLD), not latest table metadata.
			row, found, err := r.LoadSingleTableFromSourceRun(ctx, runID, snapshot.FQN{Service: "svc", Schema: "s", Table: "z"})
			require.NoError(t, err)
			require.True(t, found, "table in source run must be found")
			assert.Equal(t, "img:OLD", row.ImageTag, "must read from source :EXECUTES edge, not latest table")
			assert.Equal(t, "vOLD", row.ManifestVersion)
			assert.Equal(t, sched, row.ScheduleName)

			// Miss — table not in source run.
			zero, found, err := r.LoadSingleTableFromSourceRun(ctx, runID, snapshot.FQN{Service: "svc", Schema: "s", Table: "absent"})
			require.NoError(t, err)
			assert.False(t, found, "absent table must return found=false")
			assert.Equal(t, snapshot.LatestTableRow{}, zero)
			return nil
		},
	)
}
