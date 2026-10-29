package postgres_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/runreads"
	"github.com/carolsimone/continuo/run-controller/service/runstart"
	"github.com/carolsimone/continuo/run-controller/service/uow"
)

// BenchmarkPreviewRebase measures a rebase preview on dag-2000: a failed run
// of rel-1 whose bench.n15_0000 failed and skipped its descendants, every
// other task succeeded with one execution, and a live rel-2 that changed ten
// ancestors of the failure. "preview" is the whole PreviewRebase read (the
// selection in a rolled-back transaction and the changed nodes' durations);
// "last-success-durations" is the duration read alone over all 2,000 nodes.
//
//	go test -tags integration -run '^$' -bench PreviewRebase -benchtime 20x ./adapters/postgres/
func BenchmarkPreviewRebase(b *testing.B) {
	ctx := context.Background()
	db := newTestDB(b)
	tenant, runID, changed := failedRunWithChangedAncestors(b, db, dag2000, "bench.n15_0000", 10)

	b.Run("preview", func(b *testing.B) {
		previewer := runreads.NewRebasePreviewer(runstart.Planner{Tasks: testTaskDefaults},
			func() uow.UnitOfWork { return newPlanningUoW(db) }, postgres.NewRunReadRepository(db))
		pv, err := previewer.Preview(ctx, tenant, runID, true)
		require.NoError(b, err)
		require.Len(b, pv.Preview.Changed, changed)
		b.ResetTimer()
		for range b.N {
			_, err := previewer.Preview(ctx, tenant, runID, true)
			require.NoError(b, err)
		}
	})

	b.Run("last-success-durations", func(b *testing.B) {
		nodes := make([]run.NodeID, 0, dag2000.nodes)
		for _, n := range dag2000.release(b, tenant, "rel-1").Nodes {
			nodes = append(nodes, run.NodeID{ServiceName: n.ServiceName, SchemaName: n.SchemaName, TableName: n.TableName})
		}
		var succeeded int
		require.NoError(b, db.Get(&succeeded, `SELECT count(*) FROM tasks WHERE run_id = $1 AND status = 'succeeded'`, runID))
		repo := postgres.NewRunReadRepository(db)
		b.ResetTimer()
		for range b.N {
			got, err := repo.LastSuccessDurations(ctx, tenant, nodes)
			require.NoError(b, err)
			require.Len(b, got, succeeded)
		}
	})
}

// failedRunWithChangedAncestors stores shape as rel-1 of a new tenant, runs
// it, fails failed and skips its descendants, records one successful
// execution for every other task, and makes live a rel-2 whose content hash
// differs on up to n ancestors of failed. It returns the tenant, the failed
// run and how many ancestors changed.
func failedRunWithChangedAncestors(tb testing.TB, db *sqlx.DB, shape releaseShape, failed topology.NodeKey, n int) (string, uuid.UUID, int) {
	tb.Helper()
	ctx := context.Background()
	tenant := storeShape(tb, db, shape)
	r := startScheduleRun(tb, db, tenant, shape.schedule(), model.OperationRun)
	succeedWithAttempts(tb, db, r.RunID())

	rel := shape.release(tb, tenant, "rel-2")
	g := topology.NewGraph(rel.Nodes, rel.Edges)
	descendants := g.Descendants([]topology.NodeKey{failed})
	ancestors := g.Ancestors([]topology.NodeKey{failed})
	slices.Reverse(ancestors)
	changed := ancestors[:min(n, len(ancestors))]
	for i := range rel.Nodes {
		if slices.Contains(changed, rel.Nodes[i].Key) {
			rel.Nodes[i].ContentHash += "-changed"
		}
	}
	rel.PromotionSeq = 2

	_, err := db.Exec(`UPDATE tasks SET status = 'failed' WHERE run_id = $1 AND node_key = $2`, r.RunID(), string(failed))
	require.NoError(tb, err)
	skipped := make([]string, 0, len(descendants))
	for _, k := range descendants {
		skipped = append(skipped, string(k))
	}
	_, err = db.Exec(`UPDATE tasks SET status = 'skipped' WHERE run_id = $1 AND node_key = ANY($2)`, r.RunID(), pq.Array(skipped))
	require.NoError(tb, err)
	_, err = db.Exec(`UPDATE runs SET status = 'failed', completed_at = now() WHERE run_id = $1`, r.RunID())
	require.NoError(tb, err)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(tb, err)
	defer tx.Rollback()
	topo := postgres.NewTopologyRepository(db, tx)
	_, err = topo.SaveRelease(ctx, rel)
	require.NoError(tb, err)
	require.NoError(tb, topo.SetLivePointer(ctx, tenant, topology.Pointer{ReleaseID: "rel-2", PromotionSeq: 2}, time.Now()))
	require.NoError(tb, tx.Commit())
	_, err = db.ExecContext(ctx, `ANALYZE topology_nodes, topology_edges, tasks, task_attempts`)
	require.NoError(tb, err)
	return tenant, r.RunID(), len(changed)
}
