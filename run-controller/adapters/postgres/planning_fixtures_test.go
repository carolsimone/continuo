package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// releaseShape is a synthetic release: levels of dbt models named nKK_IIII, each reading fanIn nodes of
// the level above. With failRoot, level 0 is the single node fail_root. With
// tests, every node has one data test.
type releaseShape struct {
	name     string
	nodes    int
	levels   int
	fanIn    int
	failRoot bool
	tests    bool
}

var cascade2000 = releaseShape{name: "cascade-2000", nodes: 2000, levels: 20, fanIn: 2, failRoot: true}

// schedule is the schedule every node of the shape belongs to.
func (s releaseShape) schedule() string { return "bench-" + s.name }

func (s releaseShape) levelSizes() []int {
	if s.failRoot {
		rest := releaseShape{nodes: s.nodes - 1, levels: s.levels - 1}
		return append([]int{1}, rest.levelSizes()...)
	}
	base, extra := s.nodes/s.levels, s.nodes%s.levels
	sizes := make([]int, s.levels)
	for i := range sizes {
		sizes[i] = base
		if i < extra {
			sizes[i]++
		}
	}
	return sizes
}

// release builds the shape as release releaseID of tenant.
func (s releaseShape) release(tb testing.TB, tenant, releaseID string) topology.Release {
	tb.Helper()
	var names [][]string
	for level, size := range s.levelSizes() {
		row := make([]string, 0, size)
		if level == 0 && s.failRoot {
			row = append(row, "fail_root")
		} else {
			for index := range size {
				row = append(row, fmt.Sprintf("n%02d_%04d", level, index))
			}
		}
		names = append(names, row)
	}
	testCount := 0
	if s.tests {
		testCount = 1
	}
	var specs []topology.NodeSpec
	for level, row := range names {
		for index, table := range row {
			var upstream []string
			if level > 0 {
				previous := names[level-1]
				for offset := range min(s.fanIn, len(previous)) {
					upstream = append(upstream, "bench."+previous[(index+offset)%len(previous)])
				}
			}
			count := testCount
			specs = append(specs, topology.NodeSpec{
				UniqueID: "bench." + table, ServiceName: "bench-svc", SchemaName: "bench", TableName: table,
				NodeType: "dbt-model", TestCount: &count, ContentHash: "h-" + table, ImageTag: "bench-svc:1",
				ScheduleName: s.schedule(), UpstreamUniqueIDs: upstream,
			})
		}
	}
	rel, err := topology.NewRelease(tenant, releaseID, 1, "s3://continuo/tenants/"+tenant+"/topologies/"+releaseID+"/topology.json.gz", "sha-"+releaseID, specs)
	require.NoError(tb, err)
	return rel
}

// planningTenant returns a tenant id no other test uses and deletes its
// topology rows when the test ends.
func planningTenant(tb testing.TB, db *sqlx.DB) string {
	tb.Helper()
	tenant := "plan-" + uuid.NewString()
	tb.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM live_release WHERE tenant_id = $1`, tenant)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM topology_releases WHERE tenant_id = $1`, tenant)
	})
	return tenant
}

// storeShape stores the shape as release rel-1 of a new tenant and returns
// the tenant.
func storeShape(tb testing.TB, db *sqlx.DB, s releaseShape) string {
	tb.Helper()
	ctx := context.Background()
	tenant := planningTenant(tb, db)
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(tb, err)
	defer tx.Rollback()
	_, err = postgres.NewTopologyRepository(db, tx).SaveRelease(ctx, s.release(tb, tenant, "rel-1"))
	require.NoError(tb, err)
	require.NoError(tb, tx.Commit())
	return tenant
}

// startScheduleRun creates, starts and commits a run of schedule on release
// rel-1 of tenant, the way a schedule's run is created: the run row, then its
// tasks and plan, then one outbox row per ready task. The run, its tasks and
// its outbox rows are deleted when the test ends.
func startScheduleRun(tb testing.TB, db *sqlx.DB, tenant, schedule string, op model.Operation) *run.Run {
	tb.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	r, err := run.NewPendingRun(tenant, uuid.New(), schedule, run.KindCron, nil, identity.SystemUserID, op,
		run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 1}, now)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM run_controller_outbox WHERE aggregate_id = $1`, r.RunID())
		_, _ = db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = $1`, r.RunID())
	})

	u := newPlanningUoW(db)
	require.NoError(tb, u.Begin(ctx))
	defer u.Rollback()
	g, err := u.Topology().ScheduleGraph(ctx, tenant, "rel-1", schedule)
	require.NoError(tb, err)
	plan, err := runplan.WholeDAG{Operation: op}.Select(g)
	require.NoError(tb, err)
	require.NoError(tb, u.Run().SaveRun(ctx, r))
	events, err := r.Start(ctx, u.TaskCollection(tenant), plan, g, testTaskDefaults, now)
	require.NoError(tb, err)
	require.NoError(tb, u.Run().SaveRun(ctx, r))
	require.NoError(tb, u.Outbox().Append(ctx, events, uuid.Nil))
	require.NoError(tb, u.Commit())
	return r
}

// setTaskStatus writes status on the task of r at nodeKey.
func setTaskStatus(tb testing.TB, db *sqlx.DB, r *run.Run, nodeKey string, status run.TaskStatus) {
	tb.Helper()
	res, err := db.Exec(`UPDATE tasks SET status = $3 WHERE run_id = $1 AND node_key = $2`,
		r.RunID(), nodeKey, string(status))
	require.NoError(tb, err)
	n, err := res.RowsAffected()
	require.NoError(tb, err)
	require.Equal(tb, int64(1), n, "no task at %s", nodeKey)
}

// succeedWithAttempts marks every task of runID succeeded, each with one
// succeeded attempt whose duration grows with its position in key order; a
// dispatched task's attempt 0 becomes that attempt.
func succeedWithAttempts(tb testing.TB, db *sqlx.DB, runID uuid.UUID) {
	tb.Helper()
	_, err := db.Exec(`UPDATE tasks SET status = 'succeeded' WHERE run_id = $1`, runID)
	require.NoError(tb, err)
	_, err = db.Exec(`
		INSERT INTO task_attempts (tenant_id, task_id, attempt, job_name, status, started_at, finished_at, execution_seconds)
		SELECT tenant_id, task_id, 0, job_name, 'succeeded', now(), now(), 1 + row_number() OVER (ORDER BY node_key)
		FROM tasks WHERE run_id = $1
		ON CONFLICT (tenant_id, task_id, attempt) DO UPDATE SET status = EXCLUDED.status, started_at = EXCLUDED.started_at,
			finished_at = EXCLUDED.finished_at, execution_seconds = EXCLUDED.execution_seconds, next_check_at = NULL`, runID)
	require.NoError(tb, err)
}
