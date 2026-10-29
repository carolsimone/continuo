package test

import (
	"context"
	"strings"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/runstart"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// defaultPlanner plans runs with the task defaults run-controller's main uses.
var defaultPlanner = runstart.Planner{Tasks: run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}}

// releaseLeasesOf deletes the node write leases the tasks of runID hold, as
// the transactions that end those tasks do. A fixture that ends a run or its
// tasks by SQL calls it, so a later run of the same nodes can take them.
func releaseLeasesOf(t *testing.T, db *sqlx.DB, runID uuid.UUID) {
	t.Helper()
	_, err := db.Exec(`DELETE FROM node_leases WHERE task_id IN (SELECT task_id FROM tasks WHERE run_id = $1)`, runID)
	require.NoError(t, err)
}

// scheduledNode is promotedNode in schedule.
func scheduledNode(schema, table, nodeType, schedule string, upstream ...string) topology.NodeSpec {
	n := promotedNode(schema, table, nodeType, upstream...)
	n.ScheduleName = schedule
	return n
}

// promoteSchedules stores a new release of the default tenant holding one dbt
// model per schedule, mart.<schedule> with dashes as underscores, and makes it
// the live release under the next promotion seq. It returns the release.
func promoteSchedules(t *testing.T, db *sqlx.DB, schedules ...string) run.ReleaseStamp {
	t.Helper()
	specs := make([]topology.NodeSpec, 0, len(schedules))
	for _, s := range schedules {
		specs = append(specs, scheduledNode("mart", strings.ReplaceAll(s, "-", "_"), "dbt-model", s))
	}
	return promoteRelease(t, db, specs...)
}

// promoteRelease stores a new release of the default tenant holding specs and
// makes it the live release under the next promotion seq. It returns the
// release.
func promoteRelease(t *testing.T, db *sqlx.DB, specs ...topology.NodeSpec) run.ReleaseStamp {
	t.Helper()
	ctx := context.Background()
	releaseID := "rel-" + uuid.NewString()[:8]
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	topo := postgres.NewTopologyRepository(db, tx)
	require.NoError(t, topo.LockTopology(ctx))
	live, err := topo.LivePointer(ctx, pkgevents.DefaultTenantID)
	require.NoError(t, err)
	seq := live.PromotionSeq + 1
	rel, err := topology.NewRelease(pkgevents.DefaultTenantID, releaseID, seq,
		"s3://continuo/tenants/default/topologies/"+releaseID+"/topology.json.gz", "sha-"+releaseID, specs)
	require.NoError(t, err)
	_, err = topo.SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.NoError(t, topo.SetLivePointer(ctx, pkgevents.DefaultTenantID, topology.Pointer{ReleaseID: releaseID, PromotionSeq: seq}, time.Now()))
	require.NoError(t, tx.Commit())
	return run.ReleaseStamp{ReleaseID: releaseID, PromotionSeq: seq}
}
