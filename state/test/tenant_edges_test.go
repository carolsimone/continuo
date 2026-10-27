package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/adapters/redis"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
)

// edgeTenant is a tenant of its own, holding one live release with one node in
// its own schedule, and one running run of that schedule whose task's attempt
// 0 is dispatched. Every row of the tenant, and every dedup row of the reports
// the test delivers, is deleted when the test ends.
type edgeTenant struct {
	id       string
	schedule string
	table    string
	runID    uuid.UUID
	taskID   uuid.UUID
	messages []string
}

// newEdgeTenant stores the tenant's release, catalog row and running run.
func newEdgeTenant(t *testing.T, db *sqlx.DB) *edgeTenant {
	t.Helper()
	ctx := context.Background()
	suffix := uuid.NewString()[:8]
	e := &edgeTenant{id: "t-" + uuid.NewString(), schedule: "edge-" + suffix, table: "edge_" + suffix}
	t.Cleanup(func() { e.cleanup(db) })

	releaseID := "rel-" + suffix
	rel, err := topology.NewRelease(e.id, releaseID, 1,
		"s3://continuo/tenants/"+e.id+"/topologies/"+releaseID+"/topology.json.gz", "sha-"+releaseID,
		[]topology.NodeSpec{scheduledNode("raw", e.table, "dbt-model", e.schedule)})
	require.NoError(t, err)
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	topo := postgres.NewTopologyRepository(db, tx)
	require.NoError(t, topo.LockTopology(ctx))
	_, err = topo.SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.NoError(t, topo.SetLivePointer(ctx, e.id, topology.Pointer{ReleaseID: releaseID, PromotionSeq: 1}, time.Now()))
	require.NoError(t, tx.Commit())
	_, err = db.Exec(`INSERT INTO schedule_catalog (tenant_id, schedule_name, first_seen_at, last_seen_at) VALUES ($1, $2, now(), now())`,
		e.id, e.schedule)
	require.NoError(t, err)

	h := newBuiltHarness(db)
	id, outcome, err := svchandlers.NewStartRunHandler(defaultPlanner, h.logger).Handle(ctx, h.factory(), e.id, e.schedule,
		run.KindTrigger, nil, identity.FromUserID("okta|alice"), model.OperationRun)
	require.NoError(t, err)
	require.Equal(t, svchandlers.OutcomeRunStarted, outcome)
	e.runID = id
	e.taskID = run.TaskIDFor(e.id, id, topology.KeyOf("raw", e.table))
	return e
}

// cleanup deletes every row of the tenant and the dedup rows of its reports.
func (e *edgeTenant) cleanup(db *sqlx.DB) {
	ctx := context.Background()
	_, _ = db.ExecContext(ctx, `DELETE FROM state_outbox WHERE aggregate_id = $1`, e.runID)
	_, _ = db.ExecContext(ctx, `DELETE FROM message_processing WHERE message_id = ANY($1)`, pq.Array(e.messages))
	for _, table := range []string{
		"runs", "node_leases", "node_materializations", "schedule_catalog", "schedule_catalog_state",
		"live_release", "topology_releases",
	} {
		_, _ = db.ExecContext(ctx, `DELETE FROM `+table+` WHERE tenant_id = $1`, e.id)
	}
}

// node is the request fields naming the tenant's node.
func (e *edgeTenant) node() (service, schema, table string) { return "service-1", "raw", e.table }

// attemptStatus reads the status of attempt 0 of the tenant's task.
func (e *edgeTenant) attemptStatus(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	var s string
	require.NoError(t, db.Get(&s, `SELECT status FROM task_attempts WHERE tenant_id = $1 AND task_id = $2 AND attempt = 0`,
		e.id, e.taskID))
	return s
}

// runStatus reads the status of the tenant's run.
func (e *edgeTenant) runStatus(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	var s string
	require.NoError(t, db.Get(&s, `SELECT status FROM runs WHERE tenant_id = $1 AND run_id = $2`, e.id, e.runID))
	return s
}

// report is a queued report of attempt 0 of the tenant's task whose envelope
// names envelopeTenant, as a task.status.updated:v2 entry with a stream id of
// its own.
func (e *edgeTenant) report(t *testing.T, envelopeTenant string) goredis.XMessage {
	t.Helper()
	queued := time.Now().UTC()
	fields, err := pkgevents.TaskStatusReportFields(uuid.NewString(), envelopeTenant, "execution-controller", queued,
		pkgevents.TaskStatusReport{
			RunID: e.runID.String(), TaskID: e.taskID.String(), Attempt: 0,
			JobName: "edge-job", Status: model.AttemptStatus(run.AttemptQueued), QueuedAt: &queued,
		})
	require.NoError(t, err)
	id := fmt.Sprintf("%d-%d", time.Now().UnixMilli(), len(e.messages))
	e.messages = append(e.messages, id)
	return goredis.XMessage{ID: id, Values: fields}
}

// requireCode asserts err is a gRPC status of code.
func requireCode(t *testing.T, err error, code codes.Code, what string) {
	t.Helper()
	require.Error(t, err, what)
	assert.Equal(t, code, status.Code(err), "%s: %v", what, err)
}

// TestTenantEdges drives the places where work enters state against a run of
// a tenant other than the one the gRPC edge resolves, default. Every gRPC read
// and command reads that tenant's run, task, schedule and node as ones that do
// not exist and leaves the run as it was. A task.status.updated:v2 report
// finds the run only under the tenant its envelope names: under another it is
// permanent, so the consumer dead-letters it, and under the run's own it is
// applied.
func TestTenantEdges(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	e := newEdgeTenant(t, stateDB)
	require.Equal(t, string(run.RunStatusRunning), e.runStatus(t, stateDB))
	require.Equal(t, string(run.AttemptDispatched), e.attemptStatus(t, stateDB))
	runID := e.runID.String()
	service, schema, table := e.node()

	t.Run("reads", func(t *testing.T) {
		_, err := stateClient.GetScheduler(ctx, &statev1.GetSchedulerRequest{ScheduleId: runID})
		requireCode(t, err, codes.NotFound, "GetScheduler")
		_, err = stateClient.GetSchedulerInitStatus(ctx, &statev1.GetSchedulerInitStatusRequest{ScheduleId: runID})
		requireCode(t, err, codes.NotFound, "GetSchedulerInitStatus")
		_, err = stateClient.GetTask(ctx, &statev1.GetTaskRequest{TaskId: e.taskID.String()})
		requireCode(t, err, codes.NotFound, "GetTask")
		_, err = stateClient.GetTaskByScheduleAndNode(ctx, &statev1.GetTaskByScheduleAndNodeRequest{
			ScheduleId: runID, ServiceName: service, SchemaName: schema, TableName: table,
		})
		requireCode(t, err, codes.NotFound, "GetTaskByScheduleAndNode")

		tasks, err := stateClient.ListTasks(ctx, &statev1.ListTasksRequest{ScheduleId: runID})
		require.NoError(t, err)
		assert.Empty(t, tasks.Tasks)
		assert.Zero(t, tasks.TotalCount)
		attempts, err := stateClient.ListTaskAttempts(ctx, &statev1.ListTaskAttemptsRequest{RunId: runID})
		require.NoError(t, err)
		assert.Empty(t, attempts.Attempts)
		graph, err := stateClient.GetRunGraph(ctx, &statev1.GetRunGraphRequest{RunId: runID})
		require.NoError(t, err)
		assert.Empty(t, graph.Nodes, "another tenant's run has no graph")

		drifts, err := stateClient.ListActiveRunDrifts(ctx, &statev1.ListActiveRunDriftsRequest{})
		require.NoError(t, err)
		for _, d := range drifts.ActiveRuns {
			assert.NotEqual(t, runID, d.RunId, "another tenant's active run drifts for no request")
		}
		nodeRuns, err := stateClient.ListNodeRuns(ctx, &statev1.ListNodeRunsRequest{ServiceName: service, SchemaName: schema, TableName: table})
		require.NoError(t, err)
		assert.Empty(t, nodeRuns.Runs)
		nodes, err := stateClient.ListNodes(ctx, &statev1.ListNodesRequest{Search: table})
		require.NoError(t, err)
		assert.Empty(t, nodes.Nodes)
	})

	t.Run("commands", func(t *testing.T) {
		_, err := stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{ScheduleName: e.schedule})
		requireCode(t, err, codes.NotFound, "TriggerSchedule of another tenant's schedule")
		_, err = stateClient.CancelSchedule(ctx, &statev1.CancelScheduleRequest{ScheduleName: e.schedule})
		requireCode(t, err, codes.FailedPrecondition, "CancelSchedule finds no active run of the request's tenant")
		_, err = stateClient.CancelScheduler(ctx, &statev1.CancelSchedulerRequest{ScheduleId: runID, CancelledBy: "alice"})
		requireCode(t, err, codes.NotFound, "CancelScheduler")
		_, err = stateClient.TriggerRerun(ctx, &statev1.TriggerRerunRequest{SourceRunId: runID})
		requireCode(t, err, codes.NotFound, "TriggerRerun")
		_, err = stateClient.PreviewRebase(ctx, &statev1.PreviewRebaseRequest{SourceRunId: runID})
		requireCode(t, err, codes.NotFound, "PreviewRebase")
		_, err = stateClient.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{SourceRunId: runID})
		requireCode(t, err, codes.NotFound, "TriggerRebase")
		_, err = stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
			ServiceName: service, SchemaName: schema, TableName: table, MetadataSource: "snapshot_of_run", SourceRunId: runID,
		})
		requireCode(t, err, codes.NotFound, "TriggerSingleNodeRun of another tenant's run")
		_, err = stateClient.TriggerSingleNodeRun(ctx, &statev1.TriggerSingleNodeRunRequest{
			ServiceName: service, SchemaName: schema, TableName: table, MetadataSource: "latest",
		})
		requireCode(t, err, codes.NotFound, "TriggerSingleNodeRun of a node only another tenant's release holds")

		assert.Equal(t, string(run.RunStatusRunning), e.runStatus(t, stateDB), "the run is left as it was")
		assert.Equal(t, string(run.AttemptDispatched), e.attemptStatus(t, stateDB))
		var runs int
		require.NoError(t, stateDB.Get(&runs, `SELECT count(*) FROM runs WHERE schedule_name = $1`, e.schedule))
		assert.Equal(t, 1, runs, "no command created a run of the schedule in any tenant")
	})

	t.Run("reports", func(t *testing.T) {
		h := newBuiltHarness(stateDB)
		binding := redis.NewTaskStatusReportBinding(h.factory,
			svchandlers.NewAttemptReportHandler(defaultPlanner.Tasks, noRunMetrics{}, h.logger), h.logger)

		err := binding(ctx, e.report(t, pkgevents.DefaultTenantID))
		require.ErrorIs(t, err, pkgevents.ErrPermanent, "a report naming another tenant than its run's is dead-lettered")
		assert.Equal(t, string(run.AttemptDispatched), e.attemptStatus(t, stateDB), "the report changed nothing")
		var dedup int
		require.NoError(t, stateDB.Get(&dedup, `SELECT count(*) FROM message_processing WHERE stream_name = $1 AND message_id = $2`,
			streams.TaskStatusUpdatedV2, e.messages[0]))
		assert.Zero(t, dedup, "the refused report's transaction rolled back")

		require.NoError(t, binding(ctx, e.report(t, e.id)))
		assert.Equal(t, string(run.AttemptQueued), e.attemptStatus(t, stateDB), "the report under its run's tenant is applied")
	})
}
