package test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
)

// RunService serves the reads of a run a trigger created, from the same rows
// StateService reads, and treats another tenant's run as one that does not
// exist.
func TestRunService_ServesTheReadsOfATriggeredRun(t *testing.T) {
	ctx := context.Background()
	const schedule = "run-service-reads"
	seedActiveSchedule(t, schedule)
	promoteSchedules(t, stateDB, schedule)
	trig, err := stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{ScheduleName: schedule})
	require.NoError(t, err)
	runID := trig.ScheduleId

	got, err := runClient.GetRun(ctx, &runv1.GetRunRequest{RunId: runID})
	require.NoError(t, err)
	assert.Equal(t, runID, got.GetRun().GetRunId())
	assert.Equal(t, schedule, got.GetRun().GetScheduleName())
	assert.Equal(t, "trigger", got.GetRun().GetKind())
	assert.Equal(t, "running", got.GetRun().GetStatus())
	assert.NotEmpty(t, got.GetRun().GetReleaseId())
	assert.NotNil(t, got.GetRun().GetLastProgressAt())

	tasks, err := runClient.ListTasks(ctx, &runv1.ListTasksRequest{RunId: runID})
	require.NoError(t, err)
	require.Equal(t, int32(1), tasks.GetTotalCount())
	task := tasks.GetTasks()[0]
	assert.Equal(t, runID, task.GetRunId())
	assert.Equal(t, "dispatched", task.GetStatus())

	attempts, err := runClient.ListRunAttempts(ctx, &runv1.ListRunAttemptsRequest{RunId: runID, TaskId: task.GetTaskId()})
	require.NoError(t, err)
	require.Len(t, attempts.GetAttempts(), 1)
	assert.Equal(t, "dispatched", attempts.GetAttempts()[0].GetStatus())

	graph, err := runClient.GetRunGraph(ctx, &runv1.GetRunGraphRequest{RunId: runID})
	require.NoError(t, err)
	require.Len(t, graph.GetNodes(), 1)
	assert.Equal(t, task.GetTaskId(), graph.GetNodes()[0].GetTaskId())
	assert.Equal(t, schedule, graph.GetNodes()[0].GetScheduleName())
	assert.NotZero(t, graph.GetRunPromotionSeq())

	nodeTasks, err := runClient.ListNodeTasks(ctx, &runv1.ListNodeTasksRequest{
		ServiceName: task.GetServiceName(), SchemaName: task.GetSchemaName(), TableName: task.GetTableName(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, nodeTasks.GetTasks())
	assert.Equal(t, runID, nodeTasks.GetTasks()[0].GetRunId())
	assert.Equal(t, "running", nodeTasks.GetTasks()[0].GetRunStatus())

	nodes, err := runClient.ListNodes(ctx, &runv1.ListNodesRequest{Search: task.GetTableName()})
	require.NoError(t, err)
	require.NotEmpty(t, nodes.GetNodes())
	assert.GreaterOrEqual(t, nodes.GetNodes()[0].GetTaskCount(), int32(1))

	names, err := runClient.ListNodeNames(ctx, &runv1.ListNodeNamesRequest{ServiceName: task.GetServiceName()})
	require.NoError(t, err)
	assert.Contains(t, names.GetTableNames(), task.GetTableName())
	services, err := runClient.ListNodeServices(ctx, &runv1.ListNodeServicesRequest{})
	require.NoError(t, err)
	assert.Contains(t, services.GetServiceNames(), task.GetServiceName())

	drifts, err := runClient.ListActiveRunDrifts(ctx, &runv1.ListActiveRunDriftsRequest{})
	require.NoError(t, err)
	var driftRuns []string
	for _, d := range drifts.GetActiveRuns() {
		driftRuns = append(driftRuns, d.GetRunId())
	}
	assert.Contains(t, driftRuns, runID)

	schedules, err := runClient.ListSchedules(ctx, &runv1.ListSchedulesRequest{})
	require.NoError(t, err)
	var summary *runv1.ScheduleSummary
	for _, s := range schedules.GetSchedules() {
		if s.GetScheduleName() == schedule {
			summary = s
		}
	}
	require.NotNil(t, summary)
	assert.True(t, summary.GetHasActiveRun())
	assert.Equal(t, runID, summary.GetLastRunId())

	_, err = stateClient.CancelScheduler(ctx, &statev1.CancelSchedulerRequest{ScheduleId: runID, CancelledBy: "test"})
	require.NoError(t, err)
	listed, err := runClient.ListRuns(ctx, &runv1.ListRunsRequest{ScheduleName: schedule})
	require.NoError(t, err)
	assert.Zero(t, listed.GetTotalCount(), "a cancelling run is not completed yet")
}

// A run of another tenant is a run that does not exist: GetRun is NotFound and
// the run's lists are empty.
func TestRunService_AnotherTenantsRunDoesNotExist(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	_, err := stateDB.ExecContext(ctx, `
		INSERT INTO runs (tenant_id, run_id, schedule_name, status) VALUES ('acme', $1, $2, 'running')`,
		runID, "acme-"+runID.String()[:8])
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = stateDB.Exec(`DELETE FROM runs WHERE tenant_id = 'acme' AND run_id = $1`, runID) })

	_, err = runClient.GetRun(ctx, &runv1.GetRunRequest{RunId: runID.String()})
	assert.Equal(t, codes.NotFound, status.Code(err))
	tasks, err := runClient.ListTasks(ctx, &runv1.ListTasksRequest{RunId: runID.String()})
	require.NoError(t, err)
	assert.Zero(t, tasks.GetTotalCount())
	graph, err := runClient.GetRunGraph(ctx, &runv1.GetRunGraphRequest{RunId: runID.String()})
	require.NoError(t, err)
	assert.Empty(t, graph.GetNodes())
}
