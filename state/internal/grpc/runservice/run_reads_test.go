package runservice_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/projection"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/carolsimone/continuo/state/service/schedules"
)

var base = time.Date(2026, 10, 27, 8, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

func TestGetRun_MapsEveryField(t *testing.T) {
	store := newStore()
	id := uuid.New()
	store.putRun(tenantA, projection.RunDetail{
		RunID: id, ScheduleName: "daily", Kind: "rebase", Operation: "test", Status: run.RunStatusCancelling,
		FailureReason: "stalled", ReleaseID: "rel-9", PromotionSeq: 9, InitiatedBy: "ada",
		CreatedAt: base, StartedAt: ptr(base.Add(time.Minute)), CompletedAt: ptr(base.Add(2 * time.Minute)),
		LastProgressAt: ptr(base.Add(3 * time.Minute)), CancelledAt: ptr(base.Add(4 * time.Minute)),
		CancelledBy: "grace", CancellationReason: "wrong release",
	})

	resp, err := newServer(store).GetRun(ctxOf(tenantA), &runv1.GetRunRequest{RunId: id.String()})

	require.NoError(t, err)
	got := resp.GetRun()
	assert.Equal(t, id.String(), got.GetRunId())
	assert.Equal(t, "daily", got.GetScheduleName())
	assert.Equal(t, "rebase", got.GetKind())
	assert.Equal(t, "test", got.GetOperation())
	assert.Equal(t, "cancelling", got.GetStatus())
	assert.Equal(t, "stalled", got.GetFailureReason())
	assert.Equal(t, "rel-9", got.GetReleaseId())
	assert.Equal(t, int64(9), got.GetPromotionSeq())
	assert.Equal(t, "ada", got.GetInitiatedBy())
	assert.True(t, timestamppb.New(base).AsTime().Equal(got.GetCreatedAt().AsTime()))
	assert.True(t, base.Add(time.Minute).Equal(got.GetStartedAt().AsTime()))
	assert.True(t, base.Add(2*time.Minute).Equal(got.GetCompletedAt().AsTime()))
	assert.True(t, base.Add(3*time.Minute).Equal(got.GetLastProgressAt().AsTime()))
	assert.True(t, base.Add(4*time.Minute).Equal(got.GetCancelledAt().AsTime()))
	assert.Equal(t, "grace", got.GetCancelledBy())
	assert.Equal(t, "wrong release", got.GetCancellationReason())
}

func TestGetRun_LeavesUnreachedStepsUnset(t *testing.T) {
	store := newStore()
	id := uuid.New()
	store.putRun(tenantA, projection.RunDetail{RunID: id, Status: run.RunStatusPending, CreatedAt: base})

	resp, err := newServer(store).GetRun(ctxOf(tenantA), &runv1.GetRunRequest{RunId: id.String()})

	require.NoError(t, err)
	assert.Nil(t, resp.GetRun().GetStartedAt())
	assert.Nil(t, resp.GetRun().GetCompletedAt())
	assert.Nil(t, resp.GetRun().GetLastProgressAt())
	assert.Nil(t, resp.GetRun().GetCancelledAt())
}

func TestListTasks_MapsTheWaitFields(t *testing.T) {
	store := newStore()
	runID, holder, waiting := uuid.New(), uuid.New(), uuid.New()
	store.putTasks(tenantA, runID,
		projection.TaskDetail{
			TaskID: waiting, RunID: runID, CreatedAt: base, ServiceName: "svc", SchemaName: "sch", TableName: "orders",
			JobName: "svc-sch-orders", Status: run.TaskStatusPending, Attempt: 2, MaxRetries: 3,
			WaitReason: run.WaitReasonNodeLease, WaitingSince: ptr(base.Add(time.Minute)), WaitingOnRunID: &holder,
			CancelledAt: ptr(base), CancelledBy: "stale-stamp",
		},
		projection.TaskDetail{
			TaskID: uuid.New(), RunID: runID, CreatedAt: base, Status: run.TaskStatusCancelled,
			CancelledAt: ptr(base.Add(time.Hour)), CancelledBy: "ada",
		},
	)

	resp, err := newServer(store).ListTasks(ctxOf(tenantA), &runv1.ListTasksRequest{RunId: runID.String()})

	require.NoError(t, err)
	require.Len(t, resp.GetTasks(), 2)
	assert.Equal(t, int32(2), resp.GetTotalCount())
	w := resp.GetTasks()[0]
	assert.Equal(t, waiting.String(), w.GetTaskId())
	assert.Equal(t, runID.String(), w.GetRunId())
	assert.Equal(t, "svc", w.GetServiceName())
	assert.Equal(t, "sch", w.GetSchemaName())
	assert.Equal(t, "orders", w.GetTableName())
	assert.Equal(t, "svc-sch-orders", w.GetJobName())
	assert.Equal(t, "pending", w.GetStatus())
	assert.Equal(t, int32(2), w.GetAttempt())
	assert.Equal(t, int32(3), w.GetMaxRetries())
	assert.Equal(t, "node_lease", w.GetWaitReason())
	assert.Equal(t, holder.String(), w.GetWaitingOnRunId())
	assert.True(t, base.Add(time.Minute).Equal(w.GetWaitingSince().AsTime()))
	// The cancel stamps of a task that is not cancelled are not shown.
	assert.Nil(t, w.GetCancelledAt())
	assert.Empty(t, w.GetCancelledBy())
	c := resp.GetTasks()[1]
	assert.Equal(t, "cancelled", c.GetStatus())
	assert.Equal(t, "ada", c.GetCancelledBy())
	assert.True(t, base.Add(time.Hour).Equal(c.GetCancelledAt().AsTime()))
	assert.Empty(t, c.GetWaitReason())
	assert.Empty(t, c.GetWaitingOnRunId())
	assert.Nil(t, c.GetWaitingSince())
}

func TestListTasks_FiltersAndPages(t *testing.T) {
	store := newStore()
	runID := uuid.New()
	var tasks []projection.TaskDetail
	for range 60 {
		tasks = append(tasks, projection.TaskDetail{TaskID: uuid.New(), RunID: runID, Status: run.TaskStatusSucceeded})
	}
	tasks = append(tasks, projection.TaskDetail{TaskID: uuid.New(), RunID: runID, Status: run.TaskStatusFailed})
	store.putTasks(tenantA, runID, tasks...)
	srv := newServer(store)

	def, err := srv.ListTasks(ctxOf(tenantA), &runv1.ListTasksRequest{RunId: runID.String()})
	require.NoError(t, err)
	assert.Len(t, def.GetTasks(), 50, "a page_size of 0 means 50")
	assert.Equal(t, int32(61), def.GetTotalCount())

	big, err := srv.ListTasks(ctxOf(tenantA), &runv1.ListTasksRequest{RunId: runID.String(), PageSize: 500, PageOffset: -3})
	require.NoError(t, err)
	assert.Len(t, big.GetTasks(), 61, "there is no upper bound on page_size")

	failed, err := srv.ListTasks(ctxOf(tenantA), &runv1.ListTasksRequest{RunId: runID.String(), Status: "failed"})
	require.NoError(t, err)
	assert.Len(t, failed.GetTasks(), 1)
	assert.Equal(t, int32(1), failed.GetTotalCount())
}

func TestListRunAttempts_MapsEveryField(t *testing.T) {
	store := newStore()
	runID, taskID := uuid.New(), uuid.New()
	store.putAttempts(tenantA, runID,
		projection.TaskAttempt{
			TaskID: taskID, Attempt: 1, JobName: "job-1", Status: "failed",
			FailureClass: ptr("task"), FailureReason: ptr("exit_nonzero"), ExitCode: ptr(137),
			DispatchedAt: base, QueuedAt: ptr(base.Add(time.Second)), StartedAt: ptr(base.Add(2 * time.Second)),
			FinishedAt: ptr(base.Add(3 * time.Second)), ExecutionSeconds: ptr(1.5),
			ErrorMessage: ptr("oom"), LogS3Key: ptr("logs/1"), RunResultsURI: ptr("results/1"),
		},
		projection.TaskAttempt{TaskID: uuid.New(), Attempt: 0, JobName: "other", Status: "dispatched", DispatchedAt: base},
	)

	resp, err := newServer(store).ListRunAttempts(ctxOf(tenantA), &runv1.ListRunAttemptsRequest{RunId: runID.String(), TaskId: taskID.String()})

	require.NoError(t, err)
	require.Len(t, resp.GetAttempts(), 1)
	assert.Equal(t, int32(1), resp.GetTotalCount())
	a := resp.GetAttempts()[0]
	assert.Equal(t, taskID.String(), a.GetTaskId())
	assert.Equal(t, int32(1), a.GetAttempt())
	assert.Equal(t, "job-1", a.GetJobName())
	assert.Equal(t, "failed", a.GetStatus())
	assert.Equal(t, "task", a.GetFailureClass())
	assert.Equal(t, "exit_nonzero", a.GetFailureReason())
	assert.Equal(t, int32(137), a.GetExitCode())
	assert.True(t, base.Equal(a.GetDispatchedAt().AsTime()))
	assert.True(t, base.Add(time.Second).Equal(a.GetQueuedAt().AsTime()))
	assert.True(t, base.Add(2*time.Second).Equal(a.GetStartedAt().AsTime()))
	assert.True(t, base.Add(3*time.Second).Equal(a.GetFinishedAt().AsTime()))
	assert.InDelta(t, 1.5, a.GetExecutionSeconds(), 1e-9)
	assert.Equal(t, "oom", a.GetErrorMessage())
	assert.Equal(t, "logs/1", a.GetLogS3Key())
	assert.Equal(t, "results/1", a.GetRunResultsUri())
}

func TestListNodeTasks_MapsRunStatus(t *testing.T) {
	store := newStore()
	runID, taskID, holder := uuid.New(), uuid.New(), uuid.New()
	store.nodeTasks = []*projection.NodeRun{{
		ScheduleID: runID, ScheduleName: "daily", Kind: "cron", TerminalStatus: "failed",
		TaskID: taskID, TaskStatus: run.TaskStatusFailed, Attempt: 1, ImageTag: "v1",
		CreatedAt: base, StartedAt: ptr(base.Add(time.Minute)), CompletedAt: ptr(base.Add(2 * time.Minute)),
		ErrorMessage: ptr("bad"), LogS3Key: ptr("logs"), RunResultsURI: ptr("res"),
		FailureClass: ptr("task"), FailureReason: ptr("exit_nonzero"), Operation: "run", ContentHash: "abc",
		WaitReason: run.WaitReasonNodeLease, WaitingSince: ptr(base.Add(time.Second)), WaitingOnRunID: &holder,
	}}

	resp, err := newServer(store).ListNodeTasks(ctxOf(tenantA), &runv1.ListNodeTasksRequest{
		ServiceName: "svc", SchemaName: "sch", TableName: "orders", Limit: 7, Operation: "test",
	})

	require.NoError(t, err)
	require.Len(t, resp.GetTasks(), 1)
	n := resp.GetTasks()[0]
	assert.Equal(t, runID.String(), n.GetRunId())
	assert.Equal(t, "daily", n.GetScheduleName())
	assert.Equal(t, "cron", n.GetKind())
	assert.Equal(t, "failed", n.GetRunStatus())
	assert.Equal(t, taskID.String(), n.GetTaskId())
	assert.Equal(t, "failed", n.GetTaskStatus())
	assert.Equal(t, int32(1), n.GetAttempt())
	assert.Equal(t, "v1", n.GetImageTag())
	assert.Equal(t, "2026-10-27T08:00:00Z", n.GetCreatedAt())
	assert.Equal(t, "2026-10-27T08:01:00Z", n.GetStartedAt())
	assert.Equal(t, "2026-10-27T08:02:00Z", n.GetCompletedAt())
	assert.Equal(t, "bad", n.GetErrorMessage())
	assert.Equal(t, "logs", n.GetLogS3Key())
	assert.Equal(t, "run", n.GetOperation())
	assert.Equal(t, "res", n.GetRunResultsUri())
	assert.Equal(t, "abc", n.GetContentHash())
	assert.Equal(t, "task", n.GetFailureClass())
	assert.Equal(t, "exit_nonzero", n.GetFailureReason())
	assert.Equal(t, "node_lease", n.GetWaitReason())
	assert.Equal(t, holder.String(), n.GetWaitingOnRunId())
	assert.Equal(t, "2026-10-27T08:00:01Z", n.GetWaitingSince())
}

// A node read refuses an operation the operation vocabulary does not declare.
func TestNodeReads_RefuseAnUnknownOperation(t *testing.T) {
	srv := newServer(newStore())
	ctx := ctxOf(tenantA)
	for _, op := range []string{"tset", "RUN", "full-refresh"} {
		_, err := srv.ListNodeTasks(ctx, &runv1.ListNodeTasksRequest{ServiceName: "svc", SchemaName: "sch", TableName: "orders", Operation: op})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "ListNodeTasks with operation %q", op)
		_, err = srv.ListNodes(ctx, &runv1.ListNodesRequest{Operation: op})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "ListNodes with operation %q", op)
	}
}

// A node read hands the reader the operation its request names: every value
// of the operation vocabulary, with the empty default and "run" both read as
// model.OperationRun.
func TestNodeReads_ParseTheOperation(t *testing.T) {
	store := newStore()
	srv := newServer(store)
	ctx := ctxOf(tenantA)
	for wire, want := range map[string]model.Operation{
		"":             model.OperationRun,
		"run":          model.OperationRun,
		"test":         model.OperationTest,
		"build":        model.OperationBuild,
		"full_refresh": model.OperationFullRefresh,
	} {
		_, err := srv.ListNodeTasks(ctx, &runv1.ListNodeTasksRequest{ServiceName: "svc", SchemaName: "sch", TableName: "orders", Operation: wire})
		require.NoError(t, err, "ListNodeTasks with operation %q", wire)
		assert.Equal(t, want, store.nodeTasksOperation, "ListNodeTasks with operation %q", wire)
		_, err = srv.ListNodes(ctx, &runv1.ListNodesRequest{Operation: wire})
		require.NoError(t, err, "ListNodes with operation %q", wire)
		assert.Equal(t, want, store.nodesOperation, "ListNodes with operation %q", wire)
	}
}

func TestListNodes_MapsTaskCount(t *testing.T) {
	store := newStore()
	store.nodes = []*projection.NodeSummary{
		{
			ServiceName: "svc", SchemaName: "sch", TableName: "orders", RunCount: 12, SuccessRatePct: 90,
			AvgDurationSec: 30, P95DurationSec: 80, FlakyRatePct: 8, LastStatus: "succeeded",
			LastRunAt: base, Operation: "run", TimeoutSeconds: ptr(600),
		},
		{ServiceName: "svc", SchemaName: "sch", TableName: "users", RunCount: 1, SuccessRatePct: -1, AvgDurationSec: -1, P95DurationSec: -1, LastRunAt: base},
	}

	resp, err := newServer(store).ListNodes(ctxOf(tenantA), &runv1.ListNodesRequest{Search: "ord", ServiceName: "svc"})

	require.NoError(t, err)
	assert.Equal(t, int32(2), resp.GetTotalCount())
	require.Len(t, resp.GetNodes(), 2)
	n := resp.GetNodes()[0]
	assert.Equal(t, "svc", n.GetServiceName())
	assert.Equal(t, "sch", n.GetSchemaName())
	assert.Equal(t, "orders", n.GetTableName())
	assert.Equal(t, int32(12), n.GetTaskCount())
	assert.Equal(t, int32(90), n.GetSuccessRatePct())
	assert.Equal(t, int32(30), n.GetAvgDurationSec())
	assert.Equal(t, int32(80), n.GetP95DurationSec())
	assert.Equal(t, int32(8), n.GetFlakyRatePct())
	assert.Equal(t, "succeeded", n.GetLastStatus())
	assert.Equal(t, "2026-10-27T08:00:00Z", n.GetLastRunAt())
	assert.Equal(t, "run", n.GetOperation())
	assert.Equal(t, int32(600), n.GetTimeoutSeconds())
	assert.True(t, n.GetTimeoutDeclared())
	u := resp.GetNodes()[1]
	assert.Equal(t, int32(-1), u.GetSuccessRatePct())
	assert.Equal(t, int32(1800), u.GetTimeoutSeconds(), "a node declaring no timeout gets the install default")
	assert.False(t, u.GetTimeoutDeclared())
	assert.Equal(t, "ord", store.nodesSearch)
	assert.Equal(t, "svc", store.nodesService)
}

func TestListNodes_ClampsPaging(t *testing.T) {
	cases := []struct {
		name                  string
		limit, offset         int32
		wantLimit, wantOffset int
	}{
		{"defaults", 0, 0, 50, 0},
		{"negative", -4, -9, 50, 0},
		{"above the cap", 5000, 7, 200, 7},
		{"in range", 25, 100, 25, 100},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := newStore()
			_, err := newServer(store).ListNodes(ctxOf(tenantA), &runv1.ListNodesRequest{Limit: c.limit, Offset: c.offset})
			require.NoError(t, err)
			assert.Equal(t, c.wantLimit, store.nodesLimit)
			assert.Equal(t, c.wantOffset, store.nodesOffset)
		})
	}
}

func TestListNodeNames_PassesTheServiceFilter(t *testing.T) {
	store := newStore()
	store.nodeNames = []string{"orders", "users"}

	resp, err := newServer(store).ListNodeNames(ctxOf(tenantA), &runv1.ListNodeNamesRequest{ServiceName: "svc"})

	require.NoError(t, err)
	assert.Equal(t, []string{"orders", "users"}, resp.GetTableNames())
	assert.Equal(t, "svc", store.nodeNamesService)
}

func TestListNodeServices_ListsDistinctServices(t *testing.T) {
	store := newStore()
	store.nodeServices = []string{"a", "b"}

	resp, err := newServer(store).ListNodeServices(ctxOf(tenantA), &runv1.ListNodeServicesRequest{})

	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, resp.GetServiceNames())
}

func TestGetRunGraph_MapsNodesAndEdges(t *testing.T) {
	store := newStore()
	runID, taskID, holder := uuid.New(), uuid.New(), uuid.New()
	store.putGraph(tenantA, runID, projection.RunGraph{
		Nodes: []projection.RunGraphNode{{
			ServiceName: "svc", SchemaName: "sch", TableName: "orders", ScheduleName: "daily", NodeType: "model",
			Status: run.TaskStatusPending, TaskID: taskID, WaitReason: run.WaitReasonNodeLease,
			WaitingSince: ptr(base), WaitingOnRunID: &holder,
		}},
		Edges:           []projection.RunGraphEdge{{FromNodeID: "svc.sch.orders", ToNodeID: "svc.sch.users"}},
		RunPromotionSeq: 4, LatestPromotionSeq: 7,
	})

	resp, err := newServer(store).GetRunGraph(ctxOf(tenantA), &runv1.GetRunGraphRequest{RunId: runID.String()})

	require.NoError(t, err)
	assert.Equal(t, int64(4), resp.GetRunPromotionSeq())
	assert.Equal(t, int64(7), resp.GetLatestPromotionSeq())
	require.Len(t, resp.GetNodes(), 1)
	n := resp.GetNodes()[0]
	assert.Equal(t, "svc", n.GetServiceName())
	assert.Equal(t, "sch", n.GetSchemaName())
	assert.Equal(t, "orders", n.GetTableName())
	assert.Equal(t, "daily", n.GetScheduleName())
	assert.Equal(t, "model", n.GetNodeType())
	assert.Equal(t, "pending", n.GetStatus())
	assert.Equal(t, taskID.String(), n.GetTaskId())
	assert.Equal(t, "node_lease", n.GetWaitReason())
	assert.Equal(t, holder.String(), n.GetWaitingOnRunId())
	assert.Equal(t, "2026-10-27T08:00:00Z", n.GetWaitingSince())
	require.Len(t, resp.GetEdges(), 1)
	assert.Equal(t, "svc.sch.orders", resp.GetEdges()[0].GetFromNodeId())
	assert.Equal(t, "svc.sch.users", resp.GetEdges()[0].GetToNodeId())
}

func TestListRuns_ClampsPaging(t *testing.T) {
	cases := []struct {
		name                  string
		size, offset          int32
		wantLimit, wantOffset int
	}{
		{"defaults", 0, 0, 50, 0},
		{"negative", -1, -5, 50, 0},
		{"above the cap", 900, 3, 200, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := newStore()
			_, err := newServer(store).ListRuns(ctxOf(tenantA), &runv1.ListRunsRequest{ScheduleName: "daily", PageSize: c.size, PageOffset: c.offset})
			require.NoError(t, err)
			assert.Equal(t, c.wantLimit, store.runsLimit)
			assert.Equal(t, c.wantOffset, store.runsOffset)
		})
	}
}

func TestListRuns_MapsEveryField(t *testing.T) {
	store := newStore()
	id := uuid.New()
	store.completed[tenantA] = []projection.RunSummary{{
		RunID: id, ScheduleName: "daily", TerminalStatus: run.RunStatusFailed, FailureReason: "stalled",
		CreatedAt: base, CompletedAt: base.Add(time.Hour),
	}}

	resp, err := newServer(store).ListRuns(ctxOf(tenantA), &runv1.ListRunsRequest{ScheduleName: "daily"})

	require.NoError(t, err)
	assert.Equal(t, int32(1), resp.GetTotalCount())
	require.Len(t, resp.GetRuns(), 1)
	r := resp.GetRuns()[0]
	assert.Equal(t, id.String(), r.GetRunId())
	assert.Equal(t, "daily", r.GetScheduleName())
	assert.Equal(t, "failed", r.GetStatus())
	assert.Equal(t, "stalled", r.GetFailureReason())
	assert.Equal(t, "2026-10-27T08:00:00Z", r.GetCreatedAt())
	assert.Equal(t, "2026-10-27T09:00:00Z", r.GetCompletedAt())
}

func TestListActiveRunDrifts_MapsEveryRun(t *testing.T) {
	store := newStore()
	a, b := uuid.New(), uuid.New()
	store.latest[tenantA] = 9
	store.drifts[tenantA] = []projection.ActiveRunDrift{
		{ScheduleName: "daily", RunID: a, RunPromotionSeq: 7},
		{ScheduleName: "hourly", RunID: b, RunPromotionSeq: 0},
	}

	resp, err := newServer(store).ListActiveRunDrifts(ctxOf(tenantA), &runv1.ListActiveRunDriftsRequest{})

	require.NoError(t, err)
	assert.Equal(t, int64(9), resp.GetLatestPromotionSeq())
	require.Len(t, resp.GetActiveRuns(), 2)
	assert.Equal(t, "daily", resp.GetActiveRuns()[0].GetScheduleName())
	assert.Equal(t, a.String(), resp.GetActiveRuns()[0].GetRunId())
	assert.Equal(t, int64(7), resp.GetActiveRuns()[0].GetRunPromotionSeq())
	assert.Equal(t, b.String(), resp.GetActiveRuns()[1].GetRunId())
	assert.Equal(t, int64(0), resp.GetActiveRuns()[1].GetRunPromotionSeq())
}

// The errors the proto comments name.

func TestRunReads_RefuseAnEmptyOrMalformedID(t *testing.T) {
	srv := newServer(newStore())
	ctx := ctxOf(tenantA)
	for _, id := range []string{"", "not-a-uuid"} {
		calls := map[string]func() error{
			"GetRun": func() error { _, err := srv.GetRun(ctx, &runv1.GetRunRequest{RunId: id}); return err },
			"ListTasks": func() error {
				_, err := srv.ListTasks(ctx, &runv1.ListTasksRequest{RunId: id})
				return err
			},
			"ListRunAttempts": func() error {
				_, err := srv.ListRunAttempts(ctx, &runv1.ListRunAttemptsRequest{RunId: id})
				return err
			},
			"GetRunGraph": func() error {
				_, err := srv.GetRunGraph(ctx, &runv1.GetRunGraphRequest{RunId: id})
				return err
			},
		}
		for name, call := range calls {
			assert.Equal(t, codes.InvalidArgument, status.Code(call()), "%s with run_id %q", name, id)
		}
	}
}

func TestRunReads_RefuseOtherMalformedRequests(t *testing.T) {
	srv := newServer(newStore())
	ctx := ctxOf(tenantA)
	runID := uuid.NewString()

	_, err := srv.ListTasks(ctx, &runv1.ListTasksRequest{RunId: runID, Status: "exploded"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "an unknown task status")
	_, err = srv.ListRunAttempts(ctx, &runv1.ListRunAttemptsRequest{RunId: runID, TaskId: "nope"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "a malformed task_id")
	_, err = srv.ListRuns(ctx, &runv1.ListRunsRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err), "an empty schedule_name")
	for _, req := range []*runv1.ListNodeTasksRequest{
		{SchemaName: "s", TableName: "t"}, {ServiceName: "v", TableName: "t"}, {ServiceName: "v", SchemaName: "s"},
	} {
		_, err = srv.ListNodeTasks(ctx, req)
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "an incomplete node identity %v", req)
	}
}

func TestRunReads_AnotherTenantsRunIsNotFound(t *testing.T) {
	store := newStore()
	id := uuid.New()
	store.putRun(tenantB, projection.RunDetail{RunID: id, Status: run.RunStatusRunning, CreatedAt: base})
	store.putTasks(tenantB, id, projection.TaskDetail{TaskID: uuid.New(), RunID: id, Status: run.TaskStatusRunning})
	store.putAttempts(tenantB, id, projection.TaskAttempt{TaskID: uuid.New(), DispatchedAt: base})
	store.putGraph(tenantB, id, projection.RunGraph{
		Nodes: []projection.RunGraphNode{{TableName: "orders", TaskID: uuid.New()}}, RunPromotionSeq: 5,
	})
	store.latest[tenantA] = 8
	srv := newServer(store)
	ctx := ctxOf(tenantA)

	_, err := srv.GetRun(ctx, &runv1.GetRunRequest{RunId: id.String()})
	assert.Equal(t, codes.NotFound, status.Code(err), "GetRun of an unknown or other tenant's run")
	_, err = srv.GetRun(ctx, &runv1.GetRunRequest{RunId: uuid.NewString()})
	assert.Equal(t, codes.NotFound, status.Code(err))

	tasks, err := srv.ListTasks(ctx, &runv1.ListTasksRequest{RunId: id.String()})
	require.NoError(t, err)
	assert.Empty(t, tasks.GetTasks())
	assert.Zero(t, tasks.GetTotalCount())

	attempts, err := srv.ListRunAttempts(ctx, &runv1.ListRunAttemptsRequest{RunId: id.String()})
	require.NoError(t, err)
	assert.Empty(t, attempts.GetAttempts())
	assert.Zero(t, attempts.GetTotalCount())

	graph, err := srv.GetRunGraph(ctx, &runv1.GetRunGraphRequest{RunId: id.String()})
	require.NoError(t, err)
	assert.Empty(t, graph.GetNodes())
	assert.Empty(t, graph.GetEdges())
	assert.Zero(t, graph.GetRunPromotionSeq())
	assert.Equal(t, int64(8), graph.GetLatestPromotionSeq())
}

func TestRunReads_ARequestWithoutATenantIsInternal(t *testing.T) {
	srv := newServer(newStore())
	ctx := context.Background()
	id := uuid.NewString()
	calls := map[string]func() error{
		"GetRun":          func() error { _, err := srv.GetRun(ctx, &runv1.GetRunRequest{RunId: id}); return err },
		"ListTasks":       func() error { _, err := srv.ListTasks(ctx, &runv1.ListTasksRequest{RunId: id}); return err },
		"ListRunAttempts": func() error { _, err := srv.ListRunAttempts(ctx, &runv1.ListRunAttemptsRequest{RunId: id}); return err },
		"ListNodeTasks": func() error {
			_, err := srv.ListNodeTasks(ctx, &runv1.ListNodeTasksRequest{ServiceName: "a", SchemaName: "b", TableName: "c"})
			return err
		},
		"ListNodes":           func() error { _, err := srv.ListNodes(ctx, &runv1.ListNodesRequest{}); return err },
		"ListNodeNames":       func() error { _, err := srv.ListNodeNames(ctx, &runv1.ListNodeNamesRequest{}); return err },
		"ListNodeServices":    func() error { _, err := srv.ListNodeServices(ctx, &runv1.ListNodeServicesRequest{}); return err },
		"GetRunGraph":         func() error { _, err := srv.GetRunGraph(ctx, &runv1.GetRunGraphRequest{RunId: id}); return err },
		"ListRuns":            func() error { _, err := srv.ListRuns(ctx, &runv1.ListRunsRequest{ScheduleName: "daily"}); return err },
		"ListActiveRunDrifts": func() error { _, err := srv.ListActiveRunDrifts(ctx, &runv1.ListActiveRunDriftsRequest{}); return err },
		"ListSchedules":       func() error { _, err := srv.ListSchedules(ctx, &runv1.ListSchedulesRequest{}); return err },
		"GetScheduleGraph": func() error {
			_, err := srv.GetScheduleGraph(ctx, &runv1.GetScheduleGraphRequest{ScheduleName: "daily"})
			return err
		},
		"ListScheduleTopologies": func() error {
			_, err := srv.ListScheduleTopologies(ctx, &runv1.ListScheduleTopologiesRequest{})
			return err
		},
	}
	for name, call := range calls {
		assert.Equal(t, codes.Internal, status.Code(call()), name)
	}
}

func TestRunReads_AFailingReadIsInternal(t *testing.T) {
	store := newStore()
	store.failWith = errBoom
	srv := newServer(store)
	ctx := ctxOf(tenantA)
	id := uuid.NewString()
	calls := map[string]func() error{
		"GetRun":          func() error { _, err := srv.GetRun(ctx, &runv1.GetRunRequest{RunId: id}); return err },
		"ListTasks":       func() error { _, err := srv.ListTasks(ctx, &runv1.ListTasksRequest{RunId: id}); return err },
		"ListRunAttempts": func() error { _, err := srv.ListRunAttempts(ctx, &runv1.ListRunAttemptsRequest{RunId: id}); return err },
		"ListNodeTasks": func() error {
			_, err := srv.ListNodeTasks(ctx, &runv1.ListNodeTasksRequest{ServiceName: "a", SchemaName: "b", TableName: "c"})
			return err
		},
		"ListNodes":           func() error { _, err := srv.ListNodes(ctx, &runv1.ListNodesRequest{}); return err },
		"ListNodeNames":       func() error { _, err := srv.ListNodeNames(ctx, &runv1.ListNodeNamesRequest{}); return err },
		"ListNodeServices":    func() error { _, err := srv.ListNodeServices(ctx, &runv1.ListNodeServicesRequest{}); return err },
		"GetRunGraph":         func() error { _, err := srv.GetRunGraph(ctx, &runv1.GetRunGraphRequest{RunId: id}); return err },
		"ListRuns":            func() error { _, err := srv.ListRuns(ctx, &runv1.ListRunsRequest{ScheduleName: "daily"}); return err },
		"ListActiveRunDrifts": func() error { _, err := srv.ListActiveRunDrifts(ctx, &runv1.ListActiveRunDriftsRequest{}); return err },
		"ListSchedules":       func() error { _, err := srv.ListSchedules(ctx, &runv1.ListSchedulesRequest{}); return err },
		"GetScheduleGraph": func() error {
			_, err := srv.GetScheduleGraph(ctx, &runv1.GetScheduleGraphRequest{ScheduleName: "daily"})
			return err
		},
		"ListScheduleTopologies": func() error {
			_, err := srv.ListScheduleTopologies(ctx, &runv1.ListScheduleTopologiesRequest{})
			return err
		},
	}
	for name, call := range calls {
		assert.Equal(t, codes.Internal, status.Code(call()), name)
	}
}

func TestListRunAttempts_ClampsPaging(t *testing.T) {
	cases := []struct {
		name                  string
		size, offset          int32
		wantLimit, wantOffset int
	}{
		{"defaults", 0, 0, 50, 0},
		{"negative", -2, -8, 50, 0},
		{"above the cap", 1000, 4, 200, 4},
		{"in range", 30, 60, 30, 60},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := newStore()
			_, err := newServer(store).ListRunAttempts(ctxOf(tenantA), &runv1.ListRunAttemptsRequest{
				RunId: uuid.NewString(), PageSize: c.size, PageOffset: c.offset,
			})
			require.NoError(t, err)
			assert.Equal(t, c.wantLimit, store.attemptLimit)
			assert.Equal(t, c.wantOffset, store.attemptOffset)
		})
	}
}

// The node and schedule reads read the tenant of the request: another tenant
// sees none of the rows the default tenant holds.
func TestRunReads_NodeAndScheduleReadsAreOfTheRequestsTenant(t *testing.T) {
	store := newStore()
	store.nodeTasks = []*projection.NodeRun{{ScheduleID: uuid.New(), TaskID: uuid.New(), CreatedAt: base}}
	store.nodes = []*projection.NodeSummary{{ServiceName: "svc", TableName: "orders", LastRunAt: base}}
	store.nodeNames = []string{"orders"}
	store.nodeServices = []string{"svc"}
	store.scheduleSummaries[tenantA] = []schedules.Summary{{Name: "daily", ModelCount: 1}}
	srv := newServer(store)
	node := &runv1.ListNodeTasksRequest{ServiceName: "svc", SchemaName: "sch", TableName: "orders"}

	// The default tenant sees the rows, so an empty answer below is the
	// tenant's doing.
	own, err := srv.ListNodeTasks(ctxOf(tenantA), node)
	require.NoError(t, err)
	require.Len(t, own.GetTasks(), 1)
	ownNodes, err := srv.ListNodes(ctxOf(tenantA), &runv1.ListNodesRequest{})
	require.NoError(t, err)
	require.Len(t, ownNodes.GetNodes(), 1)
	ownNames, err := srv.ListNodeNames(ctxOf(tenantA), &runv1.ListNodeNamesRequest{})
	require.NoError(t, err)
	require.Len(t, ownNames.GetTableNames(), 1)
	ownServices, err := srv.ListNodeServices(ctxOf(tenantA), &runv1.ListNodeServicesRequest{})
	require.NoError(t, err)
	require.Len(t, ownServices.GetServiceNames(), 1)
	ownSchedules, err := srv.ListSchedules(ctxOf(tenantA), &runv1.ListSchedulesRequest{})
	require.NoError(t, err)
	require.Len(t, ownSchedules.GetSchedules(), 1)

	ctx := ctxOf(tenantB)
	tasks, err := srv.ListNodeTasks(ctx, node)
	require.NoError(t, err)
	assert.Empty(t, tasks.GetTasks())
	nodes, err := srv.ListNodes(ctx, &runv1.ListNodesRequest{})
	require.NoError(t, err)
	assert.Empty(t, nodes.GetNodes())
	assert.Zero(t, nodes.GetTotalCount())
	names, err := srv.ListNodeNames(ctx, &runv1.ListNodeNamesRequest{})
	require.NoError(t, err)
	assert.Empty(t, names.GetTableNames())
	services, err := srv.ListNodeServices(ctx, &runv1.ListNodeServicesRequest{})
	require.NoError(t, err)
	assert.Empty(t, services.GetServiceNames())
	schedules, err := srv.ListSchedules(ctx, &runv1.ListSchedulesRequest{})
	require.NoError(t, err)
	assert.Empty(t, schedules.GetSchedules())
}
