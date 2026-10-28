package schedule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const statusRunID = "11111111-1111-4111-8111-111111111111"

// fakeStateStatus implements client.RunClient for status tests.
type fakeStateStatus struct {
	client.RunClient
	schedules    *runv1.ListSchedulesResponse
	schedulesErr error
	run          *runv1.Run
	runErr       error
	taskPages    []*runv1.ListTasksResponse
	taskErr      error
	attempts     []*runv1.TaskAttempt
	// attemptsTotal is the total_count the one page of attempts reports.
	attemptsTotal int32

	getRunIDs     []string
	listTasksCall int

	// Per-call deadline capture: each RPC records whether its context carried a
	// deadline, so tests can assert the documented per-call --timeout contract.
	deadlines map[string][]bool
}

func (f *fakeStateStatus) noteDeadline(rpc string, ctx context.Context) {
	if f.deadlines == nil {
		f.deadlines = map[string][]bool{}
	}
	_, has := ctx.Deadline()
	f.deadlines[rpc] = append(f.deadlines[rpc], has)
}

func (f *fakeStateStatus) ListAllSchedules(ctx context.Context) (*runv1.ListSchedulesResponse, error) {
	f.noteDeadline("ListSchedules", ctx)
	return f.schedules, f.schedulesErr
}

func (f *fakeStateStatus) GetRun(ctx context.Context, runID string) (*runv1.RunResponse, error) {
	f.noteDeadline("GetRun", ctx)
	f.getRunIDs = append(f.getRunIDs, runID)
	if f.runErr != nil {
		return nil, f.runErr
	}
	return &runv1.RunResponse{Run: f.run}, nil
}

func (f *fakeStateStatus) ListTasks(ctx context.Context, _, _ string, _, _ int32) (*runv1.ListTasksResponse, error) {
	f.noteDeadline("ListTasks", ctx)
	if f.taskErr != nil {
		return nil, f.taskErr
	}
	resp := f.taskPages[f.listTasksCall]
	f.listTasksCall++
	return resp, nil
}

func (f *fakeStateStatus) ListRunAttempts(ctx context.Context, _, _ string, _, offset int32) (*runv1.ListRunAttemptsResponse, error) {
	f.noteDeadline("ListRunAttempts", ctx)
	if offset > 0 {
		return &runv1.ListRunAttemptsResponse{TotalCount: f.attemptsTotal}, nil
	}
	return &runv1.ListRunAttemptsResponse{TotalCount: f.attemptsTotal, Attempts: f.attempts}, nil
}

func (f *fakeStateStatus) Close() error { return nil }

func runStatus(t *testing.T, fake client.RunClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewStatusCommand(
		func(_ context.Context, _ string) (client.RunClient, error) { return fake, nil },
		cfg, &outBuf, &errBuf,
	)
	cmd.SetArgs(args)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	exit = 0
	if err != nil {
		var cliErr output.CLIError
		if errors.As(err, &cliErr) {
			exit = cliErr.ExitCode()
		} else {
			exit = 1
		}
	}
	return outBuf.String(), errBuf.String(), exit
}

type statusTaskJSON struct {
	Node           string `json:"node"`
	Status         string `json:"status"`
	Attempt        int32  `json:"attempt"`
	FailureClass   string `json:"failure_class"`
	FailureReason  string `json:"failure_reason"`
	WaitReason     string `json:"wait_reason"`
	WaitingOnRunID string `json:"waiting_on_run_id"`
}

type statusPayloadJSON struct {
	Run   map[string]any   `json:"run"`
	Tasks []statusTaskJSON `json:"tasks"`
}

func ranSchedule() *fakeStateStatus {
	return &fakeStateStatus{
		schedules: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{
			{ScheduleName: "other", LastRunId: "run-9"},
			{ScheduleName: "daily", LastRunId: statusRunID, HasActiveRun: false, LastRunStatus: "failed"},
		}},
		run: &runv1.Run{
			RunId: statusRunID, ScheduleName: "daily", Kind: "cron", Operation: "run", Status: "failed",
			CreatedAt: timestamppb.New(time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)), PromotionSeq: 3, ReleaseId: "rel-1",
			InitiatedBy: "alice", StartedAt: timestamppb.New(time.Date(2026, 10, 27, 9, 1, 0, 0, time.UTC)),
			CompletedAt:    timestamppb.New(time.Date(2026, 10, 27, 9, 5, 0, 0, time.UTC)),
			LastProgressAt: timestamppb.New(time.Date(2026, 10, 27, 9, 4, 0, 0, time.UTC)),
			CancelledAt:    timestamppb.New(time.Date(2026, 10, 27, 9, 3, 0, 0, time.UTC)),
			CancelledBy:    "bob", CancellationReason: "stop", FailureReason: "stalled",
		},
		taskPages: []*runv1.ListTasksResponse{{TotalCount: 2, Tasks: []*runv1.Task{
			{TaskId: "t-a", ServiceName: "svc", SchemaName: "analytics", TableName: "orders", Status: "succeeded"},
			{TaskId: "t-b", ServiceName: "svc", SchemaName: "analytics", TableName: "revenue", Status: "failed", Attempt: 1,
				WaitReason: "node_lease", WaitingOnRunId: "run-1"},
		}}},
		attempts: []*runv1.TaskAttempt{
			{TaskId: "t-b", Attempt: 0, Status: "failed", FailureClass: "infra", FailureReason: "create_failed"},
			{TaskId: "t-b", Attempt: 1, Status: "failed", FailureClass: "task", FailureReason: "dbt_error"},
		},
		attemptsTotal: 2,
	}
}

// schedule status prints exactly what run show prints for the schedule's
// latest run.
func TestScheduleStatus_PrintsTheRunShowShape(t *testing.T) {
	fake := ranSchedule()

	stdout, stderr, exit := runStatus(t, fake, []string{"daily"}, false)

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	var p statusPayloadJSON
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	assert.Equal(t, statusRunID, p.Run["run_id"])
	assert.Equal(t, "failed", p.Run["status"])
	assert.Equal(t, "daily", p.Run["schedule_name"])
	require.Len(t, p.Tasks, 2)
	assert.Equal(t, statusTaskJSON{Node: "svc.analytics.orders", Status: "succeeded"}, p.Tasks[0])
	assert.Equal(t, statusTaskJSON{Node: "svc.analytics.revenue", Status: "failed", Attempt: 1, FailureClass: "task",
		FailureReason: "dbt_error", WaitReason: "node_lease", WaitingOnRunID: "run-1"}, p.Tasks[1])
	assert.Equal(t, []string{statusRunID}, fake.getRunIDs)
	for _, gone := range []string{"schedule_name", "run_id", "has_active_run", "last_run_status", "nodes"} {
		var top map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(stdout), &top))
		assert.NotContains(t, top, gone, "the old top-level key %s is gone", gone)
	}
}

func TestScheduleStatus_PagesThroughAllTasks(t *testing.T) {
	var first, second []*runv1.Task
	for i := 0; i < 200; i++ {
		first = append(first, &runv1.Task{TaskId: "t", ServiceName: "s", SchemaName: "x", TableName: "a", Status: "succeeded"})
	}
	second = append(second, &runv1.Task{TaskId: "u", ServiceName: "s", SchemaName: "x", TableName: "c", Status: "skipped"})
	fake := ranSchedule()
	fake.taskPages = []*runv1.ListTasksResponse{{TotalCount: 201, Tasks: first}, {TotalCount: 201, Tasks: second}}
	fake.attempts, fake.attemptsTotal = nil, 0

	stdout, _, exit := runStatus(t, fake, []string{"daily"}, false)

	require.Equal(t, 0, exit)
	var p statusPayloadJSON
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	require.Len(t, p.Tasks, 201)
	assert.Equal(t, "skipped", p.Tasks[200].Status)
	assert.Equal(t, 2, fake.listTasksCall)
}

func TestScheduleStatus_UnknownNameExits3(t *testing.T) {
	fake := ranSchedule()

	stdout, _, exit := runStatus(t, fake, []string{"nope"}, false)

	assert.Equal(t, 3, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeNotFound, env["error"].Code)
}

func TestScheduleStatus_ANeverRunScheduleReturnsANullRunAndNoTasks(t *testing.T) {
	fake := &fakeStateStatus{schedules: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{
		{ScheduleName: "daily"},
	}}}

	stdout, _, exit := runStatus(t, fake, []string{"daily"}, false)

	assert.Equal(t, 0, exit)
	assert.JSONEq(t, `{"run":null,"tasks":[]}`, stdout)
	assert.Empty(t, fake.getRunIDs)
	assert.Zero(t, fake.listTasksCall)
}

func TestScheduleStatus_AListTasksFailureExits5(t *testing.T) {
	fake := ranSchedule()
	fake.taskErr = status.Error(codes.Unavailable, "down")

	_, _, exit := runStatus(t, fake, []string{"daily"}, false)

	assert.Equal(t, 5, exit)
}

func TestScheduleStatus_WrongArgCountExits2(t *testing.T) {
	stdout, _, exit := runStatus(t, &fakeStateStatus{}, []string{}, false)
	assert.Equal(t, 2, exit)

	// A usage error must still emit the machine-readable envelope on stdout,
	// even though it is detected before RunE.
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestScheduleStatus_EveryRPCCarriesItsOwnDeadline(t *testing.T) {
	fake := ranSchedule()

	_, _, exit := runStatus(t, fake, []string{"daily"}, false)

	require.Equal(t, 0, exit)
	for _, rpc := range []string{"ListSchedules", "GetRun", "ListTasks", "ListRunAttempts"} {
		require.NotEmpty(t, fake.deadlines[rpc], "%s was not called", rpc)
		for i, has := range fake.deadlines[rpc] {
			assert.True(t, has, "%s call %d carries no deadline", rpc, i)
		}
	}
}

// The declared output_schema lists exactly the keys the command emits, at every
// depth, for a run that has every optional field set.
func TestScheduleStatus_OutputMatchesItsSchema(t *testing.T) {
	stdout, _, exit := runStatus(t, ranSchedule(), []string{"daily"}, false)
	require.Equal(t, 0, exit)

	cmd := NewStatusCommand(nil, &config.Config{}, io.Discard, io.Discard)
	requireShapeMatches(t, json.RawMessage(cmd.Annotations["output_schema"]), json.RawMessage(stdout))
}

// The help's list of the statuses a task can carry ("status is one of: ...")
// is exactly the task_status values RunService documents on Task.status.
func TestScheduleStatus_LongNamesEveryTaskStatus(t *testing.T) {
	long := NewStatusCommand(nil, nil, io.Discard, io.Discard).Long
	const prefix = "status is one of: "
	var listed []string
	for _, line := range strings.Split(long, "\n") {
		if i := strings.Index(line, prefix); i >= 0 && strings.Contains(line, "pending") {
			listed = strings.Split(strings.TrimSuffix(strings.TrimSpace(line[i+len(prefix):]), "."), "|")
		}
	}
	require.NotEmpty(t, listed, "the help has no task status line")
	served := []string{"pending", "dispatched", "queued", "running", "stopping", "succeeded", "failed", "cancelled", "skipped"}
	assert.ElementsMatch(t, served, listed)
}

func TestScheduleStatus_LongExplainsALeaseWaitAndTheCancellingRunStatus(t *testing.T) {
	long := NewStatusCommand(nil, nil, io.Discard, io.Discard).Long

	assert.Contains(t, long, "wait_reason is node_lease while the task waits for another run to release its node")
	assert.Contains(t, long, "cancelling")
}

func TestScheduleStatus_HumanModeWritesASummaryToStderr(t *testing.T) {
	stdout, stderr, exit := runStatus(t, ranSchedule(), []string{"daily"}, true)

	require.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, statusRunID)
	assert.Contains(t, stderr, "svc.analytics.revenue")
}
