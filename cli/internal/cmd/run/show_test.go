package run

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func fullRun() *runv1.Run {
	return &runv1.Run{
		RunId: testRunID, ScheduleName: "daily", Kind: "cron", Operation: "run", Status: "failed",
		FailureReason: "stalled", ReleaseId: "rel-1", PromotionSeq: 7, InitiatedBy: "alice",
		CreatedAt: ts(2026, 10, 27, 9, 0), StartedAt: ts(2026, 10, 27, 9, 1), CompletedAt: ts(2026, 10, 27, 9, 5),
		LastProgressAt: ts(2026, 10, 27, 9, 4), CancelledAt: ts(2026, 10, 27, 9, 3),
		CancelledBy: "bob", CancellationReason: "stop",
	}
}

func showFake() *fakeRunClient {
	return &fakeRunClient{
		run: fullRun(),
		taskPages: []*runv1.ListTasksResponse{{TotalCount: 3, Tasks: []*runv1.Task{
			{TaskId: "t-a", ServiceName: "svc", SchemaName: "raw", TableName: "a", Status: "succeeded", Attempt: 1},
			{TaskId: "t-b", ServiceName: "svc", SchemaName: "raw", TableName: "b", Status: "failed", Attempt: 2},
			{TaskId: "t-c", ServiceName: "svc", SchemaName: "raw", TableName: "c", Status: "pending",
				WaitReason: "node_lease", WaitingOnRunId: "run-other"},
		}}},
		attemptPgs: []*runv1.ListRunAttemptsResponse{{TotalCount: 5, Attempts: []*runv1.TaskAttempt{
			{TaskId: "t-a", Attempt: 0, Status: "failed", FailureClass: "infra", FailureReason: "create_failed"},
			{TaskId: "t-a", Attempt: 1, Status: "succeeded"},
			{TaskId: "t-b", Attempt: 0, Status: "failed", FailureClass: "task", FailureReason: "dbt_error"},
			{TaskId: "t-b", Attempt: 1, Status: "failed", FailureClass: "task", FailureReason: "dbt_error"},
			{TaskId: "t-b", Attempt: 2, Status: "failed", FailureClass: "timeout", FailureReason: "deadline_exceeded"},
		}}},
	}
}

type showTask struct {
	Node           string `json:"node"`
	Status         string `json:"status"`
	Attempt        int32  `json:"attempt"`
	FailureClass   string `json:"failure_class"`
	FailureReason  string `json:"failure_reason"`
	WaitReason     string `json:"wait_reason"`
	WaitingOnRunID string `json:"waiting_on_run_id"`
}

func TestRunShow_PrintsTheRunAndEachTasksLatestAttemptFailure(t *testing.T) {
	fake := showFake()

	stdout, stderr, exit := execute(t, NewShowCommand, fake, testRunID)

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	var p struct {
		Run   map[string]any `json:"run"`
		Tasks []showTask     `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	assert.Equal(t, testRunID, p.Run["run_id"])
	assert.Equal(t, "failed", p.Run["status"])
	assert.Equal(t, "2026-10-27T09:05:00Z", p.Run["completed_at"])
	require.Len(t, p.Tasks, 3)
	assert.Equal(t, showTask{Node: "svc.raw.a", Status: "succeeded", Attempt: 1}, p.Tasks[0],
		"a task that succeeded after a failed attempt carries no failure")
	assert.Equal(t, showTask{Node: "svc.raw.b", Status: "failed", Attempt: 2, FailureClass: "timeout", FailureReason: "deadline_exceeded"}, p.Tasks[1],
		"the failure of a task is the one of its latest attempt")
	assert.Equal(t, showTask{Node: "svc.raw.c", Status: "pending", WaitReason: "node_lease", WaitingOnRunID: "run-other"}, p.Tasks[2],
		"a task with no attempt has empty failure fields")
	assert.Equal(t, []string{testRunID}, fake.getRunCalls)
}

func TestRunShow_PagesThroughEveryTaskAndEveryAttempt(t *testing.T) {
	fake := &fakeRunClient{run: fullRun()}
	var tasks []*runv1.Task
	var attempts []*runv1.TaskAttempt
	for i := 0; i < 250; i++ {
		id := string(rune('a'+i%26)) + string(rune('A'+i/26))
		tasks = append(tasks, &runv1.Task{TaskId: id, ServiceName: "s", SchemaName: "x", TableName: id, Status: "succeeded"})
		attempts = append(attempts, &runv1.TaskAttempt{TaskId: id, Attempt: 0, Status: "succeeded"})
	}
	fake.taskPages = []*runv1.ListTasksResponse{{TotalCount: 250, Tasks: tasks[:200]}, {TotalCount: 250, Tasks: tasks[200:]}}
	fake.attemptPgs = []*runv1.ListRunAttemptsResponse{{TotalCount: 250, Attempts: attempts[:200]}, {TotalCount: 250, Attempts: attempts[200:]}}

	stdout, _, exit := execute(t, NewShowCommand, fake, testRunID)

	require.Equal(t, 0, exit)
	var p struct {
		Tasks []showTask `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	assert.Len(t, p.Tasks, 250)
	require.Len(t, fake.attemptCalls, 2)
	assert.Equal(t, int32(200), fake.attemptCalls[0].pageSize)
	assert.Equal(t, int32(200), fake.attemptCalls[1].pageOffset)
	assert.Empty(t, fake.attemptCalls[0].taskID)
}

func TestRunShow_OutputMatchesItsSchema(t *testing.T) {
	stdout, _, exit := execute(t, NewShowCommand, showFake(), testRunID)
	require.Equal(t, 0, exit)

	cmd := NewShowCommand(nil, &config.Config{}, nil, nil)
	requireShapeMatches(t, json.RawMessage(cmd.Annotations["output_schema"]), json.RawMessage(stdout))
}

func TestRunShow_UnknownRunIsNotFound(t *testing.T) {
	fake := &fakeRunClient{getRunErr: status.Error(codes.NotFound, "run not found")}

	stdout, _, exit := execute(t, NewShowCommand, fake, testRunID)

	assert.Equal(t, 3, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeNotFound, env["error"].Code)
}

func TestRunShow_WrongArgumentCountIsRefusedExit2(t *testing.T) {
	for _, args := range [][]string{{}, {testRunID, "extra"}} {
		stdout, _, exit := execute(t, NewShowCommand, &fakeRunClient{}, args...)
		assert.Equal(t, 2, exit)
		var env map[string]output.CLIError
		require.NoError(t, json.Unmarshal([]byte(stdout), &env))
		assert.Equal(t, output.CodeUsage, env["error"].Code)
	}
}

func TestRunShow_EveryRPCCarriesItsOwnDeadline(t *testing.T) {
	fake := showFake()

	_, _, exit := execute(t, NewShowCommand, fake, testRunID)

	require.Equal(t, 0, exit)
	require.NotEmpty(t, fake.deadlineCalls)
	for i, has := range fake.deadlineCalls {
		assert.True(t, has, "RPC %d carries no deadline", i)
	}
}

func TestRunShow_ARPCFailureExitsWithItsCode(t *testing.T) {
	fake := showFake()
	fake.attemptPgs = nil
	fake.getRunErr = status.Error(codes.Unavailable, "down")

	_, _, exit := execute(t, NewShowCommand, fake, testRunID)

	assert.Equal(t, 5, exit)
}

func TestRunShow_HumanModeWritesASummaryToStderr(t *testing.T) {
	fake := showFake()
	stdout, stderr, exit := executeHuman(t, NewShowCommand, fake, testRunID)

	require.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, testRunID)
	assert.Contains(t, stderr, "svc.raw.b")
	assert.Contains(t, stderr, "timeout")
}
