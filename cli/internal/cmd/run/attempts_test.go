package run

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTaskID = "22222222-2222-4222-8222-222222222222"

func attemptsFake() *fakeRunClient {
	return &fakeRunClient{attemptPgs: []*runv1.ListRunAttemptsResponse{{
		TotalCount: 3,
		Attempts: []*runv1.TaskAttempt{{
			TaskId: testTaskID, Attempt: 1, JobName: "job-1", Status: "failed", FailureClass: "task",
			FailureReason: "dbt_error", ExitCode: 2, DispatchedAt: ts(2026, 10, 27, 9, 0), QueuedAt: ts(2026, 10, 27, 9, 1),
			StartedAt: ts(2026, 10, 27, 9, 2), FinishedAt: ts(2026, 10, 27, 9, 3), ExecutionSeconds: 61.5,
			ErrorMessage: "boom", LogS3Key: "logs/x", RunResultsUri: "results/x",
		}, {
			TaskId: testTaskID, Attempt: 2, JobName: "job-2", Status: "dispatched", DispatchedAt: ts(2026, 10, 27, 9, 4),
		}},
	}}}
}

func TestRunAttempts_PrintsOnePageAndTheTotalCount(t *testing.T) {
	fake := attemptsFake()

	stdout, stderr, exit := execute(t, NewAttemptsCommand, fake, testRunID)

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	var p struct {
		Attempts []map[string]any `json:"attempts"`
		Total    int              `json:"total_count"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	require.Len(t, p.Attempts, 2)
	assert.Equal(t, 3, p.Total, "total_count counts every attempt, not the page")
	assert.Equal(t, "dbt_error", p.Attempts[0]["failure_reason"])
	assert.Equal(t, "2026-10-27T09:03:00Z", p.Attempts[0]["finished_at"])
	assert.Equal(t, 61.5, p.Attempts[0]["execution_seconds"])
	assert.NotContains(t, p.Attempts[1], "finished_at", "an unset timestamp is omitted")
	assert.Equal(t, []attemptCall{{runID: testRunID, pageSize: 200}}, fake.attemptCalls)
}

func TestRunAttempts_TheOptionalTaskIDNarrowsTheCall(t *testing.T) {
	fake := attemptsFake()

	_, _, exit := execute(t, NewAttemptsCommand, fake, testRunID, testTaskID)

	require.Equal(t, 0, exit)
	assert.Equal(t, []attemptCall{{runID: testRunID, taskID: testTaskID, pageSize: 200}}, fake.attemptCalls)
}

func TestRunAttempts_AnEmptyRunPrintsAnEmptyListNotNull(t *testing.T) {
	stdout, _, exit := execute(t, NewAttemptsCommand, &fakeRunClient{}, testRunID)

	require.Equal(t, 0, exit)
	assert.JSONEq(t, `{"attempts":[],"total_count":0}`, stdout)
}

func TestRunAttempts_OutputMatchesItsSchema(t *testing.T) {
	stdout, _, exit := execute(t, NewAttemptsCommand, attemptsFake(), testRunID)
	require.Equal(t, 0, exit)

	cmd := NewAttemptsCommand(nil, &config.Config{}, nil, nil)
	requireShapeMatches(t, json.RawMessage(cmd.Annotations["output_schema"]), json.RawMessage(stdout))
}

func TestRunAttempts_ABlankTaskIDIsRefusedExit2(t *testing.T) {
	fake := attemptsFake()

	stdout, _, exit := execute(t, NewAttemptsCommand, fake, testRunID, "")

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
	assert.Empty(t, fake.attemptCalls, "a refused call reaches no server")
}

func TestRunAttempts_WrongArgumentCountIsRefusedExit2(t *testing.T) {
	for _, args := range [][]string{{}, {testRunID, testTaskID, "extra"}} {
		_, _, exit := execute(t, NewAttemptsCommand, &fakeRunClient{}, args...)
		assert.Equal(t, 2, exit)
	}
}
