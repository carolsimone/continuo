package node

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
)

type fakeNodeState struct {
	client.RunClient

	runsResp     *runv1.ListNodeTasksResponse
	runsErr      error
	gotSvc       string
	gotSchema    string
	gotTable     string
	gotOperation string
	gotLimit     int32

	trigResp         *runv1.TriggerSingleNodeRunResponse
	trigErr          error
	gotActor         string
	gotRequestIDs    []string
	gotTrigSvc       string
	gotTrigSourceRun string

	testResp         *runv1.TriggerSingleNodeRunResponse
	testErr          error
	gotTestActor     string
	gotTestSvc       string
	gotTestSchema    string
	gotTestTable     string
	gotTestSourceRun string

	buildResp         *runv1.TriggerSingleNodeRunResponse
	buildErr          error
	gotBuildActor     string
	gotBuildSvc       string
	gotBuildSchema    string
	gotBuildTable     string
	gotBuildSourceRun string

	fullRefreshResp         *runv1.TriggerSingleNodeRunResponse
	fullRefreshErr          error
	gotFullRefreshActor     string
	gotFullRefreshSvc       string
	gotFullRefreshSchema    string
	gotFullRefreshTable     string
	gotFullRefreshSourceRun string

	listResp         *runv1.ListNodesResponse
	listErr          error
	gotListSearch    string
	gotListService   string
	gotListOperation string
	gotListLimit     int32
	gotListOffset    int32
}

func (f *fakeNodeState) ListNodes(_ context.Context, search, service, operation string, limit, offset int32) (*runv1.ListNodesResponse, error) {
	f.gotListSearch, f.gotListService, f.gotListOperation, f.gotListLimit, f.gotListOffset = search, service, operation, limit, offset
	return f.listResp, f.listErr
}

func (f *fakeNodeState) ListNodeRuns(_ context.Context, service, schema, table, operation string, limit int32) (*runv1.ListNodeTasksResponse, error) {
	f.gotSvc, f.gotSchema, f.gotTable, f.gotOperation, f.gotLimit = service, schema, table, operation, limit
	return f.runsResp, f.runsErr
}

func (f *fakeNodeState) TriggerNodeRun(_ context.Context, requestID, service, _, _, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error) {
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotTrigSvc, f.gotTrigSourceRun, f.gotActor = service, sourceRunID, actor
	return f.trigResp, f.trigErr
}

func (f *fakeNodeState) TriggerNodeTest(_ context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error) {
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotTestSvc, f.gotTestSchema, f.gotTestTable, f.gotTestSourceRun, f.gotTestActor = service, schema, table, sourceRunID, actor
	return f.testResp, f.testErr
}

func (f *fakeNodeState) TriggerNodeBuild(_ context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error) {
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotBuildSvc, f.gotBuildSchema, f.gotBuildTable, f.gotBuildSourceRun, f.gotBuildActor = service, schema, table, sourceRunID, actor
	return f.buildResp, f.buildErr
}

func (f *fakeNodeState) TriggerNodeFullRefresh(_ context.Context, requestID, service, schema, table, sourceRunID, actor string) (*runv1.TriggerSingleNodeRunResponse, error) {
	f.gotRequestIDs = append(f.gotRequestIDs, requestID)
	f.gotFullRefreshSvc, f.gotFullRefreshSchema, f.gotFullRefreshTable, f.gotFullRefreshSourceRun, f.gotFullRefreshActor = service, schema, table, sourceRunID, actor
	return f.fullRefreshResp, f.fullRefreshErr
}

func (f *fakeNodeState) Close() error { return nil }

// runHistory invokes the history command against fake.
func runHistory(t *testing.T, fake client.RunClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewHistoryCommand(
		func(context.Context, string) (client.RunClient, error) { return fake, nil },
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

func TestHistory_SuccessMapsAllFields(t *testing.T) {
	fake := &fakeNodeState{runsResp: &runv1.ListNodeTasksResponse{Tasks: []*runv1.NodeTask{{
		RunId: "run_1", ScheduleName: "single-node-run-abcd1234", Kind: "single_node_run",
		RunStatus: "succeeded", TaskId: "task_1", TaskStatus: "succeeded",
		Attempt: 1, FailureClass: "task", FailureReason: "exit_code",
		ImageTag: "img:v9", CreatedAt: "2026-07-10T10:00:00Z", CompletedAt: "2026-07-10T10:05:00Z",
		RunResultsUri: "run-results/task-executions/finance/analytics/orders/e1.json",
	}}}}

	stdout, stderr, exit := runHistory(t, fake, []string{"finance", "analytics", "orders"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.Equal(t, int32(50), fake.gotLimit)
	assert.Equal(t, "finance", fake.gotSvc)
	assert.Equal(t, "analytics", fake.gotSchema)
	assert.Equal(t, "orders", fake.gotTable)
	assert.Equal(t, "run", fake.gotOperation)

	var payload struct {
		Runs []map[string]any `json:"runs"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Runs, 1)
	r := payload.Runs[0]
	assert.Equal(t, "run_1", r["run_id"])
	assert.Equal(t, "single_node_run", r["kind"])
	assert.Equal(t, "succeeded", r["run_status"])
	assert.Equal(t, "img:v9", r["image_tag"])
	assert.NotContains(t, r, "manifest_version")
	// The attempt number is reported as "attempt"; the retry_count key does not exist.
	assert.EqualValues(t, 1, r["attempt"])
	assert.NotContains(t, r, "retry_count")
	assert.Equal(t, "task", r["failure_class"])
	assert.Equal(t, "exit_code", r["failure_reason"])
	// The structured result key must reach the CLI payload: it is vendored
	// through this module.s own proto copy, so a run-controller-side addition is
	// invisible here until that copy is regenerated.
	assert.Equal(t, "run-results/task-executions/finance/analytics/orders/e1.json", r["run_results_uri"])
}

func TestHistory_OperationFlagIsForwarded(t *testing.T) {
	fake := &fakeNodeState{runsResp: &runv1.ListNodeTasksResponse{Tasks: []*runv1.NodeTask{{
		RunId: "run_1", TaskStatus: "succeeded", Operation: "test",
	}}}}

	stdout, _, exit := runHistory(t, fake, []string{"finance", "analytics", "orders", "--operation", "test"}, false)

	assert.Equal(t, 0, exit)
	assert.Equal(t, "test", fake.gotOperation)

	var payload struct {
		Runs []map[string]any `json:"runs"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Runs, 1)
	assert.Equal(t, "test", payload.Runs[0]["operation"])
}

func TestHistory_InvalidOperationFlagExits2(t *testing.T) {
	fake := &fakeNodeState{}
	stdout, _, exit := runHistory(t, fake, []string{"finance", "analytics", "orders", "--operation", "bogus"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestHistory_EmptyResultIsEmptyArrayNotNull(t *testing.T) {
	fake := &fakeNodeState{runsResp: &runv1.ListNodeTasksResponse{Tasks: nil}}
	stdout, _, exit := runHistory(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.Equal(t, 0, exit)
	assert.Contains(t, stdout, `"runs":[]`)
}

func TestHistory_OmitsEmptyOptionalFields(t *testing.T) {
	fake := &fakeNodeState{runsResp: &runv1.ListNodeTasksResponse{Tasks: []*runv1.NodeTask{{
		RunId: "run_1", TaskStatus: "running", // no run_status/completed_at/error/log key
	}}}}
	stdout, _, _ := runHistory(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.NotContains(t, stdout, "run_status")
	assert.NotContains(t, stdout, "completed_at")
	assert.NotContains(t, stdout, "error_message")
	assert.NotContains(t, stdout, "log_s3_key")
	assert.NotContains(t, stdout, "run_results_uri")
	assert.NotContains(t, stdout, "failure_class")
	assert.NotContains(t, stdout, "failure_reason")
}

func TestHistory_WrongArgCountExits2(t *testing.T) {
	fake := &fakeNodeState{}
	stdout, _, exit := runHistory(t, fake, []string{"finance", "analytics"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestHistory_UnavailableExits5(t *testing.T) {
	fake := &fakeNodeState{runsErr: status.Error(codes.Unavailable, "server down")}
	_, _, exit := runHistory(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.Equal(t, 5, exit)
}

func TestHistory_ContentHashComesWithEachRun(t *testing.T) {
	stateFake := &fakeNodeState{runsResp: &runv1.ListNodeTasksResponse{Tasks: []*runv1.NodeTask{
		{RunId: "run_1", TaskStatus: "succeeded", ContentHash: "c1"},
		{RunId: "run_2", TaskStatus: "succeeded"}, // names no release
	}}}

	stdout, _, exit := runHistory(t, stateFake, []string{"finance", "analytics", "orders"}, false)

	assert.Equal(t, 0, exit)
	var payload struct {
		Runs []map[string]any `json:"runs"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Runs, 2)
	assert.Equal(t, "c1", payload.Runs[0]["content_hash"])
	_, hasHash := payload.Runs[1]["content_hash"]
	assert.False(t, hasHash, "content_hash is omitted, not present-and-empty, for a run that names no release")
}

func TestHistory_HumanModeUsesStderr(t *testing.T) {
	fake := &fakeNodeState{runsResp: &runv1.ListNodeTasksResponse{Tasks: []*runv1.NodeTask{{
		RunId: "run_1", TaskStatus: "succeeded", Kind: "single_node_run", CompletedAt: "2026-07-10T10:05:00Z",
	}}}}
	stdout, stderr, exit := runHistory(t, fake, []string{"finance", "analytics", "orders"}, true)
	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "run_1")
	assert.Contains(t, stderr, "succeeded")
}

// Human mode renders the same content hash the JSON payload carries.
func TestHistory_HumanModeRendersExecutedHashColumn(t *testing.T) {
	stateFake := &fakeNodeState{runsResp: &runv1.ListNodeTasksResponse{Tasks: []*runv1.NodeTask{
		{RunId: "run_1", TaskStatus: "succeeded", ContentHash: "sha256:abcdef123456789"},
		{RunId: "run_2", TaskStatus: "succeeded"}, // names no release
	}}}

	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: true}
	cmd := NewHistoryCommand(
		func(context.Context, string) (client.RunClient, error) { return stateFake, nil },
		cfg, &outBuf, &errBuf,
	)
	cmd.SetArgs([]string{"finance", "analytics", "orders"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	require.NoError(t, cmd.Execute())

	stderr := errBuf.String()
	assert.Contains(t, stderr, "EXECUTED_HASH", "header names the content-hash column")
	assert.Contains(t, stderr, "sha256:abcde", "the executed hash itself is rendered, truncated to 12 characters")
	assert.NotContains(t, stderr, "sha256:abcdef123456789", "the full hash must be truncated, not printed verbatim")
	assert.Contains(t, stderr, "run_1")
	assert.Contains(t, stderr, "run_2")

	lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
	require.Len(t, lines, 3, "header + two run rows")
	assert.True(t, strings.HasSuffix(lines[2], "  -"), "run_2 names no release: hash column renders as '-', got %q", lines[2])
}

// The help names the values of both status fields, including the run's
// cancelling status and the task's stopping status.
func TestHistory_LongNamesTheRunAndTaskStatuses(t *testing.T) {
	long := NewHistoryCommand(nil, nil, io.Discard, io.Discard).Long

	assert.Contains(t, long, "run_status is the run's status: pending|running|cancelling|succeeded|failed|cancelled|skipped.")
	assert.Contains(t, long, "task_status is one of: pending|dispatched|queued|running|stopping|succeeded|failed|cancelled|skipped.")
}

// A run whose task waits for the node's lease says why, on which run and since
// when; a run that waits for nothing omits all three.
func TestHistory_ReportsALeaseWaitAndItsHolder(t *testing.T) {
	fake := &fakeNodeState{runsResp: &runv1.ListNodeTasksResponse{Tasks: []*runv1.NodeTask{
		{RunId: "run_2", TaskStatus: "pending", WaitReason: "node_lease", WaitingOnRunId: "run_1", WaitingSince: "2026-10-24T10:05:00Z"},
		{RunId: "run_1", TaskStatus: "running"},
	}}}

	stdout, _, exit := runHistory(t, fake, []string{"finance", "analytics", "orders"}, false)

	require.Equal(t, 0, exit)
	var payload struct {
		Runs []map[string]any `json:"runs"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Runs, 2)
	assert.Equal(t, "node_lease", payload.Runs[0]["wait_reason"])
	assert.Equal(t, "run_1", payload.Runs[0]["waiting_on_run_id"])
	assert.Equal(t, "2026-10-24T10:05:00Z", payload.Runs[0]["waiting_since"])
	for _, k := range []string{"wait_reason", "waiting_on_run_id", "waiting_since"} {
		assert.NotContains(t, payload.Runs[1], k, "a run that waits for nothing omits %s", k)
	}
}

// The output_schema annotation declares every field a run can carry: a run
// with every field set emits exactly the keys its runs item declares.
func TestHistory_OutputSchemaDeclaresEveryRunField(t *testing.T) {
	fake := &fakeNodeState{runsResp: &runv1.ListNodeTasksResponse{Tasks: []*runv1.NodeTask{{
		RunId: "run_2", ScheduleName: "single-node-run-abcd1234", Kind: "single_node_run",
		RunStatus: "running", TaskId: "task_2", TaskStatus: "pending", Attempt: 1,
		ImageTag: "img:v9", CreatedAt: "2026-10-24T10:00:00Z", StartedAt: "2026-10-24T10:01:00Z",
		CompletedAt: "2026-10-24T10:02:00Z", ErrorMessage: "boom", FailureClass: "task", FailureReason: "exit_code",
		LogS3Key: "logs/k", RunResultsUri: "results/k", Operation: "run", ContentHash: "c1",
		WaitReason: "node_lease", WaitingOnRunId: "run_1", WaitingSince: "2026-10-24T10:05:00Z",
	}}}}
	stdout, _, exit := runHistory(t, fake, []string{"finance", "analytics", "orders"}, false)
	require.Equal(t, 0, exit)

	var emitted struct {
		Runs []map[string]json.RawMessage `json:"runs"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &emitted))
	require.Len(t, emitted.Runs, 1)

	var schema struct {
		Runs []map[string]string `json:"runs"`
	}
	cmd := NewHistoryCommand(nil, &config.Config{}, io.Discard, io.Discard)
	require.NoError(t, json.Unmarshal([]byte(cmd.Annotations["output_schema"]), &schema))
	require.Len(t, schema.Runs, 1, "output_schema declares the fields of one runs item")

	assert.ElementsMatch(t, mapKeys(emitted.Runs[0]), mapKeys(schema.Runs[0]))
	assert.Equal(t, "string", schema.Runs[0]["wait_reason"])
	assert.Equal(t, "string", schema.Runs[0]["waiting_on_run_id"])
	assert.Equal(t, "string", schema.Runs[0]["waiting_since"])
}

// The help says when the wait fields are set and what they name.
func TestHistory_LongExplainsALeaseWait(t *testing.T) {
	long := NewHistoryCommand(nil, nil, io.Discard, io.Discard).Long

	assert.Contains(t, long, `"wait_reason":string,"waiting_on_run_id":string,"waiting_since":string`)
	assert.Contains(t, long, "wait_reason is node_lease while the task waits for another run to release its node")
}

func mapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
