package schedule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// fakeRunsClient implements client.RunClient for the schedule runs tests.
type fakeRunsClient struct {
	client.RunClient
	resp        *runv1.ListRunsResponse
	err         error
	calls       []*runv1.ListRunsRequest
	hadDeadline bool
}

func (f *fakeRunsClient) ListRuns(ctx context.Context, scheduleName string, pageSize, pageOffset int32) (*runv1.ListRunsResponse, error) {
	_, f.hadDeadline = ctx.Deadline()
	f.calls = append(f.calls, &runv1.ListRunsRequest{ScheduleName: scheduleName, PageSize: pageSize, PageOffset: pageOffset})
	return f.resp, f.err
}

func (f *fakeRunsClient) Close() error { return nil }

func runRuns(t *testing.T, fake client.RunClient, args ...string) (stdout string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cmd := NewRunsCommand(
		func(context.Context, string) (client.RunClient, error) { return fake, nil },
		&config.Config{Timeout: 2 * time.Second}, &outBuf, &errBuf,
	)
	cmd.SetArgs(args)
	if err := cmd.Execute(); err != nil {
		var cliErr output.CLIError
		if errors.As(err, &cliErr) {
			exit = cliErr.ExitCode()
		} else {
			exit = 1
		}
	}
	return outBuf.String(), exit
}

func twoRuns() *runv1.ListRunsResponse {
	return &runv1.ListRunsResponse{
		TotalCount: 120,
		Runs: []*runv1.RunSummary{
			{RunId: "r2", ScheduleName: "daily", Status: "failed", CreatedAt: "2026-10-27T09:00:00Z", CompletedAt: "2026-10-27T09:05:00Z", FailureReason: "stalled"},
			{RunId: "r1", ScheduleName: "daily", Status: "succeeded", CreatedAt: "2026-10-26T09:00:00Z", CompletedAt: "2026-10-26T09:04:00Z"},
		},
	}
}

func TestScheduleRuns_PrintsOnePageAndTheTotalCount(t *testing.T) {
	fake := &fakeRunsClient{resp: twoRuns()}

	stdout, exit := runRuns(t, fake, "daily")

	require.Equal(t, 0, exit, stdout)
	var p struct {
		Runs  []map[string]any `json:"runs"`
		Total int              `json:"total_count"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &p))
	require.Len(t, p.Runs, 2)
	assert.Equal(t, 120, p.Total, "total_count counts every completed run, not the page")
	assert.Equal(t, "r2", p.Runs[0]["run_id"])
	assert.Equal(t, "stalled", p.Runs[0]["failure_reason"])
	assert.NotContains(t, p.Runs[1], "failure_reason")
	require.Len(t, fake.calls, 1, "the list is one call")
	assert.Equal(t, "daily", fake.calls[0].GetScheduleName())
	assert.Zero(t, fake.calls[0].GetPageSize(), "page_size 0 asks for the server default")
	assert.True(t, fake.hadDeadline)
}

func TestScheduleRuns_ANeverRunScheduleListsNothing(t *testing.T) {
	stdout, exit := runRuns(t, &fakeRunsClient{resp: &runv1.ListRunsResponse{}}, "daily")

	require.Equal(t, 0, exit)
	assert.JSONEq(t, `{"runs":[],"total_count":0}`, stdout)
}

func TestScheduleRuns_OutputMatchesItsSchema(t *testing.T) {
	stdout, exit := runRuns(t, &fakeRunsClient{resp: twoRuns()}, "daily")
	require.Equal(t, 0, exit)

	cmd := NewRunsCommand(nil, &config.Config{}, nil, nil)
	requireShapeMatches(t, json.RawMessage(cmd.Annotations["output_schema"]), json.RawMessage(stdout))
}

func TestScheduleRuns_WrongArgumentCountIsRefusedExit2(t *testing.T) {
	for _, args := range [][]string{{}, {"a", "b"}} {
		stdout, exit := runRuns(t, &fakeRunsClient{}, args...)
		assert.Equal(t, 2, exit)
		var env map[string]output.CLIError
		require.NoError(t, json.Unmarshal([]byte(stdout), &env))
		assert.Equal(t, output.CodeUsage, env["error"].Code)
	}
}

func TestScheduleRuns_ARPCFailureExitsWithItsCode(t *testing.T) {
	_, exit := runRuns(t, &fakeRunsClient{err: status.Error(codes.Unavailable, "down")}, "daily")
	assert.Equal(t, 5, exit)
}
