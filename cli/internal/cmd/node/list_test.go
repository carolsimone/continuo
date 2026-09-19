package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func runList(t *testing.T, fake client.StateClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewListCommand(func(context.Context, string) (client.StateClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestList_SuccessMapsAllFieldsAndTotalCount(t *testing.T) {
	fake := &fakeNodeState{listResp: &statev1.ListNodesResponse{
		TotalCount: 7,
		Nodes: []*statev1.NodeSummary{{
			ServiceName: "finance", SchemaName: "analytics", TableName: "orders",
			RunCount: 12, SuccessRatePct: 92, AvgDurationSec: 34, P95DurationSec: 80,
			FlakyRatePct: 8, LastStatus: "succeeded", LastRunAt: "2026-09-18T10:00:00Z",
			Operation: "run",
		}},
	}}

	stdout, stderr, exit := runList(t, fake, []string{"--search", "orders", "--service", "finance", "--operation", "run", "--limit", "10", "--offset", "20"}, false)

	require.Equal(t, 0, exit, "stderr: %s", stderr)
	assert.Empty(t, stderr)

	var payload struct {
		TotalCount int `json:"total_count"`
		Nodes      []struct {
			ServiceName    string `json:"service_name"`
			SchemaName     string `json:"schema_name"`
			TableName      string `json:"table_name"`
			RunCount       int    `json:"run_count"`
			SuccessRatePct int    `json:"success_rate_pct"`
			AvgDurationSec int    `json:"avg_duration_sec"`
			P95DurationSec int    `json:"p95_duration_sec"`
			FlakyRatePct   int    `json:"flaky_rate_pct"`
			LastStatus     string `json:"last_status"`
			LastRunAt      string `json:"last_run_at"`
			Operation      string `json:"operation"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, 7, payload.TotalCount)
	require.Len(t, payload.Nodes, 1)
	n := payload.Nodes[0]
	assert.Equal(t, "finance", n.ServiceName)
	assert.Equal(t, "analytics", n.SchemaName)
	assert.Equal(t, "orders", n.TableName)
	assert.Equal(t, 12, n.RunCount)
	assert.Equal(t, 92, n.SuccessRatePct)
	assert.Equal(t, 34, n.AvgDurationSec)
	assert.Equal(t, 80, n.P95DurationSec)
	assert.Equal(t, 8, n.FlakyRatePct)
	assert.Equal(t, "succeeded", n.LastStatus)
	assert.Equal(t, "2026-09-18T10:00:00Z", n.LastRunAt)
	assert.Equal(t, "run", n.Operation)

	// Every flag is forwarded to the RPC verbatim.
	assert.Equal(t, "orders", fake.gotListSearch)
	assert.Equal(t, "finance", fake.gotListService)
	assert.Equal(t, "run", fake.gotListOperation)
	assert.Equal(t, int32(10), fake.gotListLimit)
	assert.Equal(t, int32(20), fake.gotListOffset)
}

func TestList_DefaultsForwardEmptyFiltersRunAndFirstPage(t *testing.T) {
	fake := &fakeNodeState{listResp: &statev1.ListNodesResponse{}}

	stdout, _, exit := runList(t, fake, nil, false)

	require.Equal(t, 0, exit)
	assert.Equal(t, "", fake.gotListSearch)
	assert.Equal(t, "", fake.gotListService)
	assert.Equal(t, "run", fake.gotListOperation)
	assert.Equal(t, defaultListLimit, fake.gotListLimit)
	assert.Equal(t, int32(0), fake.gotListOffset)
	// An empty catalog is a success with an empty array, never null.
	assert.JSONEq(t, `{"total_count":0,"nodes":[]}`, stdout)
}

func TestList_NegativeSentinelsAreOmitted(t *testing.T) {
	fake := &fakeNodeState{listResp: &statev1.ListNodesResponse{
		TotalCount: 1,
		Nodes: []*statev1.NodeSummary{{
			ServiceName: "finance", SchemaName: "analytics", TableName: "never_ran",
			RunCount: 0, SuccessRatePct: -1, AvgDurationSec: -1, P95DurationSec: -1,
			LastStatus: "pending", Operation: "run",
		}},
	}}

	stdout, _, exit := runList(t, fake, nil, false)

	require.Equal(t, 0, exit)
	var payload struct {
		Nodes []map[string]any `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Nodes, 1)
	n := payload.Nodes[0]
	_, hasSuccess := n["success_rate_pct"]
	_, hasAvg := n["avg_duration_sec"]
	_, hasP95 := n["p95_duration_sec"]
	assert.False(t, hasSuccess, "success_rate_pct must be omitted when the server sends -1")
	assert.False(t, hasAvg, "avg_duration_sec must be omitted when the server sends -1")
	assert.False(t, hasP95, "p95_duration_sec must be omitted when the server sends -1")
	// A real zero is still a value, not a sentinel.
	assert.Equal(t, float64(0), n["run_count"])
	assert.Equal(t, float64(0), n["flaky_rate_pct"])
}

func TestList_PositionalArgIsUsageError(t *testing.T) {
	fake := &fakeNodeState{listResp: &statev1.ListNodesResponse{}}

	stdout, _, exit := runList(t, fake, []string{"orders"}, false)

	assert.Equal(t, 2, exit)
	assert.Contains(t, stdout, `"code":"usage"`)
	assert.Contains(t, stdout, "--search")
	assert.Empty(t, fake.gotListOperation, "the RPC must not be called on a usage error")
}

func TestList_InvalidOperationIsUsageError(t *testing.T) {
	fake := &fakeNodeState{listResp: &statev1.ListNodesResponse{}}

	stdout, _, exit := runList(t, fake, []string{"--operation", "deploy"}, false)

	assert.Equal(t, 2, exit)
	assert.Contains(t, stdout, `"code":"usage"`)
	assert.Contains(t, stdout, "deploy")
}

func TestList_LimitOutOfRangeIsUsageError(t *testing.T) {
	for _, limit := range []string{"0", "-5", "201"} {
		t.Run("limit="+limit, func(t *testing.T) {
			fake := &fakeNodeState{listResp: &statev1.ListNodesResponse{}}
			stdout, _, exit := runList(t, fake, []string{"--limit", limit}, false)
			assert.Equal(t, 2, exit)
			assert.Contains(t, stdout, `"code":"usage"`)
			assert.Contains(t, stdout, "--limit")
		})
	}
}

func TestList_NegativeOffsetIsUsageError(t *testing.T) {
	fake := &fakeNodeState{listResp: &statev1.ListNodesResponse{}}

	stdout, _, exit := runList(t, fake, []string{"--offset", "-1"}, false)

	assert.Equal(t, 2, exit)
	assert.Contains(t, stdout, `"code":"usage"`)
	assert.Contains(t, stdout, "--offset")
}

func TestList_UnavailableMapsToExit5(t *testing.T) {
	fake := &fakeNodeState{listErr: status.Error(codes.Unavailable, "connection refused")}

	stdout, stderr, exit := runList(t, fake, nil, false)

	assert.Equal(t, 5, exit)
	assert.Empty(t, stderr)
	assert.Contains(t, stdout, `"code":"unavailable"`)
	assert.Contains(t, stdout, `"retryable":true`)
}

func TestList_HumanModeRendersRowsAndPageSummary(t *testing.T) {
	fake := &fakeNodeState{listResp: &statev1.ListNodesResponse{
		TotalCount: 42,
		Nodes: []*statev1.NodeSummary{
			{ServiceName: "finance", SchemaName: "analytics", TableName: "orders", RunCount: 12, SuccessRatePct: 92, AvgDurationSec: 34, P95DurationSec: 80, LastStatus: "succeeded", LastRunAt: "2026-09-18T10:00:00Z", Operation: "run"},
			{ServiceName: "finance", SchemaName: "analytics", TableName: "never_ran", RunCount: 0, SuccessRatePct: -1, AvgDurationSec: -1, P95DurationSec: -1, LastStatus: "pending", Operation: "run"},
		},
	}}

	stdout, stderr, exit := runList(t, fake, []string{"--limit", "2", "--offset", "10"}, true)

	require.Equal(t, 0, exit)
	assert.Empty(t, stdout, "human mode writes nothing to stdout")
	lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
	require.Len(t, lines, 4, "header + 2 rows + page summary, got: %q", stderr)
	assert.Equal(t, "NODE  OPERATION  RUNS  SUCCESS_PCT  AVG_SEC  P95_SEC  LAST_STATUS  LAST_RUN_AT", lines[0])
	assert.Equal(t, "finance.analytics.orders  run  12  92  34  80  succeeded  2026-09-18T10:00:00Z", lines[1])
	assert.Equal(t, "finance.analytics.never_ran  run  0  -  -  -  pending  -", lines[2])
	assert.Equal(t, "showing 11-12 of 42", lines[3])
}

func TestList_HumanModeEmptyPageSummary(t *testing.T) {
	fake := &fakeNodeState{listResp: &statev1.ListNodesResponse{TotalCount: 0}}

	_, stderr, exit := runList(t, fake, nil, true)

	require.Equal(t, 0, exit)
	assert.True(t, strings.HasSuffix(stderr, "showing 0 of 0\n"), "got: %q", stderr)
}
