package node

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
	orchestratorv1 "github.com/carolsimone/continuo/cli/proto/orchestrator/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func runUpstreamChanges(t *testing.T, fake client.OrchestratorClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewUpstreamChangesCommand(func(context.Context, string) (client.OrchestratorClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestUpstreamChanges_SuccessMapsAllFieldsWithDefaults(t *testing.T) {
	fake := &fakeNodeOrchestrator{upstreamResp: &orchestratorv1.GetUpstreamChangesResponse{Changes: []*orchestratorv1.UpstreamChange{{
		UniqueId: "analytics.customers",
		Depth:    2,
		Diff: &orchestratorv1.VersionDiff{
			UniqueId:      "analytics.customers",
			From:          &orchestratorv1.VersionView{VersionSeq: 1},
			To:            &orchestratorv1.VersionView{VersionSeq: 2},
			SourceChanged: true,
			Truncated:     true,
		},
	}}}}

	stdout, stderr, exit := runUpstreamChanges(t, fake, []string{"finance", "analytics", "orders"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.Equal(t, "analytics.orders", fake.gotUpstreamUniqueID)
	assert.Equal(t, int32(0), fake.gotUpstreamDepth) // 0 = server default (3 hops)
	assert.Equal(t, "", fake.gotUpstreamSince)

	var payload struct {
		Changes []map[string]any `json:"changes"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Changes, 1)
	c := payload.Changes[0]
	assert.Equal(t, "analytics.customers", c["unique_id"])
	assert.Equal(t, float64(2), c["depth"])
	diff := c["diff"].(map[string]any)
	assert.Equal(t, "analytics.customers", diff["unique_id"])
	assert.Equal(t, true, diff["truncated"])
}

func TestUpstreamChanges_DepthAndSinceFlagsAreForwarded(t *testing.T) {
	fake := &fakeNodeOrchestrator{upstreamResp: &orchestratorv1.GetUpstreamChangesResponse{}}
	_, _, exit := runUpstreamChanges(t, fake, []string{"finance", "analytics", "orders", "--depth", "5", "--since", "2026-01-01T00:00:00Z"}, false)
	assert.Equal(t, 0, exit)
	assert.Equal(t, int32(5), fake.gotUpstreamDepth)
	assert.Equal(t, "2026-01-01T00:00:00Z", fake.gotUpstreamSince)
}

func TestUpstreamChanges_EmptyResultIsEmptyArrayNotNull(t *testing.T) {
	fake := &fakeNodeOrchestrator{upstreamResp: &orchestratorv1.GetUpstreamChangesResponse{Changes: nil}}
	stdout, _, exit := runUpstreamChanges(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.Equal(t, 0, exit)
	assert.Contains(t, stdout, `"changes":[]`)
}

func TestUpstreamChanges_WrongArgCountExits2(t *testing.T) {
	fake := &fakeNodeOrchestrator{}
	stdout, _, exit := runUpstreamChanges(t, fake, []string{"finance", "analytics"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestUpstreamChanges_NotFoundExits3(t *testing.T) {
	fake := &fakeNodeOrchestrator{upstreamErr: status.Error(codes.NotFound, "node not found")}
	_, _, exit := runUpstreamChanges(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.Equal(t, 3, exit)
}

func TestUpstreamChanges_DepthOverServerCapExits2(t *testing.T) {
	// The server rejects depth > 10 as InvalidArgument; FromGRPC maps that to usage/exit 2.
	fake := &fakeNodeOrchestrator{upstreamErr: status.Error(codes.InvalidArgument, "depth must be <= 10")}
	_, _, exit := runUpstreamChanges(t, fake, []string{"finance", "analytics", "orders", "--depth", "20"}, false)
	assert.Equal(t, 2, exit)
}

func TestUpstreamChanges_UnavailableExits5(t *testing.T) {
	fake := &fakeNodeOrchestrator{upstreamErr: status.Error(codes.Unavailable, "server down")}
	_, _, exit := runUpstreamChanges(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.Equal(t, 5, exit)
}

func TestUpstreamChanges_FactoryErrorExits5(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second}
	cmd := NewUpstreamChangesCommand(func(context.Context, string) (client.OrchestratorClient, error) {
		return nil, status.Error(codes.Unavailable, "dial failed")
	}, cfg, &outBuf, &errBuf)
	cmd.SetArgs([]string{"finance", "analytics", "orders"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	var cliErr output.CLIError
	require.True(t, errors.As(err, &cliErr))
	assert.Equal(t, 5, cliErr.ExitCode())
}

func TestUpstreamChanges_HumanModeUsesStderr(t *testing.T) {
	fake := &fakeNodeOrchestrator{upstreamResp: &orchestratorv1.GetUpstreamChangesResponse{Changes: []*orchestratorv1.UpstreamChange{
		{UniqueId: "analytics.customers", Depth: 1, Diff: &orchestratorv1.VersionDiff{}},
	}}}
	stdout, stderr, exit := runUpstreamChanges(t, fake, []string{"finance", "analytics", "orders"}, true)
	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "analytics.customers")
}
