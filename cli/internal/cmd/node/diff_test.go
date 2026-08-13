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

func runDiff(t *testing.T, fake client.OrchestratorClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewDiffCommand(func(context.Context, string) (client.OrchestratorClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestDiff_SuccessMapsAllFields(t *testing.T) {
	fake := &fakeNodeOrchestrator{diffResp: &orchestratorv1.GetNodeVersionDiffResponse{Diff: &orchestratorv1.VersionDiff{
		UniqueId:          "analytics.orders",
		From:              &orchestratorv1.VersionView{VersionSeq: 1, ContentHash: "c1"},
		To:                &orchestratorv1.VersionView{VersionSeq: 3, ContentHash: "c3"},
		RawCodeDiff:       "--- a\n+++ b\n",
		ConfigDiff:        "",
		SourceChanged:     true,
		SharedCodeChanged: false,
		ConfigChanged:     false,
		Truncated:         false,
	}}}

	stdout, stderr, exit := runDiff(t, fake, []string{"finance", "analytics", "orders", "--from", "1", "--to", "3"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.Equal(t, "analytics.orders", fake.gotDiffUniqueID)
	assert.Equal(t, int64(1), fake.gotDiffFromSeq)
	assert.Equal(t, int64(3), fake.gotDiffToSeq)

	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	assert.Equal(t, "analytics.orders", payload["unique_id"])
	from := payload["from"].(map[string]any)
	assert.Equal(t, float64(1), from["version_seq"])
	to := payload["to"].(map[string]any)
	assert.Equal(t, float64(3), to["version_seq"])
	assert.Equal(t, "--- a\n+++ b\n", payload["raw_code_diff"])
	assert.Equal(t, true, payload["source_changed"])
	assert.Equal(t, false, payload["shared_code_changed"])
	assert.Equal(t, false, payload["config_changed"])
	assert.Equal(t, false, payload["truncated"])
}

func TestDiff_MissingFromOrToFlagExits2(t *testing.T) {
	fake := &fakeNodeOrchestrator{}
	stdout, _, exit := runDiff(t, fake, []string{"finance", "analytics", "orders", "--to", "3"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestDiff_WrongArgCountExits2(t *testing.T) {
	fake := &fakeNodeOrchestrator{}
	stdout, _, exit := runDiff(t, fake, []string{"finance", "analytics", "--from", "1", "--to", "2"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestDiff_NotFoundExits3(t *testing.T) {
	fake := &fakeNodeOrchestrator{diffErr: status.Error(codes.NotFound, "version not found")}
	_, _, exit := runDiff(t, fake, []string{"finance", "analytics", "orders", "--from", "1", "--to", "2"}, false)
	assert.Equal(t, 3, exit)
}

func TestDiff_UnavailableExits5(t *testing.T) {
	fake := &fakeNodeOrchestrator{diffErr: status.Error(codes.Unavailable, "server down")}
	_, _, exit := runDiff(t, fake, []string{"finance", "analytics", "orders", "--from", "1", "--to", "2"}, false)
	assert.Equal(t, 5, exit)
}

func TestDiff_FactoryErrorExits5(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second}
	cmd := NewDiffCommand(func(context.Context, string) (client.OrchestratorClient, error) {
		return nil, status.Error(codes.Unavailable, "dial failed")
	}, cfg, &outBuf, &errBuf)
	cmd.SetArgs([]string{"finance", "analytics", "orders", "--from", "1", "--to", "2"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	var cliErr output.CLIError
	require.True(t, errors.As(err, &cliErr))
	assert.Equal(t, 5, cliErr.ExitCode())
}

func TestDiff_HumanModeUsesStderr(t *testing.T) {
	fake := &fakeNodeOrchestrator{diffResp: &orchestratorv1.GetNodeVersionDiffResponse{Diff: &orchestratorv1.VersionDiff{
		UniqueId:      "analytics.orders",
		From:          &orchestratorv1.VersionView{VersionSeq: 1},
		To:            &orchestratorv1.VersionView{VersionSeq: 3},
		SourceChanged: true,
	}}}
	stdout, stderr, exit := runDiff(t, fake, []string{"finance", "analytics", "orders", "--from", "1", "--to", "3"}, true)
	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "analytics.orders")
}
