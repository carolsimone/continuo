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

func runCodeUnits(t *testing.T, fake client.OrchestratorClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewCodeUnitsCommand(func(context.Context, string) (client.OrchestratorClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestCodeUnits_UnitIDSelector_Success(t *testing.T) {
	fake := &fakeNodeOrchestrator{unitsResp: &orchestratorv1.GetCodeUnitVersionsResponse{Versions: []*orchestratorv1.UnitVersionView{{
		UnitId: "unit_abc", Checksum: "chk1", Source: "{% macro x() %}...{% endmacro %}",
		Repo: "org/repo", CommitSha: "sha1", ReleaseId: "rel_1", PromotedAt: "2026-07-01T00:00:00Z", IsCurrent: true,
	}}}}

	stdout, stderr, exit := runCodeUnits(t, fake, []string{"unit_abc"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.Equal(t, "unit_abc", fake.gotUnitsUnitID)
	assert.Equal(t, "", fake.gotUnitsUniqueID)
	assert.Equal(t, int32(20), fake.gotUnitsLimit)

	var payload struct {
		Versions []map[string]any `json:"versions"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Versions, 1)
	v := payload.Versions[0]
	assert.Equal(t, "unit_abc", v["unit_id"])
	assert.Equal(t, "chk1", v["checksum"])
	assert.Equal(t, "{% macro x() %}...{% endmacro %}", v["source"])
	assert.Equal(t, "org/repo", v["repo"])
	assert.Equal(t, "sha1", v["commit_sha"])
	assert.Equal(t, "rel_1", v["release_id"])
	assert.Equal(t, "2026-07-01T00:00:00Z", v["promoted_at"])
	assert.Equal(t, true, v["is_current"])
}

func TestCodeUnits_NodeSelector_Success(t *testing.T) {
	fake := &fakeNodeOrchestrator{unitsResp: &orchestratorv1.GetCodeUnitVersionsResponse{}}
	stdout, _, exit := runCodeUnits(t, fake, []string{"--service", "finance", "--schema", "analytics", "--table", "orders"}, false)
	assert.Equal(t, 0, exit)
	assert.Equal(t, "", fake.gotUnitsUnitID)
	assert.Equal(t, "analytics.orders", fake.gotUnitsUniqueID)
	assert.Contains(t, stdout, `"versions":[]`)
}

func TestCodeUnits_LimitFlagIsForwarded(t *testing.T) {
	fake := &fakeNodeOrchestrator{unitsResp: &orchestratorv1.GetCodeUnitVersionsResponse{}}
	_, _, exit := runCodeUnits(t, fake, []string{"unit_abc", "--limit", "5"}, false)
	assert.Equal(t, 0, exit)
	assert.Equal(t, int32(5), fake.gotUnitsLimit)
}

func TestCodeUnits_BothSelectorsGivenExits2(t *testing.T) {
	fake := &fakeNodeOrchestrator{}
	stdout, _, exit := runCodeUnits(t, fake, []string{"unit_abc", "--service", "finance", "--schema", "analytics", "--table", "orders"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestCodeUnits_NeitherSelectorGivenExits2(t *testing.T) {
	fake := &fakeNodeOrchestrator{}
	stdout, _, exit := runCodeUnits(t, fake, []string{}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestCodeUnits_PartialNodeSelectorExits2(t *testing.T) {
	fake := &fakeNodeOrchestrator{}
	stdout, _, exit := runCodeUnits(t, fake, []string{"--service", "finance"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestCodeUnits_TooManyPositionalArgsExits2(t *testing.T) {
	fake := &fakeNodeOrchestrator{}
	stdout, _, exit := runCodeUnits(t, fake, []string{"unit_abc", "extra"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestCodeUnits_NotFoundExits3(t *testing.T) {
	fake := &fakeNodeOrchestrator{unitsErr: status.Error(codes.NotFound, "unit not found")}
	_, _, exit := runCodeUnits(t, fake, []string{"unit_missing"}, false)
	assert.Equal(t, 3, exit)
}

func TestCodeUnits_UnavailableExits5(t *testing.T) {
	fake := &fakeNodeOrchestrator{unitsErr: status.Error(codes.Unavailable, "server down")}
	_, _, exit := runCodeUnits(t, fake, []string{"unit_abc"}, false)
	assert.Equal(t, 5, exit)
}

func TestCodeUnits_FactoryErrorExits5(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second}
	cmd := NewCodeUnitsCommand(func(context.Context, string) (client.OrchestratorClient, error) {
		return nil, status.Error(codes.Unavailable, "dial failed")
	}, cfg, &outBuf, &errBuf)
	cmd.SetArgs([]string{"unit_abc"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	var cliErr output.CLIError
	require.True(t, errors.As(err, &cliErr))
	assert.Equal(t, 5, cliErr.ExitCode())
}

func TestCodeUnits_HumanModeUsesStderr(t *testing.T) {
	fake := &fakeNodeOrchestrator{unitsResp: &orchestratorv1.GetCodeUnitVersionsResponse{Versions: []*orchestratorv1.UnitVersionView{
		{UnitId: "unit_abc", Checksum: "chk1"},
	}}}
	stdout, stderr, exit := runCodeUnits(t, fake, []string{"unit_abc"}, true)
	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "unit_abc")
}
