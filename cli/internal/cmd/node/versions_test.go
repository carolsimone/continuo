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

func runVersions(t *testing.T, fake client.OrchestratorClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewVersionsCommand(func(context.Context, string) (client.OrchestratorClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestVersions_SuccessMapsAllFieldsAndDefaultsLimitTo20(t *testing.T) {
	fake := &fakeNodeOrchestrator{versionsResp: &orchestratorv1.GetNodeVersionsResponse{Versions: []*orchestratorv1.VersionView{{
		UniqueId: "analytics.orders", VersionSeq: 3, ContentHash: "c1", SourceHash: "s1",
		SharedCodeHash: "sc1", ConfigHash: "cfg1", Runtime: "dbt", RawCode: "select 1",
		CompiledCode: "select 1 from x", CompiledTruncated: false, ConfigJson: `{"materialized":"table"}`,
		Repo: "org/repo", CommitSha: "abc123", ReleaseId: "rel_1", PromotedAt: "2026-07-01T00:00:00Z",
		Healed: false, Backfilled: false, IsCurrent: true,
	}}}}

	stdout, stderr, exit := runVersions(t, fake, []string{"finance", "analytics", "orders"}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.Equal(t, "analytics.orders", fake.gotVersionsUniqueID)
	assert.Equal(t, int32(20), fake.gotVersionsLimit)

	var payload struct {
		Versions []map[string]any `json:"versions"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Versions, 1)
	v := payload.Versions[0]
	assert.Equal(t, "analytics.orders", v["unique_id"])
	assert.Equal(t, float64(3), v["version_seq"])
	assert.Equal(t, "c1", v["content_hash"])
	assert.Equal(t, "s1", v["source_hash"])
	assert.Equal(t, "sc1", v["shared_code_hash"])
	assert.Equal(t, "cfg1", v["config_hash"])
	assert.Equal(t, "dbt", v["runtime"])
	assert.Equal(t, "select 1", v["raw_code"])
	assert.Equal(t, "select 1 from x", v["compiled_code"])
	assert.Equal(t, `{"materialized":"table"}`, v["config_json"])
	assert.Equal(t, "org/repo", v["repo"])
	assert.Equal(t, "abc123", v["commit_sha"])
	assert.Equal(t, "rel_1", v["release_id"])
	assert.Equal(t, "2026-07-01T00:00:00Z", v["promoted_at"])
	assert.Equal(t, true, v["is_current"])
}

func TestVersions_LimitFlagIsForwarded(t *testing.T) {
	fake := &fakeNodeOrchestrator{versionsResp: &orchestratorv1.GetNodeVersionsResponse{}}
	_, _, exit := runVersions(t, fake, []string{"finance", "analytics", "orders", "--limit", "50"}, false)
	assert.Equal(t, 0, exit)
	assert.Equal(t, int32(50), fake.gotVersionsLimit)
}

func TestVersions_EmptyResultIsEmptyArrayNotNull(t *testing.T) {
	fake := &fakeNodeOrchestrator{versionsResp: &orchestratorv1.GetNodeVersionsResponse{Versions: nil}}
	stdout, _, exit := runVersions(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.Equal(t, 0, exit)
	assert.Contains(t, stdout, `"versions":[]`)
}

func TestVersions_WrongArgCountExits2(t *testing.T) {
	fake := &fakeNodeOrchestrator{}
	stdout, _, exit := runVersions(t, fake, []string{"finance", "analytics"}, false)
	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

func TestVersions_NotFoundExits3(t *testing.T) {
	fake := &fakeNodeOrchestrator{versionsErr: status.Error(codes.NotFound, "node not found")}
	_, _, exit := runVersions(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.Equal(t, 3, exit)
}

func TestVersions_UnavailableExits5(t *testing.T) {
	fake := &fakeNodeOrchestrator{versionsErr: status.Error(codes.Unavailable, "server down")}
	_, _, exit := runVersions(t, fake, []string{"finance", "analytics", "orders"}, false)
	assert.Equal(t, 5, exit)
}

func TestVersions_FactoryErrorExits5(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second}
	cmd := NewVersionsCommand(func(context.Context, string) (client.OrchestratorClient, error) {
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

func TestVersions_HumanModeUsesStderr(t *testing.T) {
	fake := &fakeNodeOrchestrator{versionsResp: &orchestratorv1.GetNodeVersionsResponse{Versions: []*orchestratorv1.VersionView{{
		VersionSeq: 3, ContentHash: "c1", IsCurrent: true,
	}}}}
	stdout, stderr, exit := runVersions(t, fake, []string{"finance", "analytics", "orders"}, true)
	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "3")
	assert.Contains(t, stderr, "c1")
}
