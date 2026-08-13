package precedents

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

// fakePrecedentsOrchestrator is the client.OrchestratorClient fake for this
// package's tests. Only GetPrecedents returns data; every other method
// panics, since no precedents command path calls them.
type fakePrecedentsOrchestrator struct {
	resp *orchestratorv1.GetPrecedentsResponse
	err  error

	called         bool
	gotSignature   string
	gotCategory    string
	gotReason      string
	gotLimit       int32
	gotIncludeCode bool
}

func (f *fakePrecedentsOrchestrator) GetScheduleGraph(context.Context, string) (*orchestratorv1.GetScheduleGraphResponse, error) {
	panic("GetScheduleGraph should not be called in precedents tests")
}

func (f *fakePrecedentsOrchestrator) GetNodeVersions(context.Context, string, int32, bool) (*orchestratorv1.GetNodeVersionsResponse, error) {
	panic("GetNodeVersions should not be called in precedents tests")
}

func (f *fakePrecedentsOrchestrator) GetNodeVersionDiff(context.Context, string, int64, int64) (*orchestratorv1.GetNodeVersionDiffResponse, error) {
	panic("GetNodeVersionDiff should not be called in precedents tests")
}

func (f *fakePrecedentsOrchestrator) GetUpstreamChanges(context.Context, string, int32, string) (*orchestratorv1.GetUpstreamChangesResponse, error) {
	panic("GetUpstreamChanges should not be called in precedents tests")
}

func (f *fakePrecedentsOrchestrator) GetCodeUnitVersions(context.Context, string, string, int32) (*orchestratorv1.GetCodeUnitVersionsResponse, error) {
	panic("GetCodeUnitVersions should not be called in precedents tests")
}

func (f *fakePrecedentsOrchestrator) GetNodeRunHistory(context.Context, string, int32, string) (*orchestratorv1.GetNodeRunHistoryResponse, error) {
	panic("GetNodeRunHistory should not be called in precedents tests")
}

func (f *fakePrecedentsOrchestrator) GetPrecedents(_ context.Context, signature, category, reason string, limit int32, includeCode bool) (*orchestratorv1.GetPrecedentsResponse, error) {
	f.called = true
	f.gotSignature, f.gotCategory, f.gotReason = signature, category, reason
	f.gotLimit, f.gotIncludeCode = limit, includeCode
	return f.resp, f.err
}

func (f *fakePrecedentsOrchestrator) Close() error { return nil }

// runPrecedents invokes the precedents command end-to-end with the provided
// fake client. It captures stdout/stderr and returns the exit code.
func runPrecedents(t *testing.T, fake client.OrchestratorClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := newPrecedentsCommand(func(context.Context, string) (client.OrchestratorClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
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

func TestPrecedents_EmitsMatchesAsJSON(t *testing.T) {
	fake := &fakePrecedentsOrchestrator{resp: &orchestratorv1.GetPrecedentsResponse{
		Precedents: []*orchestratorv1.Precedent{
			{
				ReleaseId:    "rel-1",
				NodeId:       "analytics.orders",
				Stage:        "test",
				Category:     "logic",
				Reason:       "logic:missing_object",
				ErrorExcerpt: `relation "orders_stg" does not exist`,
				RejectedAt:   "2026-08-01T00:00:00Z",
				Resolved:     true,
				ResolvingVersion: &orchestratorv1.VersionView{
					VersionSeq:  4,
					ContentHash: "c4",
					ReleaseId:   "rel-2",
					Repo:        "org/repo",
					CommitSha:   "abc123",
					PromotedAt:  "2026-08-02T00:00:00Z",
				},
				ResolutionDiff:          "--- a\n+++ b\n",
				ResolutionDiffTruncated: false,
				Proposals: []*orchestratorv1.PrecedentProposal{
					{ProposalId: "p1", PrUrl: "https://github.com/org/repo/pull/1", PrNumber: 1, PrState: "merged"},
				},
			},
		},
	}}

	stdout, stderr, exit := runPrecedents(t, fake, []string{"--signature", "s1"}, false)

	require.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.True(t, fake.called)
	assert.Equal(t, "s1", fake.gotSignature)
	assert.Equal(t, "", fake.gotCategory)
	assert.Equal(t, "", fake.gotReason)

	var payload struct {
		Precedents []map[string]any `json:"precedents"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Precedents, 1)
	p := payload.Precedents[0]
	assert.Equal(t, "rel-1", p["release_id"])
	assert.Equal(t, "analytics.orders", p["node_id"])
	assert.Equal(t, "test", p["stage"])
	assert.Equal(t, "logic", p["category"])
	assert.Equal(t, "logic:missing_object", p["reason"])
	assert.Equal(t, `relation "orders_stg" does not exist`, p["error_excerpt"])
	assert.Equal(t, true, p["resolved"])

	rv, ok := p["resolving_version"].(map[string]any)
	require.True(t, ok, "resolving_version must be present when resolved is true")
	assert.Equal(t, float64(4), rv["version_seq"])
	assert.Equal(t, "c4", rv["content_hash"])
	assert.Equal(t, "rel-2", rv["release_id"])
	assert.Equal(t, "org/repo", rv["repo"])
	assert.Equal(t, "abc123", rv["commit_sha"])
	assert.Equal(t, "2026-08-02T00:00:00Z", rv["promoted_at"])

	assert.Equal(t, "--- a\n+++ b\n", p["resolution_diff"])

	proposals, ok := p["proposals"].([]any)
	require.True(t, ok)
	require.Len(t, proposals, 1)
	proposal := proposals[0].(map[string]any)
	assert.Equal(t, "p1", proposal["proposal_id"])
	assert.Equal(t, "https://github.com/org/repo/pull/1", proposal["pr_url"])
	assert.Equal(t, float64(1), proposal["pr_number"])
	assert.Equal(t, "merged", proposal["pr_state"])
}

func TestPrecedents_RequiresSelector(t *testing.T) {
	fake := &fakePrecedentsOrchestrator{}
	stdout, _, exit := runPrecedents(t, fake, []string{}, false)
	assert.Equal(t, 2, exit)
	assert.False(t, fake.called, "no RPC call should happen when the selector is missing")
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)

	fakeCategoryOnly := &fakePrecedentsOrchestrator{}
	stdout2, _, exit2 := runPrecedents(t, fakeCategoryOnly, []string{"--category", "logic"}, false)
	assert.Equal(t, 2, exit2)
	assert.False(t, fakeCategoryOnly.called, "--category alone must not dial the RPC")
	var env2 map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout2), &env2))
	assert.Equal(t, output.CodeUsage, env2["error"].Code)
}

func TestPrecedents_CategoryReasonSelector(t *testing.T) {
	fake := &fakePrecedentsOrchestrator{resp: &orchestratorv1.GetPrecedentsResponse{}}
	_, _, exit := runPrecedents(t, fake, []string{"--category", "logic", "--reason", "logic:missing_object"}, false)
	assert.Equal(t, 0, exit)
	require.True(t, fake.called)
	assert.Equal(t, "", fake.gotSignature)
	assert.Equal(t, "logic", fake.gotCategory)
	assert.Equal(t, "logic:missing_object", fake.gotReason)
}

func TestPrecedents_UnavailableExit5(t *testing.T) {
	fake := &fakePrecedentsOrchestrator{err: status.Error(codes.Unavailable, "server down")}
	_, _, exit := runPrecedents(t, fake, []string{"--signature", "s1"}, false)
	assert.Equal(t, 5, exit)
}

func TestPrecedents_HumanModeWritesStderrOnly(t *testing.T) {
	fake := &fakePrecedentsOrchestrator{resp: &orchestratorv1.GetPrecedentsResponse{
		Precedents: []*orchestratorv1.Precedent{
			{
				NodeId:   "analytics.orders",
				Reason:   "logic:missing_object",
				Resolved: true,
				Proposals: []*orchestratorv1.PrecedentProposal{
					{PrUrl: "https://github.com/org/repo/pull/1"},
				},
			},
		},
	}}
	stdout, stderr, exit := runPrecedents(t, fake, []string{"--signature", "s1"}, true)
	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "analytics.orders")
	assert.Contains(t, stderr, "logic:missing_object")
	assert.Contains(t, stderr, "https://github.com/org/repo/pull/1")
}
