package node

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	catalogv1 "github.com/carolsimone/continuo/cli/proto/catalog/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fullContractHistory is a node's history, newest first: a revert (version 3
// returns to the contract of version 1) above the change it undid.
func fullContractHistory() *catalogv1.ListNodeDataContractVersionsResponse {
	return &catalogv1.ListNodeDataContractVersionsResponse{Versions: []*catalogv1.ContractVersionView{
		{
			UniqueId: "analytics.py_daily_kpis", Version: 3, ContractHash: "sha256:aaa", ReleaseId: "rel-3",
			PromotedAt: "2026-11-14T09:30:00Z", ContentHash: "sha256:ccc", IsCurrent: true, SameAsVersion: 1,
			ContinuoContracts: "0.2.0",
		},
		{
			UniqueId: "analytics.py_daily_kpis", Version: 2, ContractHash: "sha256:bbb", ReleaseId: "rel-2",
			PromotedAt: "2026-11-13T09:30:00Z", ContentHash: "sha256:ddd", ContinuoContracts: "0.2.0",
		},
	}}
}

func TestDataContractHistory_MapsEveryFieldNewestFirstAndSendsZeroForNoLimit(t *testing.T) {
	fake := &fakeNodeCatalog{contractVersionsResp: fullContractHistory()}

	stdout, stderr, exit := runContractCommand(t, NewDataContractHistoryCommand, fake, []string{"analytics.py_daily_kpis"}, false)

	require.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.Equal(t, "analytics.py_daily_kpis", fake.gotContractVersionsUniqueID)
	assert.Equal(t, int32(0), fake.gotContractVersionsLimit, "no limit lists every version")

	var payload struct {
		Versions []map[string]any `json:"versions"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Versions, 2)
	first := payload.Versions[0]
	assert.Equal(t, float64(3), first["version"], "the server's newest-first order is kept")
	assert.Equal(t, "analytics.py_daily_kpis", first["unique_id"])
	assert.Equal(t, "sha256:aaa", first["contract_hash"])
	assert.Equal(t, "rel-3", first["release_id"])
	assert.Equal(t, "2026-11-14T09:30:00Z", first["promoted_at"])
	assert.Equal(t, "sha256:ccc", first["content_hash"])
	assert.Equal(t, true, first["is_current"])
	assert.Equal(t, float64(1), first["same_as_version"])
	assert.Equal(t, "0.2.0", first["continuo_contracts"])
	assert.Equal(t, float64(2), payload.Versions[1]["version"])
	assert.NotContains(t, payload.Versions[1], "is_current", "a version that is not current omits it, as node versions does")
	assert.NotContains(t, payload.Versions[1], "same_as_version")
}

func TestDataContractHistory_NeverCarriesTheDocument(t *testing.T) {
	resp := fullContractHistory()
	resp.Versions[0].DocumentYaml = "kind: DataContract\n"
	fake := &fakeNodeCatalog{contractVersionsResp: resp}

	stdout, _, exit := runContractCommand(t, NewDataContractHistoryCommand, fake, []string{"analytics.py_daily_kpis"}, false)

	require.Equal(t, 0, exit)
	assert.NotContains(t, stdout, "document_yaml")
	assert.NotContains(t, stdout, "DataContract")
}

func TestDataContractHistory_ForwardsTheLimitPositional(t *testing.T) {
	fake := &fakeNodeCatalog{contractVersionsResp: &catalogv1.ListNodeDataContractVersionsResponse{}}

	_, _, exit := runContractCommand(t, NewDataContractHistoryCommand, fake, []string{"analytics.py_daily_kpis", "5"}, false)

	assert.Equal(t, 0, exit)
	assert.Equal(t, int32(5), fake.gotContractVersionsLimit)
}

func TestDataContractHistory_ALimitOfZeroMeansEveryVersion(t *testing.T) {
	fake := &fakeNodeCatalog{contractVersionsResp: &catalogv1.ListNodeDataContractVersionsResponse{}}

	_, _, exit := runContractCommand(t, NewDataContractHistoryCommand, fake, []string{"analytics.py_daily_kpis", "0"}, false)

	assert.Equal(t, 0, exit)
	assert.Equal(t, int32(0), fake.gotContractVersionsLimit)
}

func TestDataContractHistory_ANodeWithNoHistoryIsAnEmptyArrayNotNull(t *testing.T) {
	fake := &fakeNodeCatalog{contractVersionsResp: &catalogv1.ListNodeDataContractVersionsResponse{}}

	stdout, _, exit := runContractCommand(t, NewDataContractHistoryCommand, fake, []string{"analytics.orders"}, false)

	assert.Equal(t, 0, exit)
	assert.JSONEq(t, `{"versions":[]}`, stdout)
}

func TestDataContractHistory_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeNodeCatalog{contractVersionsResp: fullContractHistory()}
	stdout, _, exit := runContractCommand(t, NewDataContractHistoryCommand, fake, []string{"analytics.py_daily_kpis"}, false)
	require.Equal(t, 0, exit)

	assertEmitsItsSchema(t, NewDataContractHistoryCommand(nil, &config.Config{}, io.Discard, io.Discard), stdout)
}

func TestDataContractHistory_UsageErrorsExit2WithoutCallingTheServer(t *testing.T) {
	cases := map[string][]string{
		"no arguments":          {},
		"too many arguments":    {"analytics.orders", "5", "extra"},
		"a limit that is text":  {"analytics.orders", "five"},
		"a fractional limit":    {"analytics.orders", "2.5"},
		"a limit beyond int32":  {"analytics.orders", "4294967296"},
		"a limit of only blank": {"analytics.orders", ""},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			fake := &fakeNodeCatalog{}
			stdout, _, exit := runContractCommand(t, NewDataContractHistoryCommand, fake, args, false)
			assert.Equal(t, 2, exit)
			var env map[string]output.CLIError
			require.NoError(t, json.Unmarshal([]byte(stdout), &env))
			assert.Equal(t, output.CodeUsage, env["error"].Code)
			assert.Zero(t, fake.contractVersionsCalls)
		})
	}
}

func TestDataContractHistory_ARefusedLimitFromTheServerExits2(t *testing.T) {
	fake := &fakeNodeCatalog{contractVersionsErr: status.Error(codes.InvalidArgument, "limit must be 0 (every version) or more")}

	_, _, exit := runContractCommand(t, NewDataContractHistoryCommand, fake, []string{"analytics.orders", "1"}, false)

	assert.Equal(t, 2, exit)
}

func TestDataContractHistory_UnavailableExits5AndInternalExits6(t *testing.T) {
	_, _, exit := runContractCommand(t, NewDataContractHistoryCommand, &fakeNodeCatalog{contractVersionsErr: status.Error(codes.Unavailable, "down")}, []string{"analytics.orders"}, false)
	assert.Equal(t, 5, exit)
	_, _, exit = runContractCommand(t, NewDataContractHistoryCommand, &fakeNodeCatalog{contractVersionsErr: status.Error(codes.Internal, "boom")}, []string{"analytics.orders"}, false)
	assert.Equal(t, 6, exit)
}

func TestDataContractHistory_FactoryErrorExits5(t *testing.T) {
	cmd := NewDataContractHistoryCommand(func(context.Context, string) (client.CatalogClient, error) {
		return nil, status.Error(codes.Unavailable, "dial failed")
	}, &config.Config{Timeout: 2 * time.Second}, io.Discard, io.Discard)
	cmd.SetArgs([]string{"analytics.orders"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	var cliErr output.CLIError
	require.ErrorAs(t, cmd.Execute(), &cliErr)
	assert.Equal(t, 5, cliErr.ExitCode())
}

func TestDataContractHistory_HumanModeWritesOneLinePerVersionToStderr(t *testing.T) {
	fake := &fakeNodeCatalog{contractVersionsResp: fullContractHistory()}

	stdout, stderr, exit := runContractCommand(t, NewDataContractHistoryCommand, fake, []string{"analytics.py_daily_kpis"}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "3  sha256:aaa  2026-11-14T09:30:00Z  [current]  (same as 1)")
	assert.Contains(t, stderr, "2  sha256:bbb  2026-11-13T09:30:00Z\n")
}

func TestDataContractHistory_IsDocumentedAsAReadOnlyCommandThatTheChatSetsNoFlagsOn(t *testing.T) {
	cmd := NewDataContractHistoryCommand(nil, &config.Config{}, io.Discard, io.Discard)

	assert.NotEmpty(t, cmd.Short)
	assert.NotEmpty(t, cmd.Long)
	assert.NotEmpty(t, cmd.Example)
	assert.JSONEq(t, `[0,2,5,6]`, cmd.Annotations["exit_codes"], "the catalog answers an empty list, never not_found, so exit 3 is not listed")
	assert.NotContains(t, cmd.Long, "not_found  (exit 3)")
	assert.NotContains(t, cmd.Annotations, "mutating")
	assert.NotContains(t, cmd.Annotations, "chat_flags")
	assert.Zero(t, cmd.LocalFlags().NFlag())
}
