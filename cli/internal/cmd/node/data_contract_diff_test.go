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

// sampleContractChanges is a changed value, an added field and a removed one.
func sampleContractChanges() *catalogv1.GetNodeDataContractDiffResponse {
	return &catalogv1.GetNodeDataContractDiffResponse{Changes: []*catalogv1.ContractFieldChange{
		{Path: "description.purpose", BeforeJson: `"Daily KPIs"`, AfterJson: `"Daily KPIs by region"`},
		{Path: "schema[0].properties[2]", AfterJson: `{"name":"region","physicalType":"text"}`},
		{Path: "tags", BeforeJson: `["kpi"]`},
	}}
}

func TestDataContractDiff_MapsEveryChangeAndForwardsTheVersions(t *testing.T) {
	fake := &fakeNodeCatalog{contractDiffResp: sampleContractChanges()}

	stdout, stderr, exit := runContractCommand(t, NewDataContractDiffCommand, fake, []string{"analytics.py_daily_kpis", "1", "3"}, false)

	require.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.Equal(t, "analytics.py_daily_kpis", fake.gotContractDiffUniqueID)
	assert.Equal(t, int64(1), fake.gotContractDiffFrom)
	assert.Equal(t, int64(3), fake.gotContractDiffTo)

	var payload struct {
		Changes []map[string]any `json:"changes"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Changes, 3)
	assert.Equal(t, "description.purpose", payload.Changes[0]["path"])
	assert.Equal(t, `"Daily KPIs"`, payload.Changes[0]["before_json"])
	assert.Equal(t, `"Daily KPIs by region"`, payload.Changes[0]["after_json"])
	assert.Equal(t, "", payload.Changes[1]["before_json"], "an added path has an empty before_json, and the key stays")
	assert.Equal(t, "", payload.Changes[2]["after_json"], "a removed path has an empty after_json, and the key stays")
}

func TestDataContractDiff_TwoEqualVersionsAreAnEmptyArrayNotNull(t *testing.T) {
	fake := &fakeNodeCatalog{contractDiffResp: &catalogv1.GetNodeDataContractDiffResponse{}}

	stdout, _, exit := runContractCommand(t, NewDataContractDiffCommand, fake, []string{"analytics.orders", "2", "2"}, false)

	assert.Equal(t, 0, exit)
	assert.JSONEq(t, `{"changes":[]}`, stdout)
}

func TestDataContractDiff_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeNodeCatalog{contractDiffResp: sampleContractChanges()}
	stdout, _, exit := runContractCommand(t, NewDataContractDiffCommand, fake, []string{"analytics.py_daily_kpis", "1", "3"}, false)
	require.Equal(t, 0, exit)

	assertEmitsItsSchema(t, NewDataContractDiffCommand(nil, &config.Config{}, io.Discard, io.Discard), stdout)
}

func TestDataContractDiff_UsageErrorsExit2WithoutCallingTheServer(t *testing.T) {
	cases := map[string][]string{
		"no arguments":             {},
		"a unique_id alone":        {"analytics.orders"},
		"a missing to version":     {"analytics.orders", "1"},
		"too many arguments":       {"analytics.orders", "1", "2", "3"},
		"a from version of text":   {"analytics.orders", "one", "2"},
		"a to version of text":     {"analytics.orders", "1", "two"},
		"a fractional version":     {"analytics.orders", "1.5", "2"},
		"an empty version":         {"analytics.orders", "", "2"},
		"a version beyond int64":   {"analytics.orders", "1", "9223372036854775808"},
		"both versions are blanks": {"analytics.orders", " ", " "},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			fake := &fakeNodeCatalog{}
			stdout, _, exit := runContractCommand(t, NewDataContractDiffCommand, fake, args, false)
			assert.Equal(t, 2, exit)
			var env map[string]output.CLIError
			require.NoError(t, json.Unmarshal([]byte(stdout), &env))
			assert.Equal(t, output.CodeUsage, env["error"].Code)
			assert.Zero(t, fake.contractDiffCalls)
		})
	}
}

func TestDataContractDiff_AVersionBelowOneIsRefusedByTheServerWithExit2(t *testing.T) {
	fake := &fakeNodeCatalog{contractDiffErr: status.Error(codes.InvalidArgument, "from_version and to_version must be 1 or more")}

	stdout, _, exit := runContractCommand(t, NewDataContractDiffCommand, fake, []string{"analytics.orders", "0", "2"}, false)

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
	assert.Contains(t, env["error"].Message, "1 or more")
}

func TestDataContractDiff_AVersionTheNodeLacksExits3(t *testing.T) {
	fake := &fakeNodeCatalog{contractDiffErr: status.Error(codes.NotFound, `node "analytics.orders" has no data contract version 1 or 9`)}

	stdout, _, exit := runContractCommand(t, NewDataContractDiffCommand, fake, []string{"analytics.orders", "1", "9"}, false)

	assert.Equal(t, 3, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeNotFound, env["error"].Code)
}

func TestDataContractDiff_UnavailableExits5AndInternalExits6(t *testing.T) {
	_, _, exit := runContractCommand(t, NewDataContractDiffCommand, &fakeNodeCatalog{contractDiffErr: status.Error(codes.Unavailable, "down")}, []string{"analytics.orders", "1", "2"}, false)
	assert.Equal(t, 5, exit)
	_, _, exit = runContractCommand(t, NewDataContractDiffCommand, &fakeNodeCatalog{contractDiffErr: status.Error(codes.Internal, "boom")}, []string{"analytics.orders", "1", "2"}, false)
	assert.Equal(t, 6, exit)
}

func TestDataContractDiff_FactoryErrorExits5(t *testing.T) {
	cmd := NewDataContractDiffCommand(func(context.Context, string) (client.CatalogClient, error) {
		return nil, status.Error(codes.Unavailable, "dial failed")
	}, &config.Config{Timeout: 2 * time.Second}, io.Discard, io.Discard)
	cmd.SetArgs([]string{"analytics.orders", "1", "2"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	var cliErr output.CLIError
	require.ErrorAs(t, cmd.Execute(), &cliErr)
	assert.Equal(t, 5, cliErr.ExitCode())
}

func TestDataContractDiff_HumanModeWritesOneLinePerChangeToStderr(t *testing.T) {
	fake := &fakeNodeCatalog{contractDiffResp: sampleContractChanges()}

	stdout, stderr, exit := runContractCommand(t, NewDataContractDiffCommand, fake, []string{"analytics.py_daily_kpis", "1", "3"}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, `description.purpose: "Daily KPIs" -> "Daily KPIs by region"`)
	assert.Contains(t, stderr, `schema[0].properties[2]: (absent) -> {"name":"region","physicalType":"text"}`)
	assert.Contains(t, stderr, `tags: ["kpi"] -> (absent)`)
}

func TestDataContractDiff_IsDocumentedAsAReadOnlyCommandThatTheChatSetsNoFlagsOn(t *testing.T) {
	cmd := NewDataContractDiffCommand(nil, &config.Config{}, io.Discard, io.Discard)

	assert.NotEmpty(t, cmd.Short)
	assert.NotEmpty(t, cmd.Long)
	assert.NotEmpty(t, cmd.Example)
	assert.JSONEq(t, `[0,2,3,5,6]`, cmd.Annotations["exit_codes"])
	assert.NotContains(t, cmd.Annotations, "mutating")
	assert.NotContains(t, cmd.Annotations, "chat_flags")
	assert.Zero(t, cmd.LocalFlags().NFlag())
}
