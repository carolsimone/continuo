package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	catalogv1 "github.com/carolsimone/continuo/cli/proto/catalog/v1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// runContractCommand executes one contract command built by newCmd against fake
// and returns what it wrote and its exit code.
func runContractCommand(t *testing.T, newCmd func(CatalogClientFactory, *config.Config, io.Writer, io.Writer) *cobra.Command, fake client.CatalogClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := newCmd(func(context.Context, string) (client.CatalogClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
	cmd.SetArgs(args)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
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

// assertEmitsItsSchema holds a command's output_schema annotation to the JSON
// it printed: the same keys at every level. A one-element schema array stands
// for every element: each element's keys are among the schema's, and between
// them the elements carry every one, so the fixture needs one row with each
// optional field.
func assertEmitsItsSchema(t *testing.T, cmd *cobra.Command, stdout string) {
	t.Helper()
	var schema, emitted any
	require.NoError(t, json.Unmarshal([]byte(cmd.Annotations["output_schema"]), &schema), "output_schema is JSON")
	require.NoError(t, json.Unmarshal([]byte(stdout), &emitted), "stdout is JSON")
	assertSameKeys(t, "$", schema, emitted)
}

func assertSameKeys(t *testing.T, path string, schema, emitted any) {
	t.Helper()
	switch sv := schema.(type) {
	case map[string]any:
		ev, ok := emitted.(map[string]any)
		require.True(t, ok, "%s: the schema has an object, the output has %T", path, emitted)
		for k := range sv {
			assert.Contains(t, ev, k, "%s: a schema key is missing from the output", path)
		}
		for k := range ev {
			assert.Contains(t, sv, k, "%s: an output key is missing from the schema", path)
		}
		for k, child := range sv {
			if ec, ok := ev[k]; ok {
				assertSameKeys(t, path+"."+k, child, ec)
			}
		}
	case []any:
		ev, ok := emitted.([]any)
		require.True(t, ok, "%s: the schema has an array, the output has %T", path, emitted)
		require.Len(t, sv, 1, "%s: a schema array describes its elements with one example", path)
		require.NotEmpty(t, ev, "%s: the fixture must carry an element", path)
		union := map[string]any{}
		for _, e := range ev {
			obj, ok := e.(map[string]any)
			require.True(t, ok, "%s: an element is %T, not an object", path, e)
			for k, v := range obj {
				assert.Contains(t, sv[0], k, "%s[]: an output key is missing from the schema", path)
				union[k] = v
			}
		}
		assertSameKeys(t, path+"[]", sv[0], union)
	}
}

// fullContractView carries every field of a promotion, a revert's
// same_as_version and the document included.
func fullContractView() *catalogv1.ContractVersionView {
	return &catalogv1.ContractVersionView{
		UniqueId:          "analytics.py_daily_kpis",
		Version:           3,
		ContractHash:      "sha256:aaa",
		DocumentYaml:      "apiVersion: v3.1.0\nkind: DataContract\nid: analytics.py_daily_kpis\n",
		ReleaseId:         "rel-3",
		PromotedAt:        "2026-11-14T09:30:00Z",
		ContentHash:       "sha256:ccc",
		IsCurrent:         true,
		SameAsVersion:     1,
		ContinuoContracts: "0.2.0",
	}
}

func TestContract_AsksForTheDocumentAndMapsEveryField(t *testing.T) {
	fake := &fakeNodeCatalog{contractResp: &catalogv1.GetNodeContractResponse{Contract: fullContractView()}}

	stdout, stderr, exit := runContractCommand(t, NewContractCommand, fake, []string{"analytics.py_daily_kpis"}, false)

	require.Equal(t, 0, exit)
	assert.Empty(t, stderr)
	assert.Equal(t, "analytics.py_daily_kpis", fake.gotContractUniqueID)
	assert.True(t, fake.gotContractIncludeDocument, "node contract always asks for the document")

	var payload struct {
		Contract map[string]any `json:"contract"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	c := payload.Contract
	assert.Equal(t, "analytics.py_daily_kpis", c["unique_id"])
	assert.Equal(t, float64(3), c["version"])
	assert.Equal(t, "sha256:aaa", c["contract_hash"])
	assert.Equal(t, "rel-3", c["release_id"])
	assert.Equal(t, "2026-11-14T09:30:00Z", c["promoted_at"], "the server's RFC 3339 string passes through")
	assert.Equal(t, "sha256:ccc", c["content_hash"])
	assert.Equal(t, true, c["is_current"])
	assert.Equal(t, float64(1), c["same_as_version"])
	assert.Equal(t, "0.2.0", c["continuo_contracts"])
	assert.Equal(t, "apiVersion: v3.1.0\nkind: DataContract\nid: analytics.py_daily_kpis\n", c["document_yaml"])
}

func TestContract_OmitsWhatAVersionDoesNotCarry(t *testing.T) {
	fake := &fakeNodeCatalog{contractResp: &catalogv1.GetNodeContractResponse{Contract: &catalogv1.ContractVersionView{
		UniqueId: "analytics.py_daily_kpis", Version: 1, ContractHash: "sha256:aaa", ContentHash: "sha256:ccc", IsCurrent: true,
	}}}

	stdout, _, exit := runContractCommand(t, NewContractCommand, fake, []string{"analytics.py_daily_kpis"}, false)

	require.Equal(t, 0, exit)
	var payload struct {
		Contract map[string]any `json:"contract"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	for _, k := range []string{"same_as_version", "release_id", "promoted_at", "continuo_contracts", "document_yaml"} {
		assert.NotContains(t, payload.Contract, k, "%s is left out when the version has none", k)
	}
	for _, k := range []string{"unique_id", "version", "contract_hash", "content_hash", "is_current"} {
		assert.Contains(t, payload.Contract, k)
	}
}

func TestContract_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeNodeCatalog{contractResp: &catalogv1.GetNodeContractResponse{Contract: fullContractView()}}
	stdout, _, exit := runContractCommand(t, NewContractCommand, fake, []string{"analytics.py_daily_kpis"}, false)
	require.Equal(t, 0, exit)

	assertEmitsItsSchema(t, NewContractCommand(nil, &config.Config{}, io.Discard, io.Discard), stdout)
}

func TestContract_NotFoundExits3(t *testing.T) {
	fake := &fakeNodeCatalog{contractErr: status.Error(codes.NotFound, `node "analytics.orders" has no contract`)}

	stdout, _, exit := runContractCommand(t, NewContractCommand, fake, []string{"analytics.orders"}, false)

	assert.Equal(t, 3, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeNotFound, env["error"].Code)
	assert.Contains(t, env["error"].Message, "has no contract")
}

func TestContract_WrongArgCountExits2WithoutCallingTheServer(t *testing.T) {
	for _, args := range [][]string{{}, {"analytics.orders", "extra"}} {
		fake := &fakeNodeCatalog{}
		stdout, _, exit := runContractCommand(t, NewContractCommand, fake, args, false)
		assert.Equal(t, 2, exit, "%v", args)
		var env map[string]output.CLIError
		require.NoError(t, json.Unmarshal([]byte(stdout), &env))
		assert.Equal(t, output.CodeUsage, env["error"].Code)
		assert.Zero(t, fake.contractCalls)
	}
}

func TestContract_UnavailableExits5AndInternalExits6(t *testing.T) {
	_, _, exit := runContractCommand(t, NewContractCommand, &fakeNodeCatalog{contractErr: status.Error(codes.Unavailable, "down")}, []string{"analytics.orders"}, false)
	assert.Equal(t, 5, exit)
	_, _, exit = runContractCommand(t, NewContractCommand, &fakeNodeCatalog{contractErr: status.Error(codes.Internal, "boom")}, []string{"analytics.orders"}, false)
	assert.Equal(t, 6, exit)
}

func TestContract_FactoryErrorExits5(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	cmd := NewContractCommand(func(context.Context, string) (client.CatalogClient, error) {
		return nil, status.Error(codes.Unavailable, "dial failed")
	}, &config.Config{Timeout: 2 * time.Second}, &outBuf, &errBuf)
	cmd.SetArgs([]string{"analytics.orders"})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	var cliErr output.CLIError
	require.True(t, errors.As(cmd.Execute(), &cliErr))
	assert.Equal(t, 5, cliErr.ExitCode())
}

func TestContract_HumanModeWritesTheSummaryAndTheDocumentToStderr(t *testing.T) {
	fake := &fakeNodeCatalog{contractResp: &catalogv1.GetNodeContractResponse{Contract: fullContractView()}}

	stdout, stderr, exit := runContractCommand(t, NewContractCommand, fake, []string{"analytics.py_daily_kpis"}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "analytics.py_daily_kpis")
	assert.Contains(t, stderr, "sha256:aaa")
	assert.Contains(t, stderr, "kind: DataContract")
}

func TestContract_IsDocumentedAsAReadOnlyCommandThatTheChatSetsNoFlagsOn(t *testing.T) {
	cmd := NewContractCommand(nil, &config.Config{}, io.Discard, io.Discard)

	assert.NotEmpty(t, cmd.Short)
	assert.NotEmpty(t, cmd.Long)
	assert.NotEmpty(t, cmd.Example)
	assert.JSONEq(t, `[0,2,3,5,6]`, cmd.Annotations["exit_codes"])
	assert.NotContains(t, cmd.Annotations, "mutating")
	assert.NotContains(t, cmd.Annotations, "chat_flags")
	assert.Zero(t, cmd.LocalFlags().NFlag())
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) { assert.NotEmpty(t, f.Usage, f.Name) })
}
