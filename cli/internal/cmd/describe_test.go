package cmd

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type describeFlagJSON struct {
	Name    string `json:"name"`
	Usage   string `json:"usage"`
	Default string `json:"default"`
}
type describeCommandJSON struct {
	Path         string             `json:"path"`
	Short        string             `json:"short"`
	Long         string             `json:"long"`
	Args         []string           `json:"args"`
	Flags        []describeFlagJSON `json:"flags"`
	Examples     []string           `json:"examples"`
	OutputSchema json.RawMessage    `json:"output_schema"`
	ExitCodes    json.RawMessage    `json:"exit_codes"`
	Mutating     bool               `json:"mutating"`
}
type describePayloadJSON struct {
	Commands []describeCommandJSON `json:"commands"`
}

func runDescribe(t *testing.T) describePayloadJSON {
	t.Helper()
	var out, errBuf bytes.Buffer
	exit := executeWith([]string{"describe"}, &out, &errBuf)
	require.Equal(t, 0, exit)
	var p describePayloadJSON
	require.NoError(t, json.Unmarshal(out.Bytes(), &p))
	return p
}

func TestDescribe_ListsEveryRunnableCommand(t *testing.T) {
	p := runDescribe(t)
	paths := map[string]bool{}
	for _, c := range p.Commands {
		paths[c.Path] = true
	}
	for _, want := range []string{"schedule list", "schedule trigger", "schedule graph", "schedule status", "describe"} {
		assert.True(t, paths[want], "describe missing command %q", want)
	}
}

func TestDescribe_EveryCommandMeetsTheDocumentationStandard(t *testing.T) {
	p := runDescribe(t)
	require.NotEmpty(t, p.Commands)
	for _, c := range p.Commands {
		assert.NotEmpty(t, c.Long, "command %q has empty Long", c.Path)
		assert.NotEmpty(t, c.Examples, "command %q has no Example", c.Path)
		assert.True(t, json.Valid(c.OutputSchema), "command %q output_schema not valid JSON", c.Path)
		assert.True(t, json.Valid(c.ExitCodes), "command %q exit_codes not valid JSON", c.Path)
	}
}

func findCmd(t *testing.T, p describePayloadJSON, path string) describeCommandJSON {
	t.Helper()
	for _, c := range p.Commands {
		if c.Path == path {
			return c
		}
	}
	t.Fatalf("command %q not found in catalog", path)
	return describeCommandJSON{}
}

func TestDescribe_CommandContentIsAccurate(t *testing.T) {
	p := runDescribe(t)

	status := findCmd(t, p, "schedule status")
	assert.Equal(t, []string{"<schedule-name>"}, status.Args)
	require.NotEmpty(t, status.Examples)
	assert.Contains(t, status.Examples[0], "continuo schedule status")

	// output_schema is real nested JSON (not an escaped string) with the documented keys.
	var schema map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(status.OutputSchema, &schema))
	for _, k := range []string{"schedule_name", "run_id", "is_running", "last_run_status", "nodes"} {
		_, ok := schema[k]
		assert.True(t, ok, "status output_schema missing key %q", k)
	}

	// exit_codes is a real JSON array.
	var codes []int
	require.NoError(t, json.Unmarshal(status.ExitCodes, &codes))
	assert.Contains(t, codes, 3) // not_found

	// The global persistent flags must be discoverable on each command.
	flagNames := map[string]bool{}
	for _, f := range status.Flags {
		flagNames[f.Name] = true
	}
	for _, want := range []string{"human", "endpoint", "timeout"} {
		assert.True(t, flagNames[want], "expected global flag --%s in the catalog flag list", want)
	}
}

func TestDescribe_HumanModeWritesSummaryToStderr(t *testing.T) {
	var out, errBuf bytes.Buffer
	exit := executeWith([]string{"--human", "describe"}, &out, &errBuf)
	require.Equal(t, 0, exit)
	assert.Empty(t, out.String(), "human mode must not write JSON to stdout")
	assert.Contains(t, errBuf.String(), "schedule status", "human summary should list commands on stderr")
}

func TestDescribe_ExtraArgEmitsUsageEnvelopeExits2(t *testing.T) {
	var out, errBuf bytes.Buffer
	exit := executeWith([]string{"describe", "extra"}, &out, &errBuf)
	require.Equal(t, 2, exit)

	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &env))
	assert.Equal(t, "usage", env.Error.Code)
}

func TestDescribe_MutatingFlagMarksMutatingCommands(t *testing.T) {
	p := runDescribe(t)

	trigger := findCmd(t, p, "schedule trigger")
	assert.True(t, trigger.Mutating, "schedule trigger must be marked mutating")

	cancel := findCmd(t, p, "schedule cancel")
	assert.True(t, cancel.Mutating, "schedule cancel must be marked mutating")

	for _, path := range []string{"schedule status", "schedule list", "schedule graph", "describe"} {
		c := findCmd(t, p, path)
		assert.False(t, c.Mutating, "command %q must not be mutating", path)
	}
}
