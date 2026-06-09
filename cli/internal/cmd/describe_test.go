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
	Path         string            `json:"path"`
	Short        string            `json:"short"`
	Long         string            `json:"long"`
	Args         []string          `json:"args"`
	Flags        []describeFlagJSON `json:"flags"`
	Examples     []string          `json:"examples"`
	OutputSchema json.RawMessage   `json:"output_schema"`
	ExitCodes    json.RawMessage   `json:"exit_codes"`
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
