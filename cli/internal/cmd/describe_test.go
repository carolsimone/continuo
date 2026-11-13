package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type describeFlagJSON struct {
	Name     string `json:"name"`
	Usage    string `json:"usage"`
	Default  string `json:"default"`
	Chat     bool   `json:"chat"`
	Repeated bool   `json:"repeated"`
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
	for _, want := range []string{"schedule list", "schedule trigger", "schedule cancel", "schedule graph", "schedule status", "schedule runs", "schedule set-cron", "schedule remove-cron", "schedule pause", "schedule resume", "run show", "run graph", "run attempts", "run cancel", "run rerun", "run rebase-preview", "run rebase", "dlq list", "dlq show", "dlq redrive", "release notices", "describe", "precedents"} {
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
	for _, k := range []string{"run", "tasks"} {
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

func TestDescribe_ReleaseNoticesIsADocumentedReadOnlyCommand(t *testing.T) {
	c := findCmd(t, runDescribe(t), "release notices")
	assert.Equal(t, []string{"<release-id>"}, c.Args)
	assert.False(t, c.Mutating, "release notices only reads")
	require.NotEmpty(t, c.Examples)
	assert.Equal(t, "continuo release notices rel-a1b2c3d-201", c.Examples[0])
	assert.Contains(t, c.Long, "predicts")
	assert.Contains(t, c.Long, "preflight")

	var schema map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(c.OutputSchema, &schema))
	for _, k := range []string{"release_id", "status", "notices"} {
		_, ok := schema[k]
		assert.True(t, ok, "release notices output_schema missing key %q", k)
	}
	var exits []int
	require.NoError(t, json.Unmarshal(c.ExitCodes, &exits))
	assert.Equal(t, []int{0, 2, 3, 5, 6}, exits)

	flagNames := map[string]bool{}
	for _, f := range c.Flags {
		flagNames[f.Name] = true
	}
	assert.True(t, flagNames["release-endpoint"], "expected global flag --release-endpoint in the catalog flag list")
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

func TestDescribe_CancelUsesPositionalReasonAndNoCommandFlags(t *testing.T) {
	p := runDescribe(t)

	cancel := findCmd(t, p, "schedule cancel")
	assert.Equal(t, []string{"<schedule-name>", "<reason>"}, cancel.Args)

	flagNames := map[string]bool{}
	for _, f := range cancel.Flags {
		flagNames[f.Name] = true
	}
	assert.False(t, flagNames["reason"], "cancel must not expose a --reason flag")
	assert.False(t, flagNames["by"], "cancel must not expose a --by flag")
	// Global persistent flags are still present.
	for _, want := range []string{"human", "endpoint", "timeout"} {
		assert.True(t, flagNames[want], "expected global flag --%s on cancel", want)
	}
}

func TestDescribe_MutatingFlagMarksMutatingCommands(t *testing.T) {
	p := runDescribe(t)

	trigger := findCmd(t, p, "schedule trigger")
	assert.True(t, trigger.Mutating, "schedule trigger must be marked mutating")

	cancel := findCmd(t, p, "schedule cancel")
	assert.True(t, cancel.Mutating, "schedule cancel must be marked mutating")

	redrive := findCmd(t, p, "dlq redrive")
	assert.True(t, redrive.Mutating, "dlq redrive must be marked mutating")

	for _, path := range []string{"schedule set-cron", "schedule remove-cron", "schedule pause", "schedule resume"} {
		c := findCmd(t, p, path)
		assert.True(t, c.Mutating, "%s changes a schedule, so the chat must ask for approval", path)
	}

	for _, path := range []string{"run cancel", "run rerun", "run rebase"} {
		assert.True(t, findCmd(t, p, path).Mutating, "%s must be marked mutating", path)
	}

	for _, path := range []string{"schedule status", "schedule runs", "schedule list", "schedule graph", "run show", "run graph", "run attempts", "run rebase-preview", "dlq list", "dlq show", "describe"} {
		c := findCmd(t, p, path)
		assert.False(t, c.Mutating, "command %q must not be mutating", path)
	}
}

func TestDescribe_DLQCommandsDocumentTheirPositionalArgsAndExitCodes(t *testing.T) {
	p := runDescribe(t)

	list := findCmd(t, p, "dlq list")
	assert.Equal(t, []string{"[source]", "[stream]"}, list.Args)
	show := findCmd(t, p, "dlq show")
	assert.Equal(t, []string{"<id>"}, show.Args)
	redrive := findCmd(t, p, "dlq redrive")
	assert.Equal(t, []string{"<ids>", "<reason>"}, redrive.Args)

	var exitCodes []int
	require.NoError(t, json.Unmarshal(redrive.ExitCodes, &exitCodes))
	assert.Contains(t, exitCodes, 4, "redrive must document conflict (expired / not redrivable)")

	for _, c := range []describeCommandJSON{list, show, redrive} {
		flagNames := map[string]bool{}
		for _, f := range c.Flags {
			flagNames[f.Name] = true
		}
		assert.True(t, flagNames["dead-letter-endpoint"], "%s must expose the global --dead-letter-endpoint flag", c.Path)
	}
}

// The cron commands take the schedule name first. set-cron's description is the
// one optional positional, and the catalog the chat builds offers it as optional
// because of its brackets.
func TestDescribe_CronCommandsDocumentTheirPositionalArgsAndExitCodes(t *testing.T) {
	p := runDescribe(t)

	setCron := findCmd(t, p, "schedule set-cron")
	assert.Equal(t, []string{"<name>", "<cron>", "<timezone>", "[description]"}, setCron.Args)
	assert.Equal(t, []string{"<name>"}, findCmd(t, p, "schedule remove-cron").Args)
	assert.Equal(t, []string{"<name>"}, findCmd(t, p, "schedule pause").Args)
	assert.Equal(t, []string{"<name>"}, findCmd(t, p, "schedule resume").Args)

	for _, path := range []string{"schedule pause", "schedule resume"} {
		var exitCodes []int
		require.NoError(t, json.Unmarshal(findCmd(t, p, path).ExitCodes, &exitCodes))
		assert.Contains(t, exitCodes, 4, "%s must document the conflict of a concurrent edit", path)
		assert.Contains(t, exitCodes, 3, "%s must document a schedule with no cron", path)
	}
	for _, path := range []string{"schedule set-cron", "schedule remove-cron", "schedule pause", "schedule resume"} {
		var exitCodes []int
		require.NoError(t, json.Unmarshal(findCmd(t, p, path).ExitCodes, &exitCodes))
		assert.Contains(t, exitCodes, 7, "%s is refused in maintenance mode", path)
	}

	var setCronCodes []int
	require.NoError(t, json.Unmarshal(setCron.ExitCodes, &setCronCodes))
	assert.Contains(t, setCronCodes, 4, "set-cron must document the conflict of a schedule that changed twice while it ran")
}

func TestDescribe_RunReadCommandsDocumentTheirArgsAndExitCodes(t *testing.T) {
	p := runDescribe(t)

	assert.Equal(t, []string{"<run-id>"}, findCmd(t, p, "run show").Args)
	assert.Equal(t, []string{"<run-id>"}, findCmd(t, p, "run graph").Args)
	assert.Equal(t, []string{"<run-id>", "[task-id]"}, findCmd(t, p, "run attempts").Args)
	assert.Equal(t, []string{"<schedule-name>"}, findCmd(t, p, "schedule runs").Args)

	var exitCodes []int
	require.NoError(t, json.Unmarshal(findCmd(t, p, "run show").ExitCodes, &exitCodes))
	assert.Contains(t, exitCodes, 3, "run show must document not_found")
}

func TestDescribe_RunWriteCommandsDocumentTheirArgsAndExitCodes(t *testing.T) {
	p := runDescribe(t)

	assert.Equal(t, []string{"<run-id>", "<reason>"}, findCmd(t, p, "run cancel").Args)
	assert.Equal(t, []string{"<run-id>"}, findCmd(t, p, "run rerun").Args)
	assert.Equal(t, []string{"<run-id>", "[with-changed-upstream]"}, findCmd(t, p, "run rebase-preview").Args)
	assert.Equal(t, []string{"<run-id>", "<release-id>", "[with-changed-upstream]"}, findCmd(t, p, "run rebase").Args)

	for path, want := range map[string][]int{
		"run cancel":         {0, 2, 3, 4, 5, 6},
		"run rerun":          {0, 2, 3, 4, 5, 6, 7},
		"run rebase-preview": {0, 2, 3, 4, 5, 6},
		"run rebase":         {0, 2, 3, 4, 5, 6, 7},
	} {
		var got []int
		require.NoError(t, json.Unmarshal(findCmd(t, p, path).ExitCodes, &got))
		assert.Equal(t, want, got, path)
	}
}

// The chat sets only the flags a command marks in its chat_flags annotation.
// node full-refresh marks --acknowledge, which repeats; every other flag of
// every command, the global flags included, stays out of the chat's reach.
func TestDescribe_MarksTheFlagsTheChatMaySetAndTheOnesThatRepeat(t *testing.T) {
	p := runDescribe(t)

	chatFlags := map[string][]string{}
	for _, c := range p.Commands {
		for _, f := range c.Flags {
			if f.Chat {
				chatFlags[c.Path] = append(chatFlags[c.Path], f.Name)
			}
		}
	}
	assert.Equal(t, map[string][]string{"node full-refresh": {"acknowledge"}}, chatFlags)

	byName := map[string]describeFlagJSON{}
	for _, f := range findCmd(t, p, "node full-refresh").Flags {
		byName[f.Name] = f
	}
	require.Contains(t, byName, "acknowledge")
	assert.True(t, byName["acknowledge"].Repeated, "--acknowledge is a string array, so describe marks it repeated")
	require.Contains(t, byName, "endpoint")
	assert.False(t, byName["endpoint"].Repeated)
	assert.False(t, byName["endpoint"].Chat)
}

// Every name a command lists in chat_flags is one of its own flags, and is a
// string or a string array, since the chat renders it as --<name> <value>
// once per item: a string slice would split one item on its commas.
func TestChatFlags_NameRealStringFlagsOfTheirCommand(t *testing.T) {
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		if raw, ok := c.Annotations[chatFlagsAnnotation]; ok {
			for _, name := range strings.Split(raw, ",") {
				name = strings.TrimSpace(name)
				if !assert.NotEmpty(t, name, "%s lists a blank chat flag in %q", c.CommandPath(), raw) {
					continue
				}
				f := c.LocalFlags().Lookup(name)
				if assert.NotNil(t, f, "%s lists chat flag %q, which it does not define", c.CommandPath(), name) {
					assert.Contains(t, []string{"string", "stringArray"}, f.Value.Type(),
						"%s chat flag --%s must be a string or a string array", c.CommandPath(), name)
				}
			}
		}
		for _, child := range c.Commands() {
			walk(child)
		}
	}
	walk(newRootCommand(io.Discard, io.Discard))
}
