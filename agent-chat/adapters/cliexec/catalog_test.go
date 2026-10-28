package cliexec

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadCatalog(t *testing.T) *Catalog {
	raw, err := os.ReadFile("testdata/describe.json")
	require.NoError(t, err)
	c, err := NewCatalogFromDescribe(raw)
	require.NoError(t, err)
	return c
}

func TestCatalog_DerivesToolsFromDescribe(t *testing.T) {
	c := loadCatalog(t)

	status, ok := c.Lookup("schedule_status")
	require.True(t, ok)
	assert.Equal(t, []string{"schedule", "status"}, status.CLIPath)
	assert.False(t, status.Mutating)
	require.Len(t, status.Params, 1)
	assert.Equal(t, "schedule-name", status.Params[0].Name)
	assert.True(t, status.Params[0].Required)
	assert.NotEmpty(t, status.Description)

	trigger, ok := c.Lookup("schedule_trigger")
	require.True(t, ok)
	assert.True(t, trigger.Mutating)

	list, ok := c.Lookup("schedule_list")
	require.True(t, ok)
	assert.Empty(t, list.Params)
}

func TestCatalog_CancelIsTwoPositionalMutatingTool(t *testing.T) {
	c := loadCatalog(t)

	cancel, ok := c.Lookup("schedule_cancel")
	require.True(t, ok)
	assert.Equal(t, []string{"schedule", "cancel"}, cancel.CLIPath)
	assert.True(t, cancel.Mutating)
	require.Len(t, cancel.Params, 2)
	assert.Equal(t, "schedule-name", cancel.Params[0].Name)
	assert.True(t, cancel.Params[0].Required)
	assert.Equal(t, "reason", cancel.Params[1].Name)
	assert.True(t, cancel.Params[1].Required)
	assert.Equal(t, []string{"schedule-name", "reason"}, cancel.ParamOrder)
}

func TestCatalog_ExcludesDescribeItself(t *testing.T) {
	c := loadCatalog(t)
	_, ok := c.Lookup("describe")
	assert.False(t, ok)
}

func TestCatalog_UnknownToolDoesNotExist(t *testing.T) {
	c := loadCatalog(t)
	_, ok := c.Lookup("rm_rf")
	assert.False(t, ok)
}

// A bracketed positional in the CLI's Use string ("[source-run-id]") is the
// only way an optional value reaches the chat: the catalog exposes no flags.
// This pins that node trigger's fourth positional is offered to the model as
// an optional parameter, ordered after the identity triple.
func TestCatalog_BracketedPositionalIsOptional(t *testing.T) {
	c := loadCatalog(t)

	trigger, ok := c.Lookup("node_trigger")
	require.True(t, ok)
	assert.Equal(t, []string{"node", "trigger"}, trigger.CLIPath)
	assert.True(t, trigger.Mutating)
	require.Len(t, trigger.Params, 4)
	for _, p := range trigger.Params[:3] {
		assert.True(t, p.Required, "identity triple param %q must be required", p.Name)
	}
	assert.Equal(t, "source-run-id", trigger.Params[3].Name)
	assert.False(t, trigger.Params[3].Required)
	assert.Equal(t, []string{"service", "schema", "table", "source-run-id"}, trigger.ParamOrder)
}

// The dlq commands drive dead-letter-controller: redrive mutates and takes the
// ids plus an audit reason, while list's filters are optional bracketed
// positionals.
func TestCatalog_DLQTools(t *testing.T) {
	c := loadCatalog(t)

	redrive, ok := c.Lookup("dlq_redrive")
	require.True(t, ok)
	assert.Equal(t, []string{"dlq", "redrive"}, redrive.CLIPath)
	assert.True(t, redrive.Mutating)
	require.Len(t, redrive.Params, 2)
	assert.Equal(t, []string{"ids", "reason"}, redrive.ParamOrder)
	for _, p := range redrive.Params {
		assert.True(t, p.Required, "param %q must be required", p.Name)
	}

	list, ok := c.Lookup("dlq_list")
	require.True(t, ok)
	assert.False(t, list.Mutating)
	require.Len(t, list.Params, 2)
	for _, p := range list.Params {
		assert.False(t, p.Required, "param %q must be optional", p.Name)
	}
}

// The cron commands change a schedule, so the chat asks the operator before
// running one. set-cron's description is the one optional parameter; the others
// take only the schedule name.
func TestCatalog_CronToolsAreMutatingAndSetCronTakesAnOptionalDescription(t *testing.T) {
	c := loadCatalog(t)

	setCron, ok := c.Lookup("schedule_set-cron")
	require.True(t, ok)
	assert.Equal(t, []string{"schedule", "set-cron"}, setCron.CLIPath)
	assert.True(t, setCron.Mutating)
	assert.Equal(t, []string{"name", "cron", "timezone", "description"}, setCron.ParamOrder)
	require.Len(t, setCron.Params, 4)
	for _, p := range setCron.Params[:3] {
		assert.True(t, p.Required, "param %q must be required", p.Name)
	}
	assert.False(t, setCron.Params[3].Required, "description keeps the current one when omitted")

	for _, name := range []string{"schedule_remove-cron", "schedule_pause", "schedule_resume"} {
		tool, ok := c.Lookup(name)
		require.True(t, ok, name)
		assert.True(t, tool.Mutating, "%s changes a schedule", name)
		assert.Equal(t, []string{"name"}, tool.ParamOrder, name)
		require.Len(t, tool.Params, 1, name)
		assert.True(t, tool.Params[0].Required, name)
	}
}

// run rebase takes arguments that are partly required and partly optional: the
// run id and the release id the preview printed are required, and the
// changed-upstream choice is a bracketed positional the model may omit.
func TestCatalog_RunRebaseHasTwoRequiredAndOneOptionalPositional(t *testing.T) {
	c := loadCatalog(t)

	rebase, ok := c.Lookup("run_rebase")
	require.True(t, ok)
	assert.Equal(t, []string{"run", "rebase"}, rebase.CLIPath)
	require.Len(t, rebase.Params, 3)
	assert.Equal(t, []string{"run-id", "release-id", "with-changed-upstream"}, rebase.ParamOrder)
	assert.True(t, rebase.Params[0].Required)
	assert.True(t, rebase.Params[1].Required)
	assert.False(t, rebase.Params[2].Required)
}

// Every command that cancels a run or starts another one is mutating, so the
// chat asks the user to approve it before it runs; the reads of a run never
// are.
func TestCatalog_RunMutatingCommandsNeedApproval(t *testing.T) {
	c := loadCatalog(t)

	for _, name := range []string{"run_cancel", "run_rerun", "run_rebase"} {
		tool, ok := c.Lookup(name)
		require.True(t, ok, "tool %q must be in the catalog", name)
		assert.True(t, tool.Mutating, "tool %q must need approval", name)
	}
	for _, name := range []string{"run_show", "run_graph", "run_attempts", "run_rebase-preview", "schedule_runs"} {
		tool, ok := c.Lookup(name)
		require.True(t, ok, "tool %q must be in the catalog", name)
		assert.False(t, tool.Mutating, "tool %q is a read", name)
	}
}
