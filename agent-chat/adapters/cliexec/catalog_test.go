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
