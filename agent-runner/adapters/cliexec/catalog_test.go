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
