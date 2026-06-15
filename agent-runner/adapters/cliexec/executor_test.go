package cliexec

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-runner/service/ports"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCLI writes an executable script that echoes its argv as JSON and exits 0,
// or emits a CLIError and exits 3 when asked for the magic name "missing".
func fakeCLI(t *testing.T) string {
	dir := t.TempDir()
	path := filepath.Join(dir, "continuo")
	script := `#!/bin/sh
if [ "$3" = "missing" ]; then
  echo '{"error":{"code":"not_found","message":"no schedule named missing","retryable":false}}'
  exit 3
fi
echo "{\"argv\":\"$*\"}"
`
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))
	return path
}

func testCatalog(t *testing.T) *Catalog {
	raw, err := os.ReadFile("testdata/describe.json")
	require.NoError(t, err)
	c, err := NewCatalogFromDescribe(raw)
	require.NoError(t, err)
	return c
}

func newExecutor(t *testing.T) *Executor {
	return NewExecutor(testCatalog(t), fakeCLI(t), nil, 5*time.Second, 64,
		slog.New(slog.NewTextHandler(os.Stderr, nil)))
}

func call(name string, args map[string]string) ports.ToolCall {
	return ports.ToolCall{ID: "c1", Name: name, Args: args}
}

func TestExecutor_RunsArgvDirectly(t *testing.T) {
	e := newExecutor(t)
	res := e.Execute(context.Background(), "alice", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": "daily"}))
	assert.False(t, res.IsError)
	assert.Contains(t, res.Output, `"argv":"schedule status daily"`)
}

func TestExecutor_RejectsUnknownTool(t *testing.T) {
	e := newExecutor(t)
	res := e.Execute(context.Background(), "alice", uuid.New(), call("rm_rf", nil))
	assert.True(t, res.IsError)
	assert.Contains(t, res.Output, "unknown tool")
}

func TestExecutor_RejectsMissingRequiredParam(t *testing.T) {
	e := newExecutor(t)
	res := e.Execute(context.Background(), "alice", uuid.New(), call("schedule_status", map[string]string{}))
	assert.True(t, res.IsError)
	assert.Contains(t, res.Output, "missing required parameter")
}

func TestExecutor_RejectsUnknownParam(t *testing.T) {
	e := newExecutor(t)
	res := e.Execute(context.Background(), "alice", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": "daily", "extra": "x"}))
	assert.True(t, res.IsError)
	assert.Contains(t, res.Output, "unknown parameter")
}

func TestExecutor_RejectsFlagSmuggling(t *testing.T) {
	e := newExecutor(t)
	res := e.Execute(context.Background(), "alice", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": "--endpoint=evil:1"}))
	assert.True(t, res.IsError)
	assert.Contains(t, res.Output, "must not start with '-'")
}

func TestExecutor_NonZeroExitFeedsCLIErrorBack(t *testing.T) {
	e := newExecutor(t)
	res := e.Execute(context.Background(), "alice", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": "missing"}))
	assert.True(t, res.IsError)
	assert.Contains(t, res.Output, `"code":"not_found"`)
}

func TestExecutor_TruncatesOutput(t *testing.T) {
	e := newExecutor(t) // maxBytes = 64 in newExecutor
	res := e.Execute(context.Background(), "alice", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": strings.Repeat("x", 200)}))
	assert.LessOrEqual(t, len(res.Output), 64+len(truncationNotice))
}
