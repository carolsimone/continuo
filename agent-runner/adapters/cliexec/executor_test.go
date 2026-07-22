package cliexec

import (
	"context"
	"encoding/json"
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
// or emits a CLIError and exits 3 when any argument equals the magic name "missing".
// The loop over all args makes the check position-independent regardless of CLIPath depth.
func fakeCLI(t *testing.T) string {
	dir := t.TempDir()
	path := filepath.Join(dir, "continuo")
	script := `#!/bin/sh
for a in "$@"; do
  if [ "$a" = "missing" ]; then
    echo '{"error":{"code":"not_found","message":"no schedule named missing","retryable":false}}'
    exit 3
  fi
done
echo "{\"argv\":\"$*\"}"
`
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755)) //nolint:gosec // G306: must be executable to run as the fake CLI under test.
	return path
}

// stderrCLI writes a script that exits non-zero with output only on stderr.
func stderrCLI(t *testing.T) string {
	dir := t.TempDir()
	path := filepath.Join(dir, "continuo")
	script := `#!/bin/sh
echo "something went wrong on stderr" >&2
exit 1
`
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755)) //nolint:gosec // G306: must be executable to run as the fake CLI under test.
	return path
}

// actorCLI writes an executable script that echoes the CONTINUO_ACTOR env var
// it received, as JSON. Used to assert the executor stamps the actor per call.
func actorCLI(t *testing.T) string {
	dir := t.TempDir()
	path := filepath.Join(dir, "continuo")
	script := "#!/bin/sh\n" +
		`printf '{"actor":"%s"}' "$CONTINUO_ACTOR"` + "\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755)) //nolint:gosec // G306: must be executable to run as the fake CLI under test.
	return path
}

// actorAndMarkerCLI echoes both the CONTINUO_ACTOR the executor stamped and an
// inherited marker env var, so a test can prove the subprocess still inherits
// the parent environment when a nil base env is used.
func actorAndMarkerCLI(t *testing.T) string {
	dir := t.TempDir()
	path := filepath.Join(dir, "continuo")
	script := "#!/bin/sh\n" +
		`printf '{"actor":"%s","marker":"%s"}' "$CONTINUO_ACTOR" "$CONTINUO_TEST_MARKER"` + "\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755)) //nolint:gosec // G306: must be executable to run as the fake CLI under test.
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

func TestExecutor_StampsActorFromUserID(t *testing.T) {
	// Base env has no CONTINUO_ACTOR; the executor must add it from userID.
	e := NewExecutor(testCatalog(t), actorCLI(t), []string{"PATH=/usr/bin:/bin"}, 5*time.Second, 4096,
		slog.New(slog.NewTextHandler(os.Stderr, nil)))
	res := e.Execute(context.Background(), "okta.example.com|alice", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": "daily"}))
	assert.False(t, res.IsError)
	assert.Contains(t, res.Output, `"actor":"okta.example.com|alice"`)
}

func TestExecutor_NilEnvStillInheritsParentWhenStampingActor(t *testing.T) {
	// A nil base env means "inherit the parent environment". Stamping the actor
	// must NOT break that inheritance (regression: appending to nil would replace
	// the whole env with a single CONTINUO_ACTOR entry, stripping PATH and the
	// inherited CONTINUO_* service config).
	t.Setenv("CONTINUO_TEST_MARKER", "present")
	e := NewExecutor(testCatalog(t), actorAndMarkerCLI(t), nil, 5*time.Second, 4096,
		slog.New(slog.NewTextHandler(os.Stderr, nil)))
	res := e.Execute(context.Background(), "okta.example.com|alice", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": "daily"}))
	assert.False(t, res.IsError)
	assert.Contains(t, res.Output, `"actor":"okta.example.com|alice"`)
	assert.Contains(t, res.Output, `"marker":"present"`, "nil base env must still inherit the parent environment")
}

func TestExecutor_EmptyUserIDOmitsActor(t *testing.T) {
	// Empty userID must leave CONTINUO_ACTOR unset so state records the system sentinel.
	e := NewExecutor(testCatalog(t), actorCLI(t), []string{"PATH=/usr/bin:/bin"}, 5*time.Second, 4096,
		slog.New(slog.NewTextHandler(os.Stderr, nil)))
	res := e.Execute(context.Background(), "", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": "daily"}))
	assert.False(t, res.IsError)
	assert.Contains(t, res.Output, `"actor":""`)
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

// TestExecutor_RejectErrorIsValidJSON verifies that when an arg value contains
// a double-quote and a control character the rejection output is valid JSON and
// round-trips the original message intact (Fix 1).
func TestExecutor_RejectErrorIsValidJSON(t *testing.T) {
	e := newExecutor(t)
	// Value starts with '-' to trigger the flag-smuggling reject path.
	// The value also contains a double-quote and a BEL control char (\x07),
	// both of which would produce invalid JSON under Go's %q formatting.
	badVal := "-a\"b\x07c"
	res := e.Execute(context.Background(), "alice", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": badVal}))
	assert.True(t, res.IsError)
	require.True(t, json.Valid([]byte(res.Output)),
		"rejection Output must be valid JSON, got: %s", res.Output)

	// Round-trip: the message field must survive the encoding faithfully.
	var env errorEnvelope
	require.NoError(t, json.Unmarshal([]byte(res.Output), &env))
	assert.Contains(t, env.Error.Message, "must not start with '-'",
		"error message should describe the rejection reason")
}

// TestExecutor_StderrFallback verifies that when the CLI exits non-zero with
// empty stdout the stderr text is surfaced as valid JSON (Fix 2).
func TestExecutor_StderrFallback(t *testing.T) {
	catalog := testCatalog(t)
	e := NewExecutor(catalog, stderrCLI(t), nil, 5*time.Second, 4096,
		slog.New(slog.NewTextHandler(os.Stderr, nil)))
	res := e.Execute(context.Background(), "alice", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": "daily"}))
	assert.True(t, res.IsError)
	require.True(t, json.Valid([]byte(res.Output)),
		"stderr fallback Output must be valid JSON, got: %s", res.Output)
	assert.Contains(t, res.Output, "something went wrong on stderr")
}

// TestExecutor_BinaryMissing verifies that pointing cliPath at a non-existent
// file yields IsError=true and valid JSON output containing "unavailable"
// (Fix 6, covers the non-ExitError branch + Fix 1 encoding).
func TestExecutor_BinaryMissing(t *testing.T) {
	catalog := testCatalog(t)
	e := NewExecutor(catalog, "/nonexistent/path/to/continuo", nil, 5*time.Second, 4096,
		slog.New(slog.NewTextHandler(os.Stderr, nil)))
	res := e.Execute(context.Background(), "alice", uuid.New(),
		call("schedule_status", map[string]string{"schedule-name": "daily"}))
	assert.True(t, res.IsError)
	require.True(t, json.Valid([]byte(res.Output)),
		"binary-missing Output must be valid JSON, got: %s", res.Output)
	assert.Contains(t, res.Output, "unavailable")
}

// TestNewExecutor_DefaultsMaxBytes verifies that maxBytes <= 0 is replaced by
// the default (Fix 3).
func TestNewExecutor_DefaultsMaxBytes(t *testing.T) {
	catalog := testCatalog(t)
	e := NewExecutor(catalog, fakeCLI(t), nil, 5*time.Second, 0,
		slog.New(slog.NewTextHandler(os.Stderr, nil)))
	assert.Equal(t, defaultMaxBytes, e.maxBytes)

	e2 := NewExecutor(catalog, fakeCLI(t), nil, 5*time.Second, -1,
		slog.New(slog.NewTextHandler(os.Stderr, nil)))
	assert.Equal(t, defaultMaxBytes, e2.maxBytes)
}
