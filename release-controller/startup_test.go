package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func refused() error { return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED} }

func TestRetryWhileUnreachable_RetriesAnOutageUntilItClears(t *testing.T) {
	calls := 0
	step := func(context.Context) error {
		calls++
		if calls < 3 {
			return refused()
		}
		return nil
	}
	require.NoError(t, retryWhileUnreachable(context.Background(), discardLogger(), "test", step, time.Millisecond, 2*time.Millisecond))
	assert.Equal(t, 3, calls)
}

func TestRetryWhileUnreachable_ReturnsAnyOtherErrorAtOnce(t *testing.T) {
	calls := 0
	boom := errors.New("load current_prod topology of r1: topology artifact corrupt")
	err := retryWhileUnreachable(context.Background(), discardLogger(), "test", func(context.Context) error {
		calls++
		return boom
	}, time.Millisecond, time.Millisecond)
	assert.ErrorIs(t, err, boom)
	assert.Equal(t, 1, calls)
}

func TestRetryWhileUnreachable_StopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := retryWhileUnreachable(ctx, discardLogger(), "test", func(context.Context) error { return refused() }, time.Hour, time.Hour)
	assert.ErrorIs(t, err, context.Canceled)
}

// logLines runs fn with a logger that writes JSON lines to a buffer and returns
// what it logged.
func logLines(t *testing.T, fn func(*slog.Logger)) string {
	t.Helper()
	var buf bytes.Buffer
	fn(slog.New(slog.NewJSONHandler(&buf, nil)))
	return buf.String()
}

// A project still released as kind python is logged as exactly that, naming the
// projects, and stops the process with a failure.
func TestPythonKindCheckExitCode_NamesTheProjectsAndFails(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &handlers.PythonKindProjectsError{Projects: []string{"svc-a", "svc-b"}})
	var code int
	out := logLines(t, func(l *slog.Logger) { code = pythonKindCheckExitCode(context.Background(), l, err) })

	assert.Equal(t, 1, code)
	assert.Contains(t, out, "Refusing to start: projects are still released as kind python")
	assert.Contains(t, out, "svc-a")
	assert.Contains(t, out, "svc-b")
}

// A read that fails for any other reason, such as a lost connection, is not
// reported as a python project: it is logged as a failed check.
func TestPythonKindCheckExitCode_AnyOtherErrorIsLoggedAsAFailedCheck(t *testing.T) {
	var code int
	out := logLines(t, func(l *slog.Logger) {
		code = pythonKindCheckExitCode(context.Background(), l, errors.New("list the projects: connection refused"))
	})

	assert.Equal(t, 1, code)
	assert.Contains(t, out, "Failed to check")
	assert.Contains(t, out, "connection refused")
	assert.NotContains(t, out, "Refusing to start")
}

// A shutdown signal that arrives during the check ends the process quietly, as
// a shutdown during the Postgres connect does: exit code 0 and no error logged.
func TestPythonKindCheckExitCode_ShutdownDuringTheCheckExitsCleanly(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, err := range map[string]error{
		"the read reports the cancellation": fmt.Errorf("list the projects: %w", context.Canceled),
		"the read reports another error":    errors.New("driver: bad connection"),
	} {
		t.Run(name, func(t *testing.T) {
			var code int
			out := logLines(t, func(l *slog.Logger) { code = pythonKindCheckExitCode(cancelled, l, err) })

			assert.Equal(t, 0, code)
			assert.NotContains(t, out, `"level":"ERROR"`)
		})
	}
}
