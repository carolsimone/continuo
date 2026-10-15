package http

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/liveness"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", ":0") //nolint:gosec // G102: the server under test binds every interface, so a taken port must too
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	return strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
}

// A taken health port is reported by Listen, before the service serves.
func TestListen_FailsWhenPortIsTaken(t *testing.T) {
	taken, err := net.Listen("tcp", ":0") //nolint:gosec // G102: the server under test binds every interface, so a taken port must too
	require.NoError(t, err)
	defer func() { _ = taken.Close() }()

	s := NewServer(strconv.Itoa(taken.Addr().(*net.TCPAddr).Port), liveness.NewRegistry(), slog.Default())
	require.Error(t, s.Listen())
}

func TestServer_ServesHealthOnTheBoundPort(t *testing.T) {
	port := freePort(t)
	s := NewServer(port, liveness.NewRegistry(), slog.Default())
	require.NoError(t, s.Listen())
	done := make(chan error, 1)
	go func() { done <- s.Start() }()

	resp, err := http.Get("http://127.0.0.1:" + port + "/health") //nolint:noctx // test against a local listener
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
	assert.NoError(t, <-done)
}
