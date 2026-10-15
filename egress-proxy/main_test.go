package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	proxyhttp "github.com/carolsimone/continuo/egress-proxy/adapters/http"
	"github.com/carolsimone/continuo/egress-proxy/config"
	"github.com/carolsimone/continuo/pkg/liveness"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNameMatchesDirectory(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	assert.Equal(t, filepath.Base(wd), config.Name)
}

// The proxy reports ready only once it listens with its allowlist loaded.
func TestReadiness_WaitsForStartup(t *testing.T) {
	reg := liveness.NewRegistry()
	gate := reg.AddStartupGate()
	ready := proxyhttp.NewReadinessHandler(reg)

	rec := httptest.NewRecorder()
	ready(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	gate.Done()
	rec = httptest.NewRecorder()
	ready(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
}
