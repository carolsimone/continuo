//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_PythonRelease_SkipsCompileLeg verifies the full HTTP path for
// a python-kind release: POST /releases activates it straight into Parsing
// (no compile leg), release.requested:v1 carries the per-entry kind and the
// contract.yaml artifact URI, and no compile.requested:v1 row is ever written.
func TestIntegration_PythonRelease_SkipsCompileLeg(t *testing.T) {
	srv, deps, db := setup(t)
	defer db.Close()

	body, _ := json.Marshal(handlers.ReceiveCandidateInput{
		Service:   "svc-py",
		ReleaseID: "it-py-1",
		ImageTag:  "img",
		Repo:      "acme/py",
		CommitSHA: "cafebabe",
		Kind:      "python",
	})
	req := httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.Routes().ServeHTTP(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)

	r, err := deps.NewUoW().ReleaseRepo().Get(context.Background(), "it-py-1")
	require.NoError(t, err)
	require.NotNil(t, r)
	assert.Equal(t, release.StatusParsing, r.Status())
	assert.Equal(t, release.ManifestKindPython, r.ManifestKind())

	entries, err := deps.NewUoW().OutboxRepo().GetPendingBatch(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "exactly one outbox row: release.requested:v1, no compile.requested:v1")
	assert.Equal(t, streams.ReleaseRequestedV1, entries[0].StreamName)

	var p struct {
		ReleaseID    string `json:"release_id"`
		ManifestKeys []struct {
			Service string `json:"service"`
			S3URI   string `json:"s3_uri"`
			Kind    string `json:"kind"`
		} `json:"manifest_keys"`
	}
	require.NoError(t, json.Unmarshal(entries[0].Payload, &p))
	assert.Equal(t, "it-py-1", p.ReleaseID)
	require.Len(t, p.ManifestKeys, 1)
	assert.Equal(t, "svc-py", p.ManifestKeys[0].Service)
	assert.Equal(t, "python", p.ManifestKeys[0].Kind)
	assert.Equal(t, "s3://test-bucket/svc-py/it-py-1/contract.yaml", p.ManifestKeys[0].S3URI)
}
