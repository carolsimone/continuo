//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_ListReleasesPaginated(t *testing.T) {
	srv, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()
	for _, id := range []string{"rh1", "rh2", "rh3"} {
		require.NoError(t, handlers.ReceiveCandidate(ctx, deps, handlers.ReceiveCandidateInput{
			Service: "service-1", ReleaseID: id, ImageTag: "t",
			Repo: "acme/demo", CommitSHA: "deadbeefcafe1234",
		}))
	}
	req := httptest.NewRequest(http.MethodGet, "/releases?limit=2", nil)
	w := httptest.NewRecorder()
	srv.Routes().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Releases   []map[string]any `json:"releases"`
		NextCursor string           `json:"next_cursor"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Len(t, resp.Releases, 2)
	assert.NotEmpty(t, resp.NextCursor)
}

// TestIntegration_ListReleasesLimitFallback locks in the documented contract:
// an unparseable, non-positive, or out-of-range limit is not a client error —
// it falls back to the default page size rather than returning 400.
func TestIntegration_ListReleasesLimitFallback(t *testing.T) {
	srv, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()
	for _, id := range []string{"rl1", "rl2", "rl3"} {
		require.NoError(t, handlers.ReceiveCandidate(ctx, deps, handlers.ReceiveCandidateInput{
			Service: "service-1", ReleaseID: id, ImageTag: "t",
			Repo: "acme/demo", CommitSHA: "deadbeefcafe1234",
		}))
	}
	for _, limit := range []string{"abc", "0", "-1", "999"} {
		req := httptest.NewRequest(http.MethodGet, "/releases?limit="+limit, nil)
		w := httptest.NewRecorder()
		srv.Routes().ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, "limit=%q should fall back, not 400", limit)

		var resp struct {
			Releases []map[string]any `json:"releases"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Len(t, resp.Releases, 3, "limit=%q should return the default page of all 3 seeded releases", limit)
	}
}

func TestIntegration_GetReleaseIncludesPerNode(t *testing.T) {
	srv, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()

	// Drive through the full failure path so per_node_results are persisted.
	require.NoError(t, handlers.ReceiveCandidate(ctx, deps, handlers.ReceiveCandidateInput{
		Service: "service-1", ReleaseID: "rd1", ImageTag: "t",
		Repo: "acme/demo", CommitSHA: "deadbeefcafe1234",
	}))
	require.NoError(t, handlers.AdvanceQueue(ctx, deps))
	require.NoError(t, handlers.HandleCompileResult(ctx, deps, handlers.HandleCompileResultInput{
		ReleaseID: "rd1", Status: "ok",
	}))
	require.NoError(t, handlers.HandleParsedManifest(ctx, deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rd1", Status: "ok",
		Topology: release.Topology{
			{UniqueID: "a", ServiceName: "service-1"},
			{UniqueID: "b", ServiceName: "service-1", UpstreamUniqueIDs: []string{"a"}},
		},
	}))
	// Fail validation on node b so the release is rejected and per-node results are stored.
	require.NoError(t, handlers.HandleValidationResult(ctx, deps, handlers.HandleValidationResultInput{
		ReleaseID: "rd1",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"},
			{NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/b"},
		},
		AggregateStatus: "partial_failed",
	}))

	req := httptest.NewRequest(http.MethodGet, "/releases/rd1", nil)
	w := httptest.NewRecorder()
	srv.Routes().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	perNode, ok := resp["per_node_results"].([]any)
	require.True(t, ok)
	assert.Len(t, perNode, 2)
}
