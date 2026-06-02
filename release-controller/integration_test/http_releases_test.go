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
			ReleaseID: id, ImageTags: map[string]string{"service-1": "t"}, ManifestsURI: "u",
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

func TestIntegration_GetReleaseIncludesPerNode(t *testing.T) {
	srv, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()

	// Drive through the full failure path so per_node_results are persisted.
	require.NoError(t, handlers.ReceiveCandidate(ctx, deps, handlers.ReceiveCandidateInput{
		ReleaseID: "rd1", ImageTags: map[string]string{"service-1": "t"}, ManifestsURI: "u",
	}))
	require.NoError(t, handlers.AdvanceQueue(ctx, deps))
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
