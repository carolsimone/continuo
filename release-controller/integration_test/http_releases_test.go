//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
	// Project each node's result via the per-node stream (node b failing) so the
	// per_node_results are stored, then deliver the slim terminal decision.
	for _, n := range []handlers.NodeValidationResultInput{
		{ReleaseID: "rd1", Stage: "validation", NodeID: "a", Status: "ok"},
		{ReleaseID: "rd1", Stage: "validation", NodeID: "b", Status: "failed", DBTLogURI: "s3://logs/b"},
	} {
		require.NoError(t, handlers.HandleNodeValidationResult(ctx, deps, n))
	}
	require.NoError(t, handlers.HandleValidationResult(ctx, deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rd1",
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

// TestIntegration_PostReleases_UnknownKind_Returns400 verifies that POST
// /releases rejects an unrecognised kind with HTTP 400 (rather than silently
// defaulting or accepting it) and persists no release row.
func TestIntegration_PostReleases_UnknownKind_Returns400(t *testing.T) {
	srv, deps, db := setup(t)
	defer db.Close()

	body, _ := json.Marshal(handlers.ReceiveCandidateInput{
		Service:   "svc-bad",
		ReleaseID: "rBadKind",
		ImageTag:  "t",
		Repo:      "acme/demo",
		CommitSHA: "deadbeefcafe1234",
		Kind:      "r",
	})
	req := httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.Routes().ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "unknown manifest kind")

	r, err := deps.NewUoW().ReleaseRepo().Get(context.Background(), "rBadKind")
	require.NoError(t, err)
	assert.Nil(t, r, "no release row should be persisted for a rejected kind")
}

// TestIntegration_ShadowReleaseExposedInDetailAndList verifies that a shadow
// release's shadow flag is exposed by both GET /releases/{id} and its list
// item, and that a release resolved into the terminal 'validated' status
// carries a non-empty resolved_at in its list item — the same as any other
// terminal status.
func TestIntegration_ShadowReleaseExposedInDetailAndList(t *testing.T) {
	srv, deps, db := setup(t)
	defer db.Close()
	ctx := context.Background()

	r := release.Rehydrate(release.RehydrateInput{
		ID:             "rShadowV",
		Status:         release.StatusValidated,
		ChangedService: "service-1",
		ImageTags:      map[string]string{"service-1": "t"},
		Shadow:         true,
		Repo:           "acme/demo",
		CommitSHA:      "deadbeefcafe1234",
		ManifestKind:   release.ManifestKindDbt,
		CreatedAt:      time.Unix(100, 0).UTC(),
		Transitions: []release.Transition{
			{To: release.StatusReceived, At: time.Unix(100, 0).UTC()},
			{To: release.StatusValidating, At: time.Unix(101, 0).UTC()},
			{To: release.StatusValidated, At: time.Unix(102, 0).UTC()},
		},
	})
	require.NoError(t, deps.NewUoW().ReleaseRepo().Save(ctx, r))

	req := httptest.NewRequest(http.MethodGet, "/releases/rShadowV", nil)
	w := httptest.NewRecorder()
	srv.Routes().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var detail map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &detail))
	assert.Equal(t, true, detail["shadow"], "detail response must expose shadow=true")

	reqList := httptest.NewRequest(http.MethodGet, "/releases", nil)
	wList := httptest.NewRecorder()
	srv.Routes().ServeHTTP(wList, reqList)
	require.Equal(t, http.StatusOK, wList.Code)
	var listResp struct {
		Releases []map[string]any `json:"releases"`
	}
	require.NoError(t, json.Unmarshal(wList.Body.Bytes(), &listResp))
	require.Len(t, listResp.Releases, 1)
	item := listResp.Releases[0]
	assert.Equal(t, true, item["shadow"], "list item must expose shadow=true")
	assert.NotEmpty(t, item["resolved_at"], "a validated release's list item must have a non-empty resolved_at")
}
