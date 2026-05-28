//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	httpinfra "github.com/carolsimone/continuo/release-controller/adapters/http"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (*httpinfra.Server, *handlers.Deps, *sqlx.DB) {
	t.Helper()
	dsn := os.Getenv("RELEASE_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("RELEASE_TEST_PG_DSN not set")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	_, err = db.Exec("TRUNCATE releases, current_prod, release_controller_outbox, message_processing RESTART IDENTITY CASCADE")
	require.NoError(t, err)
	deps := &handlers.Deps{
		UoW:       postgres.NewUnitOfWork(db, slog.Default()),
		Clock:     ports.SystemClock{},
		Telemetry: ports.NoOpTelemetry{},
		Logger:    slog.Default(),
	}
	srv := httpinfra.NewServer(deps, "0", slog.Default())
	return srv, deps, db
}

func TestIntegration_HappyPath(t *testing.T) {
	srv, deps, db := setup(t)
	defer db.Close()

	// 1. POST /releases
	body, _ := json.Marshal(handlers.ReceiveCandidateInput{
		ReleaseID:      "rA",
		ChangedNodeIDs: []string{"a"},
		ImageTags:      map[string]string{"service-1": "sha-rA"},
		ManifestsURI:   "s3://b/r/rA/manifests/",
	})
	req := httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.Routes().ServeHTTP(w, req)
	assert.Equal(t, http.StatusAccepted, w.Code)

	// 2. Verify release is Parsing (AdvanceQueue ran on POST)
	r, err := deps.UoW.ReleaseRepo().Get(context.Background(), "rA")
	require.NoError(t, err)
	assert.Equal(t, release.StatusParsing, r.Status())

	// 3. Simulate manifest-controller reply
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "a", ServiceName: "service-1"},
			{UniqueID: "b", ServiceName: "service-1", UpstreamUniqueIDs: []string{"a"}},
		},
	}))
	r, _ = deps.UoW.ReleaseRepo().Get(context.Background(), "rA")
	assert.Equal(t, release.StatusValidating, r.Status())

	// 4. Simulate validation result
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID: "rA",
		PerNodeResults: []handlers.NodeResult{
			{NodeID: "a", Status: "ok"}, {NodeID: "b", Status: "ok"},
		},
		AggregateStatus: "ok",
	}))
	r, _ = deps.UoW.ReleaseRepo().Get(context.Background(), "rA")
	assert.Equal(t, release.StatusPromoted, r.Status())

	// 5. GET /current-prod
	req = httptest.NewRequest(http.MethodGet, "/current-prod", nil)
	w = httptest.NewRecorder()
	srv.Routes().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var cp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &cp))
	assert.Equal(t, "rA", cp["current_prod_release_id"])

	// 6. Outbox has 3 entries (release.requested + validation.requested + release.promoted)
	var count int
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM release_controller_outbox`))
	assert.Equal(t, 3, count)
}
