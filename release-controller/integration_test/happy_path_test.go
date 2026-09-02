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

	"github.com/carolsimone/continuo/pkg/liveness"
	httpinfra "github.com/carolsimone/continuo/release-controller/adapters/http"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
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
	_, err = db.Exec("TRUNCATE releases, current_prod, release_controller_outbox, message_processing, service_prod RESTART IDENTITY CASCADE")
	require.NoError(t, err)
	deps := &handlers.Deps{
		NewUoW:    func() uow.UnitOfWork { return postgres.NewUnitOfWork(db, slog.Default(), nil) },
		Clock:     ports.SystemClock{},
		Telemetry: ports.NoOpTelemetry{},
		Logger:    slog.Default(),
		Bucket:    "test-bucket",
	}
	srv := httpinfra.NewServer(deps, liveness.NewRegistry(), "0", slog.Default())
	return srv, deps, db
}

func TestIntegration_HappyPath(t *testing.T) {
	srv, deps, db := setup(t)
	defer db.Close()

	// 1. POST /releases
	body, _ := json.Marshal(handlers.ReceiveCandidateInput{
		Service:   "service-1",
		ReleaseID: "rA",
		ImageTag:  "sha-rA",
		Repo:      "acme/demo",
		CommitSHA: "deadbeefcafe1234",
	})
	req := httptest.NewRequest(http.MethodPost, "/releases", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.Routes().ServeHTTP(w, req)
	assert.Equal(t, http.StatusAccepted, w.Code)

	// 2. Verify release is Compiling (AdvanceQueue ran on POST, promoting the
	// release into the compile leg rather than straight to Parsing).
	r, err := deps.NewUoW().RunRepo().Get(context.Background(), "rA")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusCompiling, r.Status())

	// 2b. Simulate the dbt compile job completing ok: Compiling -> Parsing.
	require.NoError(t, handlers.HandleCompileResult(context.Background(), deps, handlers.HandleCompileResultInput{
		ReleaseID: "rA",
		Status:    "ok",
	}))
	r, err = deps.NewUoW().RunRepo().Get(context.Background(), "rA")
	require.NoError(t, err)
	assert.Equal(t, pipeline.StatusParsing, r.Status())

	// 3. Simulate topology-controller reply
	require.NoError(t, handlers.HandleParsedManifest(context.Background(), deps, handlers.HandleParsedManifestInput{
		ReleaseID: "rA",
		Status:    "ok",
		Topology: release.Topology{
			{UniqueID: "a", ServiceName: "service-1"},
			{UniqueID: "b", ServiceName: "service-1", UpstreamUniqueIDs: []string{"a"}},
		},
	}))
	r, _ = deps.NewUoW().RunRepo().Get(context.Background(), "rA")
	assert.Equal(t, pipeline.StatusValidating, r.Status())

	// 4. Project each node's result via the kind=node messages, then deliver the
	// terminal decision. The kind=complete terminal message carries only the
	// decision; it reads back what the per-node messages stored.
	for _, n := range []handlers.NodeValidationResultInput{
		{ReleaseID: "rA", Stage: "validation", NodeID: "a", Status: "ok"},
		{ReleaseID: "rA", Stage: "validation", NodeID: "b", Status: "ok"},
	} {
		require.NoError(t, handlers.HandleNodeValidationResult(context.Background(), deps, n))
	}
	require.NoError(t, handlers.HandleValidationResult(context.Background(), deps, handlers.HandleValidationResultInput{
		ReleaseID:       "rA",
		AggregateStatus: "ok",
	}))
	r, _ = deps.NewUoW().RunRepo().Get(context.Background(), "rA")
	assert.Equal(t, pipeline.StatusPromoted, r.Status())
	require.NotNil(t, r)
	require.NotEmpty(t, r.PerNodeResults())
	assert.Equal(t, "ok", r.PerNodeResults()[0].Status)

	// 5. GET /current-prod
	req = httptest.NewRequest(http.MethodGet, "/current-prod", nil)
	w = httptest.NewRecorder()
	srv.Routes().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var cp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &cp))
	assert.Equal(t, "rA", cp["current_prod_release_id"])

	// 6. service_prod must be upserted for service-1.
	var spCount int
	require.NoError(t, db.Get(&spCount, `SELECT count(*) FROM service_prod WHERE service_name = 'service-1'`))
	assert.Equal(t, 1, spCount, "service_prod must have a row for the promoted service")

	// 7. Outbox has 4 entries (compile.requested + release.requested +
	// validation.requested + release.promoted)
	var count int
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM release_controller_outbox`))
	assert.Equal(t, 4, count)
}
