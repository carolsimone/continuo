package handlers_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	statehandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/database"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeCatalogRepo struct {
	upsertNames            []string
	upsertManifestVersions map[string]string
}

func (f *fakeCatalogRepo) UpsertAll(_ context.Context, names []string, manifestVersions map[string]string) error {
	f.upsertNames = append([]string(nil), names...)
	f.upsertManifestVersions = manifestVersions
	return nil
}

func (f *fakeCatalogRepo) SoftDeleteAbsent(_ context.Context, _ []string) error { return nil }
func (f *fakeCatalogRepo) ListActive(_ context.Context) ([]string, error)        { return nil, nil }
func (f *fakeCatalogRepo) ExistsActive(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (f *fakeCatalogRepo) GetManifestVersions(_ context.Context, _ string) (map[string]string, error) {
	return map[string]string{}, nil
}

func getScheduleCatalogHandlerTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS processed_events (
			event_id UUID PRIMARY KEY,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM processed_events`)
	})

	return db
}

func TestScheduleCatalogHandler_Handle_PassesManifestVersionsToRepository(t *testing.T) {
	db := getScheduleCatalogHandlerTestDB(t)
	repo := &fakeCatalogRepo{}
	handler := statehandlers.NewScheduleCatalogHandler(
		repo,
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	shouldACK, err := handler.Handle(context.Background(), `{
		"event_id":"4f1f4f31-d8be-4cc3-beb7-e73b784fd4af",
		"schedule_names":["daily","hourly"],
		"service_metadata":{
			"svc-a":{"manifest_version":"v3","image_tag":"sha256:aaa"},
			"svc-b":{"manifest_version":"v5","image_tag":"sha256:bbb"}
		}
	}`)

	require.NoError(t, err)
	assert.True(t, shouldACK)
	assert.ElementsMatch(t, []string{"daily", "hourly"}, repo.upsertNames)
	assert.Equal(t, map[string]string{"svc-a": "v3", "svc-b": "v5"}, repo.upsertManifestVersions)
}
