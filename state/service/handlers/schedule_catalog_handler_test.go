package handlers_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	statehandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeCatalogRepo struct {
	upsertNames         []string
	upsertServiceMetadata map[string]model.ServiceMetadata
}

func (f *fakeCatalogRepo) UpsertAll(_ context.Context, names []string, serviceMetadata map[string]model.ServiceMetadata) error {
	f.upsertNames = append([]string(nil), names...)
	f.upsertServiceMetadata = serviceMetadata
	return nil
}

func (f *fakeCatalogRepo) SoftDeleteAbsent(_ context.Context, _ []string) error { return nil }
func (f *fakeCatalogRepo) ListActive(_ context.Context) ([]string, error)        { return nil, nil }
func (f *fakeCatalogRepo) ExistsActive(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (f *fakeCatalogRepo) GetServiceMetadata(_ context.Context, _ string) (map[string]model.ServiceMetadata, error) {
	return map[string]model.ServiceMetadata{}, nil
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

func TestScheduleCatalogHandler_Handle_PassesServiceMetadataToRepository(t *testing.T) {
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
	assert.Equal(t, map[string]model.ServiceMetadata{
		"svc-a": {ManifestVersion: "v3", ImageTag: "sha256:aaa"},
		"svc-b": {ManifestVersion: "v5", ImageTag: "sha256:bbb"},
	}, repo.upsertServiceMetadata)
}
