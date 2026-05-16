package handlers_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCatalogRepo implements postgres.ScheduleCatalogRepository for handler
// tests. It records the arguments passed to UpsertAll and SoftDeleteAbsent;
// the other interface methods are inert stubs because the handler does not
// exercise them.
type fakeCatalogRepo struct {
	upsertNames   []string
	upsertMeta    map[string]model.ServiceMetadata
	softDelete    []string
	upsertErr     error
	softDeleteErr error
}

func (f *fakeCatalogRepo) UpsertAll(_ context.Context, names []string, meta map[string]model.ServiceMetadata) error {
	f.upsertNames = append([]string(nil), names...)
	f.upsertMeta = meta
	return f.upsertErr
}

func (f *fakeCatalogRepo) UpsertAllTx(_ context.Context, _ *sqlx.Tx, _ []string, _ map[string]map[string]model.ServiceMetadata) error {
	return nil
}

func (f *fakeCatalogRepo) SoftDeleteAbsent(_ context.Context, names []string) error {
	f.softDelete = append([]string(nil), names...)
	return f.softDeleteErr
}

func (f *fakeCatalogRepo) SoftDeleteAbsentTx(_ context.Context, _ *sqlx.Tx, _ []string) error {
	return nil
}

func (f *fakeCatalogRepo) ListActive(_ context.Context) ([]string, error) { return nil, nil }

func (f *fakeCatalogRepo) ExistsActive(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (f *fakeCatalogRepo) GetServiceMetadata(_ context.Context, _ string) (map[string]model.ServiceMetadata, error) {
	return map[string]model.ServiceMetadata{}, nil
}

func (f *fakeCatalogRepo) ListAll(_ context.Context) ([]postgres.ScheduleCatalogRow, error) {
	return nil, nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestScheduleCatalogHandler_HappyPath verifies the handler hands the
// schedule names and service metadata from the typed event to UpsertAll
// and then calls SoftDeleteAbsent with the same name set.
func TestScheduleCatalogHandler_HappyPath(t *testing.T) {
	repo := &fakeCatalogRepo{}
	u := &uow.FakeUnitOfWork{ScheduleCatalog: repo}
	h := handlers.NewScheduleCatalogHandler(testLogger())

	evt := events.ScheduleCatalogLoaded{
		EventID:       uuid.New(),
		ScheduleNames: []string{"daily", "hourly"},
		ServiceMetadata: map[string]model.ServiceMetadata{
			"svc-a": {ManifestVersion: "v3", ImageTag: "sha256:aaa"},
			"svc-b": {ManifestVersion: "v5", ImageTag: "sha256:bbb"},
		},
	}
	err := h.Handle(context.Background(), u, evt, uuid.New())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"daily", "hourly"}, repo.upsertNames)
	assert.Equal(t, map[string]model.ServiceMetadata{
		"svc-a": {ManifestVersion: "v3", ImageTag: "sha256:aaa"},
		"svc-b": {ManifestVersion: "v5", ImageTag: "sha256:bbb"},
	}, repo.upsertMeta)
	assert.ElementsMatch(t, []string{"daily", "hourly"}, repo.softDelete)
}

// TestScheduleCatalogHandler_UpsertErrorPropagates verifies that an error
// from UpsertAll bubbles up unchanged and short-circuits SoftDeleteAbsent.
func TestScheduleCatalogHandler_UpsertErrorPropagates(t *testing.T) {
	repo := &fakeCatalogRepo{upsertErr: errors.New("db down")}
	u := &uow.FakeUnitOfWork{ScheduleCatalog: repo}
	h := handlers.NewScheduleCatalogHandler(testLogger())

	err := h.Handle(context.Background(), u, events.ScheduleCatalogLoaded{
		EventID: uuid.New(), ScheduleNames: []string{"s"},
	}, uuid.New())
	require.Error(t, err)
	assert.Nil(t, repo.softDelete, "SoftDeleteAbsent should not be called when UpsertAll fails")
}

// TestScheduleCatalogHandler_SoftDeleteErrorPropagates verifies a
// SoftDeleteAbsent failure surfaces as a handler error.
func TestScheduleCatalogHandler_SoftDeleteErrorPropagates(t *testing.T) {
	repo := &fakeCatalogRepo{softDeleteErr: errors.New("db down")}
	u := &uow.FakeUnitOfWork{ScheduleCatalog: repo}
	h := handlers.NewScheduleCatalogHandler(testLogger())

	err := h.Handle(context.Background(), u, events.ScheduleCatalogLoaded{
		EventID: uuid.New(), ScheduleNames: []string{"s"},
	}, uuid.New())
	require.Error(t, err)
}
