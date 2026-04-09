package scheduler_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/internal/scheduler"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getActivationTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// fakeActivator stubs PrepareActivation.
type fakeActivator struct {
	tracker *model.SchedulerTracker
	err     error
}

func (f *fakeActivator) ActivateSchedule(_ context.Context, _ string) (uuid.UUID, error) {
	return uuid.Nil, nil
}
func (f *fakeActivator) PrepareActivation(_ context.Context, _ string) (*model.SchedulerTracker, error) {
	return f.tracker, f.err
}

// fakeCreateTxRepo embeds stubRepo and records CreateTx calls.
type fakeCreateTxRepo struct {
	stubRepo
	createdTracker *model.SchedulerTracker
	createTxErr    error
}

func (r *fakeCreateTxRepo) CreateTx(_ context.Context, _ *sqlx.Tx, t *model.SchedulerTracker) error {
	r.createdTracker = t
	return r.createTxErr
}

type fakeCatalogRepoSvc struct {
	manifestVersions map[string]string
	err              error
}

func (f *fakeCatalogRepoSvc) UpsertAll(_ context.Context, _ []string, _ map[string]string) error { return nil }
func (f *fakeCatalogRepoSvc) SoftDeleteAbsent(_ context.Context, _ []string) error                { return nil }
func (f *fakeCatalogRepoSvc) ListActive(_ context.Context) ([]string, error)                      { return nil, nil }
func (f *fakeCatalogRepoSvc) ExistsActive(_ context.Context, _ string) (bool, error)              { return false, nil }
func (f *fakeCatalogRepoSvc) GetManifestVersions(_ context.Context, _ string) (map[string]string, error) {
	return f.manifestVersions, f.err
}

// fakeOutboxRepoSvc records outbox Create calls.
type fakeOutboxRepoSvc struct {
	createdEntry *postgres.OutboxEntry
	createErr    error
}

func (f *fakeOutboxRepoSvc) Create(_ context.Context, _ *sqlx.Tx, e *postgres.OutboxEntry) error {
	f.createdEntry = e
	return f.createErr
}
func (f *fakeOutboxRepoSvc) ListPending(_ context.Context, _ int) ([]*postgres.OutboxEntry, error) {
	return nil, nil
}
func (f *fakeOutboxRepoSvc) MarkPublished(_ context.Context, _ uuid.UUID) error  { return nil }
func (f *fakeOutboxRepoSvc) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }

func TestScheduleActivationService_NoopWhenAlreadyActive(t *testing.T) {
	db := getActivationTestDB(t)
	activator := &fakeActivator{tracker: nil}
	repo := &fakeCreateTxRepo{}
	outbox := &fakeOutboxRepoSvc{}
	svc := scheduler.NewScheduleActivationService(
		db, activator, &fakeCatalogRepoSvc{}, repo, outbox, "scheduler.started:v1", testLogger(),
	)

	id, err := svc.ActivateSchedule(context.Background(), "my-schedule")

	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, id)
	assert.Nil(t, repo.createdTracker, "CreateTx must not be called when already active")
	assert.Nil(t, outbox.createdEntry, "outbox Create must not be called when already active")
}

func TestScheduleActivationService_PropagatesPrepareError(t *testing.T) {
	db := getActivationTestDB(t)
	activator := &fakeActivator{err: errors.New("db check error")}
	svc := scheduler.NewScheduleActivationService(
		db, activator, &fakeCatalogRepoSvc{}, &fakeCreateTxRepo{}, &fakeOutboxRepoSvc{}, "scheduler.started:v1", testLogger(),
	)

	_, err := svc.ActivateSchedule(context.Background(), "my-schedule")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "db check error")
}

func TestScheduleActivationService_AtomicallyWritesTrackerAndOutbox(t *testing.T) {
	db := getActivationTestDB(t)
	tracker := &model.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "my-schedule",
		Status:               model.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}
	activator := &fakeActivator{tracker: tracker}
	repo := &fakeCreateTxRepo{}
	outbox := &fakeOutboxRepoSvc{}
	catalog := &fakeCatalogRepoSvc{manifestVersions: map[string]string{"svc-a": "v3"}}
	svc := scheduler.NewScheduleActivationService(
		db, activator, catalog, repo, outbox, "scheduler.started:v1", testLogger(),
	)

	id, err := svc.ActivateSchedule(context.Background(), "my-schedule")

	require.NoError(t, err)
	assert.Equal(t, tracker.ScheduleID, id)

	require.NotNil(t, repo.createdTracker, "CreateTx must be called")
	assert.Equal(t, tracker.ScheduleID, repo.createdTracker.ScheduleID)
	assert.Equal(t, map[string]string{"svc-a": "v3"}, repo.createdTracker.GetManifestVersions())

	require.NotNil(t, outbox.createdEntry, "outbox Create must be called")
	assert.Equal(t, "scheduler.started:v1", outbox.createdEntry.StreamName)
	assert.Equal(t, "scheduler_started", outbox.createdEntry.EventType)
	assert.Equal(t, tracker.ScheduleID, outbox.createdEntry.AggregateID)
	assert.Contains(t, string(outbox.createdEntry.Payload), tracker.ScheduleID.String())
	assert.Contains(t, string(outbox.createdEntry.Payload), "my-schedule")

	var payload map[string]any
	require.NoError(t, json.Unmarshal(outbox.createdEntry.Payload, &payload))
	assert.Equal(t, map[string]any{"svc-a": "v3"}, payload["manifest_versions"])
}
