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

func (f *fakeActivator) ActivateSchedule(_ context.Context, _ string, _ string, _ *uuid.UUID) (uuid.UUID, error) {
	return uuid.Nil, nil
}
func (f *fakeActivator) PrepareActivation(_ context.Context, _ string, _ string, _ *uuid.UUID) (*model.SchedulerTracker, error) {
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
	serviceMetadata map[string]model.ServiceMetadata
	err             error
}

func (f *fakeCatalogRepoSvc) UpsertAll(_ context.Context, _ []string, _ map[string]model.ServiceMetadata) error {
	return nil
}
func (f *fakeCatalogRepoSvc) SoftDeleteAbsent(_ context.Context, _ []string) error   { return nil }
func (f *fakeCatalogRepoSvc) ListActive(_ context.Context) ([]string, error)          { return nil, nil }
func (f *fakeCatalogRepoSvc) ExistsActive(_ context.Context, _ string) (bool, error)  { return false, nil }
func (f *fakeCatalogRepoSvc) GetServiceMetadata(_ context.Context, _ string) (map[string]model.ServiceMetadata, error) {
	return f.serviceMetadata, f.err
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

	id, err := svc.ActivateSchedule(context.Background(), "my-schedule", "cron", nil)

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

	_, err := svc.ActivateSchedule(context.Background(), "my-schedule", "cron", nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "db check error")
}

func TestActivationService_TransitionsInitStatusAtomically(t *testing.T) {
	db := getActivationTestDB(t)
	ctx := context.Background()

	scheduleID := uuid.New()
	tracker := &model.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "atomic-init-test",
		Status:               model.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}

	activator := &fakeActivator{tracker: tracker}
	logger := testLogger()
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	catalog := &fakeCatalogRepoSvc{}

	svc := scheduler.NewScheduleActivationService(
		db, activator, catalog, schedulerRepo, outboxRepo, "scheduler.started:v1", logger,
	)

	id, err := svc.ActivateSchedule(ctx, "atomic-init-test", "cron", nil)
	require.NoError(t, err)
	assert.Equal(t, scheduleID, id)

	// Verify initialization_status was set to 'in_progress' in the DB.
	var initStatus string
	err = db.QueryRowContext(ctx,
		`SELECT initialization_status FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID,
	).Scan(&initStatus)
	require.NoError(t, err, "scheduler_tracker row must exist after ActivateSchedule")
	assert.Equal(t, "in_progress", initStatus, "initialization_status must be 'in_progress' after commit")

	// Verify outbox entry exists.
	var outboxCount int
	err = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM state_outbox WHERE aggregate_id = $1 AND stream_name = 'scheduler.started:v1'`, scheduleID,
	).Scan(&outboxCount)
	require.NoError(t, err)
	assert.Equal(t, 1, outboxCount, "outbox entry must exist for scheduler.started:v1")

	// Cleanup.
	t.Cleanup(func() {
		db.ExecContext(ctx, `DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
		db.ExecContext(ctx, `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
	})
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
	catalog := &fakeCatalogRepoSvc{serviceMetadata: map[string]model.ServiceMetadata{"svc-a": {ManifestVersion: "v3", ImageTag: ""}}}
	svc := scheduler.NewScheduleActivationService(
		db, activator, catalog, repo, outbox, "scheduler.started:v1", testLogger(),
	)

	id, err := svc.ActivateSchedule(context.Background(), "my-schedule", "cron", nil)

	require.NoError(t, err)
	assert.Equal(t, tracker.ScheduleID, id)

	require.NotNil(t, repo.createdTracker, "CreateTx must be called")
	assert.Equal(t, tracker.ScheduleID, repo.createdTracker.ScheduleID)
	assert.Equal(t, map[string]model.ServiceMetadata{"svc-a": {ManifestVersion: "v3", ImageTag: ""}}, repo.createdTracker.GetServiceMetadata())

	require.NotNil(t, outbox.createdEntry, "outbox Create must be called")
	assert.Equal(t, "scheduler.started:v1", outbox.createdEntry.StreamName)
	assert.Equal(t, "scheduler_started", outbox.createdEntry.EventType)
	assert.Equal(t, tracker.ScheduleID, outbox.createdEntry.AggregateID)
	assert.Contains(t, string(outbox.createdEntry.Payload), tracker.ScheduleID.String())
	assert.Contains(t, string(outbox.createdEntry.Payload), "my-schedule")

	var payload map[string]any
	require.NoError(t, json.Unmarshal(outbox.createdEntry.Payload, &payload))
	// service_metadata is a nested object now: {"svc-a": {"manifest_version": "v3", "image_tag": ""}}
	svcMeta, ok := payload["service_metadata"].(map[string]any)
	require.True(t, ok, "service_metadata must be a map in the outbox payload")
	svcA, ok := svcMeta["svc-a"].(map[string]any)
	require.True(t, ok, "svc-a must be a map in service_metadata")
	assert.Equal(t, "v3", svcA["manifest_version"])
}

func TestActivationService_OutboxPayloadCarriesKindAndSourceRunID(t *testing.T) {
	db := getActivationTestDB(t)
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, testLogger())
	catalogRepo := postgres.NewScheduleCatalogRepository(db, testLogger())
	outboxRepo := postgres.NewOutboxRepository(db, testLogger())
	activator := scheduler.NewScheduleActivator(schedulerRepo, testLogger())
	svc := scheduler.NewScheduleActivationService(
		db, activator, catalogRepo, schedulerRepo, outboxRepo,
		"scheduler.started:v1", testLogger(),
	)

	scheduleName := "obx-" + uuid.New().String()[:8]
	require.NoError(t, catalogRepo.UpsertAll(context.Background(), []string{scheduleName}, map[string]model.ServiceMetadata{}))

	sourceRunID := uuid.New()
	scheduleID, err := svc.ActivateSchedule(context.Background(), scheduleName, "rerun", &sourceRunID)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, scheduleID)

	// Read back the outbox row and assert payload contains kind + source_run_id.
	var rawPayload []byte
	require.NoError(t, db.GetContext(context.Background(), &rawPayload,
		`SELECT payload FROM state_outbox WHERE aggregate_id = $1 ORDER BY created_at DESC LIMIT 1`, scheduleID))
	var outboxPayload map[string]interface{}
	require.NoError(t, json.Unmarshal(rawPayload, &outboxPayload))
	assert.Equal(t, "rerun", outboxPayload["kind"])
	assert.Equal(t, sourceRunID.String(), outboxPayload["source_run_id"])

	// Assert the tracker row also has kind + source_run_id.
	tracker, err := schedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, "rerun", tracker.Kind)
	require.NotNil(t, tracker.SourceRunID)
	assert.Equal(t, sourceRunID, *tracker.SourceRunID)

	// Cleanup.
	t.Cleanup(func() {
		db.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
		db.ExecContext(context.Background(), `DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
		db.ExecContext(context.Background(), `DELETE FROM schedule_catalog WHERE schedule_name = $1`, scheduleName)
	})
}
