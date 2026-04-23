package scheduler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/carolsimone/continuo/state/internal/scheduler"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubRepo is a minimal fake SchedulerTrackerRepository for activator unit tests.
type stubRepo struct {
	hasActive    bool
	hasActiveErr error
	created      *model.SchedulerTracker
	createErr    error
}

func (r *stubRepo) HasActiveSchedule(_ context.Context, _ string) (bool, error) {
	return r.hasActive, r.hasActiveErr
}
func (r *stubRepo) Create(_ context.Context, t *model.SchedulerTracker) error {
	r.created = t
	return r.createErr
}
func (r *stubRepo) GetByID(_ context.Context, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return nil, nil
}
func (r *stubRepo) Update(_ context.Context, _ *model.SchedulerTracker) error { return nil }
func (r *stubRepo) Cancel(_ context.Context, _ uuid.UUID, _, _ string) error  { return nil }
func (r *stubRepo) List(_ context.Context, _ postgres.SchedulerFilters) ([]*model.SchedulerTracker, int, error) {
	return nil, 0, nil
}
func (r *stubRepo) UpdateInitializationStatus(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (r *stubRepo) ResetInProgressInitializations(_ context.Context) (int, error) { return 0, nil }
func (r *stubRepo) GetLastRunPerSchedule(_ context.Context) (map[string]postgres.LastRunData, error) {
	return nil, nil
}
func (r *stubRepo) GetActiveScheduler(_ context.Context, _ string) (*model.SchedulerTracker, error) {
	return nil, nil
}
func (r *stubRepo) UpdateTx(_ context.Context, _ *sqlx.Tx, _ *model.SchedulerTracker) error {
	return nil
}
func (r *stubRepo) UpdateInitializationStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (r *stubRepo) CreateTx(_ context.Context, _ *sqlx.Tx, _ *model.SchedulerTracker) error {
	return nil
}
func (r *stubRepo) IncrementTerminalCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (int32, int32, error) {
	return 0, 0, nil
}
func (r *stubRepo) DecrementTerminalCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ int32) error {
	return nil
}
func (r *stubRepo) SetTotalTaskCountTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ int32) error {
	return nil
}
func (r *stubRepo) UpdateStatusTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (r *stubRepo) GetByIDForUpdateTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID) (*model.SchedulerTracker, error) {
	return nil, nil
}
func (r *stubRepo) FinalizeRunTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _ string) error {
	return nil
}
func (r *stubRepo) CancelTx(_ context.Context, _ *sqlx.Tx, _ uuid.UUID, _, _ string) error {
	return nil
}

func TestPrepareActivation_ReturnsTracker_WhenNoActiveSchedule(t *testing.T) {
	a := scheduler.NewScheduleActivator(&stubRepo{hasActive: false}, testLogger())

	tracker, err := a.PrepareActivation(context.Background(), "my-schedule")

	require.NoError(t, err)
	require.NotNil(t, tracker)
	assert.Equal(t, "my-schedule", tracker.ScheduleName)
	assert.Equal(t, model.SchedulerStatusPending, tracker.Status)
	assert.Equal(t, "pending", tracker.InitializationStatus)
	assert.NotEqual(t, uuid.Nil, tracker.ScheduleID)
	assert.WithinDuration(t, time.Now(), tracker.CreatedAt, time.Second)
}

func TestPrepareActivation_ReturnsNil_WhenActiveScheduleExists(t *testing.T) {
	a := scheduler.NewScheduleActivator(&stubRepo{hasActive: true}, testLogger())

	tracker, err := a.PrepareActivation(context.Background(), "my-schedule")

	require.NoError(t, err)
	assert.Nil(t, tracker)
}

func TestPrepareActivation_PropagatesRepoError(t *testing.T) {
	a := scheduler.NewScheduleActivator(&stubRepo{hasActiveErr: errors.New("db error")}, testLogger())

	_, err := a.PrepareActivation(context.Background(), "my-schedule")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to check for active schedule")
}
