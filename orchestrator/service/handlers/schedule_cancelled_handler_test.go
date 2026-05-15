package handlers_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeCancelledRepo struct {
	inserted  []uuid.UUID
	insertErr error
}

func (f *fakeCancelledRepo) Insert(_ context.Context, id uuid.UUID) error {
	if f.insertErr != nil {
		return f.insertErr
	}
	f.inserted = append(f.inserted, id)
	return nil
}
func (f *fakeCancelledRepo) Exists(_ context.Context, _ uuid.UUID) (bool, error) { return false, nil }
func (f *fakeCancelledRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

var _ repository.CancelledSchedulesRepository = (*fakeCancelledRepo)(nil)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestScheduleCancelledHandler_HappyPath(t *testing.T) {
	repo := &fakeCancelledRepo{}
	h := handlers.NewScheduleCancelledHandler(repo, testLogger())
	id := uuid.New()
	err := h.Handle(context.Background(), domain.ScheduleCancelled{ScheduleID: id})
	require.NoError(t, err)
	require.Len(t, repo.inserted, 1)
	assert.Equal(t, id, repo.inserted[0])
}

func TestScheduleCancelledHandler_RepoErrorPropagates(t *testing.T) {
	repo := &fakeCancelledRepo{insertErr: errors.New("db down")}
	h := handlers.NewScheduleCancelledHandler(repo, testLogger())
	err := h.Handle(context.Background(), domain.ScheduleCancelled{ScheduleID: uuid.New()})
	require.Error(t, err)
}
