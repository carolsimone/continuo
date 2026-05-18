// executor-controller/service/handlers/schedule_cancelled_handler_test.go
package handlers_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingCancelledRepo struct {
	mu       sync.Mutex
	inserted []uuid.UUID
}

func (r *recordingCancelledRepo) Insert(_ context.Context, id uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inserted = append(r.inserted, id)
	return nil
}
func (r *recordingCancelledRepo) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (r *recordingCancelledRepo) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

func TestScheduleCancelledHandler_InsertsCancelledRow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cancelled := &recordingCancelledRepo{}
	u := newFakeUoW(nil, cancelled)

	id := uuid.New()
	h := handlers.NewScheduleCancelledHandler(logger)
	err := h.Handle(context.Background(), u, events.ScheduleCancelled{ScheduleID: id}, uuid.New())
	require.NoError(t, err)
	require.Len(t, cancelled.inserted, 1)
	assert.Equal(t, id, cancelled.inserted[0])
}
