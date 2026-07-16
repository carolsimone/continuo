package handlers_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// releaseCall records one ReleaseSlot invocation.
type releaseCall struct {
	id uuid.UUID
	at time.Time
}

// stubReleasingRepo records ReleaseSlot calls and replays a scripted result, so
// a test can drive the already-released and error paths.
type stubReleasingRepo struct {
	*stubDeploymentsRepo
	calls    []releaseCall
	released bool
	err      error
}

func (r *stubReleasingRepo) ReleaseSlot(_ context.Context, id uuid.UUID, at time.Time) (bool, error) {
	r.calls = append(r.calls, releaseCall{id: id, at: at})
	return r.released, r.err
}

func newTerminalHandler() (*handlers.JobTerminalHandler, *stubReleasingRepo, *stubOutboxRepo, *uow.FakeUnitOfWork) {
	repo := &stubReleasingRepo{stubDeploymentsRepo: &stubDeploymentsRepo{}, released: true}
	outbox := &stubOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: repo, Outbox: outbox}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return handlers.NewJobTerminalHandler(logger), repo, outbox, u
}

func terminalEvent(depID uuid.UUID) events.ExecutorJobTerminal {
	return events.ExecutorJobTerminal{
		ExecutorDeploymentID: depID,
		JobName:              "job-1",
		TerminalStatus:       "succeeded",
		CompletedAt:          time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC),
	}
}

// TestJobTerminalHandler_ReleasesTheSlot is the whole point of the event: the
// Job has settled, so the capacity it held returns to the budget.
func TestJobTerminalHandler_ReleasesTheSlot(t *testing.T) {
	h, repo, _, u := newTerminalHandler()
	depID := uuid.New()

	require.NoError(t, h.Handle(context.Background(), u, terminalEvent(depID), uuid.Nil))

	require.Len(t, repo.calls, 1)
	assert.Equal(t, depID, repo.calls[0].id)
	assert.Equal(t, time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC), repo.calls[0].at.UTC(),
		"the slot is released as of when the Job settled, not when the event was consumed")
}

// TestJobTerminalHandler_AlreadyReleasedIsAccepted keeps a duplicate observation
// idempotent: at-least-once delivery means the same terminal can arrive twice,
// and the second must ACK rather than fail and redeliver forever.
func TestJobTerminalHandler_AlreadyReleasedIsAccepted(t *testing.T) {
	h, repo, _, u := newTerminalHandler()
	repo.released = false // the row held no slot

	require.NoError(t, h.Handle(context.Background(), u, terminalEvent(uuid.New()), uuid.Nil),
		"a slot already released is a no-op, not an error")
}

// TestJobTerminalHandler_WritesNoLifecycleEvents pins the event's scope. It is
// capacity accounting only; task and node lifecycle belong to the streams that
// carry the Job's actual outcome, and writing them here would double-report it.
func TestJobTerminalHandler_WritesNoLifecycleEvents(t *testing.T) {
	h, _, outbox, u := newTerminalHandler()

	require.NoError(t, h.Handle(context.Background(), u, terminalEvent(uuid.New()), uuid.Nil))

	assert.Empty(t, outbox.entries, "releasing a slot announces nothing")
}

// TestJobTerminalHandler_ReleaseErrorPropagates keeps a slot from being lost to
// a transient database failure: the message must stay pending and be retried.
func TestJobTerminalHandler_ReleaseErrorPropagates(t *testing.T) {
	h, repo, _, u := newTerminalHandler()
	repo.err = errors.New("connection reset")

	assert.Error(t, h.Handle(context.Background(), u, terminalEvent(uuid.New()), uuid.Nil))
}

// TestJobTerminalHandler_ZeroCompletedAtFallsBackToNow keeps a Job that reported
// no completion instant releasable with a sane released-at stamp.
func TestJobTerminalHandler_ZeroCompletedAtFallsBackToNow(t *testing.T) {
	h, repo, _, u := newTerminalHandler()
	evt := terminalEvent(uuid.New())
	evt.CompletedAt = time.Time{}

	before := time.Now()
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.Nil))

	require.Len(t, repo.calls, 1)
	assert.False(t, repo.calls[0].at.IsZero(), "a released slot must carry a real instant")
	assert.False(t, repo.calls[0].at.Before(before))
}
