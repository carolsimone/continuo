package redis

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// dedupStateChange is one UpdateState call, with how many commits the unit of
// work had made when it happened.
type dedupStateChange struct {
	id            uuid.UUID
	state         string
	commitsBefore int
}

// recordingDedupRepo claims a fresh dedup row on every insert, or reports a
// duplicate when dup is set, and records each state change against the unit
// of work's commit count.
type recordingDedupRepo struct {
	u         *uow.FakeUnitOfWork
	dup       bool
	claimed   uuid.UUID
	changes   []dedupStateChange
	updateErr error
}

var _ messageprocessing.Repository = (*recordingDedupRepo)(nil)

func (r *recordingDedupRepo) InsertIfNotExists(context.Context, *messageprocessing.MessageProcessing) (uuid.UUID, bool, error) {
	r.claimed = uuid.New()
	return r.claimed, !r.dup, nil
}

func (r *recordingDedupRepo) AlreadyProcessed(context.Context, string, string, *uuid.UUID) (bool, error) {
	return r.dup, nil
}

func (r *recordingDedupRepo) GetByMessageIDAndStream(context.Context, string, string) (*messageprocessing.MessageProcessing, error) {
	return &messageprocessing.MessageProcessing{ID: r.claimed, State: messageprocessing.StateCompleted}, nil
}

func (r *recordingDedupRepo) GetByID(_ context.Context, id uuid.UUID) (*messageprocessing.MessageProcessing, error) {
	return &messageprocessing.MessageProcessing{ID: id, State: messageprocessing.StateCompleted}, nil
}

func (r *recordingDedupRepo) UpdateState(_ context.Context, id uuid.UUID, state string) error {
	r.changes = append(r.changes, dedupStateChange{id: id, state: state, commitsBefore: r.u.CommitCalled})
	return r.updateErr
}

func (r *recordingDedupRepo) DeleteOlderThan(context.Context, time.Duration, int) (int64, error) {
	return 0, nil
}

// newRecordingUnitOfWork returns a FakeUnitOfWork whose dedup repository is a
// recordingDedupRepo.
func newRecordingUnitOfWork(dup bool) (*uow.FakeUnitOfWork, *recordingDedupRepo) {
	u := &uow.FakeUnitOfWork{}
	repo := &recordingDedupRepo{u: u, dup: dup}
	u.MessageProcessing = repo
	return u, repo
}

// testStreamBinding binds a plain-string event to handle. Its stream name is a
// test label, not a contract stream.
func testStreamBinding(handle func(context.Context, uow.UnitOfWork, string, uuid.UUID) error) streamBinding[string] {
	return streamBinding[string]{
		label:      "bind-stream-handler-test",
		streamName: "bind-stream-handler-test",
		parse:      func(msg goredis.XMessage) (string, error) { return msg.ID, nil },
		payload:    defaultPayload,
		handle:     handle,
	}
}

func bindTestMessage() goredis.XMessage {
	return goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "{}"}}
}

func bindTestLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestBindStreamHandler_MarksDedupRowCompletedBeforeCommit: a handled
// message's dedup row is marked completed inside the handler's transaction,
// before the commit.
func TestBindStreamHandler_MarksDedupRowCompletedBeforeCommit(t *testing.T) {
	u, repo := newRecordingUnitOfWork(false)
	var handledID uuid.UUID
	h := bindStreamHandler(func() uow.UnitOfWork { return u }, bindTestLogger(),
		testStreamBinding(func(_ context.Context, _ uow.UnitOfWork, _ string, id uuid.UUID) error {
			handledID = id
			return nil
		}))

	require.NoError(t, h(context.Background(), bindTestMessage()))

	require.Equal(t, repo.claimed, handledID, "the handler receives the claimed dedup row id")
	require.Equal(t, []dedupStateChange{{id: repo.claimed, state: messageprocessing.StateCompleted, commitsBefore: 0}}, repo.changes)
	require.Equal(t, 1, u.CommitCalled)
	require.Equal(t, 0, u.RollbackCalled)
}

// TestBindStreamHandler_HandlerErrorLeavesDedupRowUnmarked: a failed handler
// rolls its claim back with everything else, so nothing is marked.
func TestBindStreamHandler_HandlerErrorLeavesDedupRowUnmarked(t *testing.T) {
	u, repo := newRecordingUnitOfWork(false)
	boom := errors.New("boom")
	h := bindStreamHandler(func() uow.UnitOfWork { return u }, bindTestLogger(),
		testStreamBinding(func(context.Context, uow.UnitOfWork, string, uuid.UUID) error { return boom }))

	require.ErrorIs(t, h(context.Background(), bindTestMessage()), boom)
	require.Empty(t, repo.changes)
	require.Equal(t, 0, u.CommitCalled)
	require.Equal(t, 1, u.RollbackCalled)
}

// TestBindStreamHandler_DuplicateIsNotMarkedAgain: a duplicate commits the
// empty transaction without invoking the handler or touching the row.
func TestBindStreamHandler_DuplicateIsNotMarkedAgain(t *testing.T) {
	u, repo := newRecordingUnitOfWork(true)
	handled := false
	h := bindStreamHandler(func() uow.UnitOfWork { return u }, bindTestLogger(),
		testStreamBinding(func(context.Context, uow.UnitOfWork, string, uuid.UUID) error {
			handled = true
			return nil
		}))

	require.NoError(t, h(context.Background(), bindTestMessage()))
	require.False(t, handled)
	require.Empty(t, repo.changes)
	require.Equal(t, 1, u.CommitCalled)
}

// TestBindStreamHandler_CompletionErrorRollsBack verifies that a failure to
// mark a handled message completed prevents the transaction from committing.
func TestBindStreamHandler_CompletionErrorRollsBack(t *testing.T) {
	u, repo := newRecordingUnitOfWork(false)
	boom := errors.New("completion failed")
	repo.updateErr = boom
	h := bindStreamHandler(func() uow.UnitOfWork { return u }, bindTestLogger(),
		testStreamBinding(func(context.Context, uow.UnitOfWork, string, uuid.UUID) error { return nil }))

	err := h(context.Background(), bindTestMessage())
	require.ErrorIs(t, err, boom)
	require.ErrorContains(t, err, "mark completed")
	require.Equal(t, []dedupStateChange{{id: repo.claimed, state: messageprocessing.StateCompleted, commitsBefore: 0}}, repo.changes)
	require.Equal(t, 0, u.CommitCalled)
	require.Equal(t, 1, u.RollbackCalled)
}
