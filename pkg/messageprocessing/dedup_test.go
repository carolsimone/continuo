package messageprocessing_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	mp "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRepo is an in-memory Repository for unit tests. The map is keyed on
// (messageID, streamName) to match the per-stream dedup invariant: the same
// Redis message_id can appear on multiple streams and must NOT collide.
type fakeRepo struct {
	byKey     map[string]*mp.MessageProcessing
	insertErr error
	getErr    error
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{byKey: map[string]*mp.MessageProcessing{}}
}

func fakeKey(messageID, streamName string) string {
	return messageID + "|" + streamName
}

func (f *fakeRepo) InsertIfNotExists(_ context.Context, m *mp.MessageProcessing) (uuid.UUID, bool, error) {
	if f.insertErr != nil {
		return uuid.Nil, false, f.insertErr
	}
	k := fakeKey(m.MessageID, m.StreamName)
	if existing, ok := f.byKey[k]; ok {
		return existing.ID, false, nil
	}
	id := uuid.New()
	stored := *m
	stored.ID = id
	f.byKey[k] = &stored
	return id, true, nil
}

func (f *fakeRepo) GetByMessageIDAndStream(_ context.Context, messageID, streamName string) (*mp.MessageProcessing, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	m, ok := f.byKey[fakeKey(messageID, streamName)]
	if !ok {
		return nil, errors.New("not found")
	}
	return m, nil
}

func (f *fakeRepo) UpdateState(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestDedup_FirstInsert(t *testing.T) {
	r := newFakeRepo()
	id, dup, err := mp.Dedup(context.Background(), r, newLogger(), "msg-1", "stream:v1", []byte(`{"k":"v"}`))
	require.NoError(t, err)
	assert.False(t, dup)
	assert.NotEqual(t, uuid.Nil, id)
}

func TestDedup_DuplicateCompletedReturnsExistingID(t *testing.T) {
	r := newFakeRepo()
	id1, _, _ := mp.Dedup(context.Background(), r, newLogger(), "msg-1", "stream:v1", []byte(`{}`))
	// flip the stored row to "completed" to exercise the Info path
	r.byKey[fakeKey("msg-1", "stream:v1")].State = "completed"
	id2, dup, err := mp.Dedup(context.Background(), r, newLogger(), "msg-1", "stream:v1", []byte(`{}`))
	require.NoError(t, err)
	assert.True(t, dup)
	assert.Equal(t, id1, id2)
}

func TestDedup_DuplicateInFlightReturnsExistingID(t *testing.T) {
	r := newFakeRepo()
	id1, _, _ := mp.Dedup(context.Background(), r, newLogger(), "msg-1", "stream:v1", []byte(`{}`))
	// leave State as "processing" (the default that InsertIfNotExists set)
	id2, dup, err := mp.Dedup(context.Background(), r, newLogger(), "msg-1", "stream:v1", []byte(`{}`))
	require.NoError(t, err)
	assert.True(t, dup)
	assert.Equal(t, id1, id2)
}

func TestDedup_InsertErrorPropagates(t *testing.T) {
	r := newFakeRepo()
	r.insertErr = errors.New("db down")
	_, _, err := mp.Dedup(context.Background(), r, newLogger(), "msg-1", "stream:v1", []byte(`{}`))
	require.Error(t, err)
}

// TestDedup_SameMessageIDDifferentStreamBothInsertSuccessfully is the
// regression guard for the bug where k8s-controller's outbox-processor
// publishes task.status.updated:v1 and task.execution.recorded:v1 in the
// same millisecond, producing identical Redis message IDs across the two
// streams. With a per-message-only UNIQUE constraint, the second insert
// false-dedupped against the first. The fix includes stream_name in the
// uniqueness key; this test asserts that.
func TestDedup_SameMessageIDDifferentStreamBothInsertSuccessfully(t *testing.T) {
	r := newFakeRepo()
	id1, dup1, err1 := mp.Dedup(context.Background(), r, newLogger(), "msg-1", "stream-a:v1", []byte(`{}`))
	require.NoError(t, err1)
	require.False(t, dup1)
	require.NotEqual(t, uuid.Nil, id1)

	id2, dup2, err2 := mp.Dedup(context.Background(), r, newLogger(), "msg-1", "stream-b:v1", []byte(`{}`))
	require.NoError(t, err2)
	require.False(t, dup2, "same message_id on a different stream must NOT be treated as duplicate")
	require.NotEqual(t, uuid.Nil, id2)
	require.NotEqual(t, id1, id2)
}
