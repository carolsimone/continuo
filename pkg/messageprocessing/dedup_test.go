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

// fakeRepo is an in-memory Repository for unit tests.
type fakeRepo struct {
	byMessageID map[string]*mp.MessageProcessing
	insertErr   error
	getErr      error
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{byMessageID: map[string]*mp.MessageProcessing{}}
}

func (f *fakeRepo) InsertIfNotExists(_ context.Context, m *mp.MessageProcessing) (uuid.UUID, bool, error) {
	if f.insertErr != nil {
		return uuid.Nil, false, f.insertErr
	}
	if existing, ok := f.byMessageID[m.MessageID]; ok {
		return existing.ID, false, nil
	}
	id := uuid.New()
	stored := *m
	stored.ID = id
	f.byMessageID[m.MessageID] = &stored
	return id, true, nil
}

func (f *fakeRepo) GetByMessageID(_ context.Context, messageID string) (*mp.MessageProcessing, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	m, ok := f.byMessageID[messageID]
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
	r.byMessageID["msg-1"].State = "completed"
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
