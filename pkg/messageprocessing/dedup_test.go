package messageprocessing_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	mp "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRepo is an in-memory Repository for unit tests. It mirrors the
// production uniqueness contract: both (messageID, streamName) AND
// outbox_entry_id (when set) are independent dedup keys. The same Redis
// message_id can appear on multiple streams and must NOT collide; the same
// upstream outbox row republished with a fresh Redis message_id MUST collide.
type fakeRepo struct {
	byKey         map[string]*mp.MessageProcessing
	byOutboxEntry map[uuid.UUID]*mp.MessageProcessing
	byID          map[uuid.UUID]*mp.MessageProcessing
	insertErr     error
	getErr        error
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		byKey:         map[string]*mp.MessageProcessing{},
		byOutboxEntry: map[uuid.UUID]*mp.MessageProcessing{},
		byID:          map[uuid.UUID]*mp.MessageProcessing{},
	}
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
	if m.OutboxEntryID != nil {
		if existing, ok := f.byOutboxEntry[*m.OutboxEntryID]; ok {
			return existing.ID, false, nil
		}
	}
	id := uuid.New()
	stored := *m
	stored.ID = id
	f.byKey[k] = &stored
	f.byID[id] = &stored
	if m.OutboxEntryID != nil {
		f.byOutboxEntry[*m.OutboxEntryID] = &stored
	}
	return id, true, nil
}

func (f *fakeRepo) AlreadyProcessed(_ context.Context, messageID, streamName string, outboxEntryID *uuid.UUID) (bool, error) {
	if f.getErr != nil {
		return false, f.getErr
	}
	if _, ok := f.byKey[fakeKey(messageID, streamName)]; ok {
		return true, nil
	}
	if outboxEntryID != nil {
		// Scope the outbox axis by stream_name, matching the table's
		// (outbox_entry_id, stream_name) dedup identity: a match on another
		// stream must not report this stream's message as processed.
		if existing, ok := f.byOutboxEntry[*outboxEntryID]; ok && existing.StreamName == streamName {
			return true, nil
		}
	}
	return false, nil
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

func (f *fakeRepo) GetByID(_ context.Context, id uuid.UUID) (*mp.MessageProcessing, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	m, ok := f.byID[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return m, nil
}

func (f *fakeRepo) UpdateState(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (f *fakeRepo) DeleteTerminalOlderThan(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return 0, nil
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

// TestDedupWithOutboxEntryID_SameOutboxEntryFreshMessageIDIsDuplicate is the
// regression guard for the P1 correctness gap raised against pkg/outbox.
// Scenario: pkg/outbox.Processor publishes an outbox row (XADD succeeds, gets
// Redis msg_id=A) but then crashes before MarkProcessed; on the next tick it
// republishes the same row (XADD succeeds, gets fresh Redis msg_id=B). With
// only (message_id, stream_name) dedup the consumer sees msg_id=B as a brand
// new message and runs the handler twice. The fix is the outbox_entry_id
// secondary uniqueness key, which catches the redelivery because both XADDs
// carry the same upstream outbox row UUID.
func TestDedupWithOutboxEntryID_SameOutboxEntryFreshMessageIDIsDuplicate(t *testing.T) {
	r := newFakeRepo()
	entryID := uuid.New()

	// First delivery: msg_id=A, outbox_entry_id=entryID.
	id1, dup1, err := mp.DedupWithOutboxEntryID(
		context.Background(), r, newLogger(),
		"msg-A", "stream:v1", []byte(`{}`), &entryID)
	require.NoError(t, err)
	require.False(t, dup1)
	require.NotEqual(t, uuid.Nil, id1)

	// Mark first delivery completed so the Info log path is exercised on dup.
	r.byID[id1].State = "completed"

	// Second delivery: DIFFERENT msg_id but SAME outbox_entry_id (the
	// Processor-crash redelivery case).
	id2, dup2, err := mp.DedupWithOutboxEntryID(
		context.Background(), r, newLogger(),
		"msg-B", "stream:v1", []byte(`{}`), &entryID)
	require.NoError(t, err)
	require.True(t, dup2, "different msg_id but same outbox_entry_id MUST be treated as duplicate")
	require.Equal(t, id1, id2, "must return the existing row's ID, not a fresh one")
}

// TestDedupWithOutboxEntryID_NilOutboxEntryFallsBackToMessageIDStream verifies
// the backward-compat path: when outboxEntryID is nil (HTTP triggers,
// scheduler ticks, anything not originating from pkg/outbox), behavior is
// identical to Dedup.
func TestDedupWithOutboxEntryID_NilOutboxEntryFallsBackToMessageIDStream(t *testing.T) {
	r := newFakeRepo()

	id1, dup1, err := mp.DedupWithOutboxEntryID(
		context.Background(), r, newLogger(),
		"msg-1", "stream:v1", []byte(`{}`), nil)
	require.NoError(t, err)
	require.False(t, dup1)

	// Second delivery: same msg_id+stream — duplicate via primary key.
	id2, dup2, err := mp.DedupWithOutboxEntryID(
		context.Background(), r, newLogger(),
		"msg-1", "stream:v1", []byte(`{}`), nil)
	require.NoError(t, err)
	require.True(t, dup2)
	require.Equal(t, id1, id2)
}

// TestDedupWithOutboxEntryID_DifferentOutboxEntriesAreNotDuplicates verifies
// outbox_entry_id is treated as a uniqueness key, not just an arbitrary tag:
// two messages with different outbox_entry_ids on the same stream with
// different msg_ids must both insert successfully.
func TestDedupWithOutboxEntryID_DifferentOutboxEntriesAreNotDuplicates(t *testing.T) {
	r := newFakeRepo()
	entry1 := uuid.New()
	entry2 := uuid.New()

	id1, dup1, err := mp.DedupWithOutboxEntryID(
		context.Background(), r, newLogger(),
		"msg-A", "stream:v1", []byte(`{}`), &entry1)
	require.NoError(t, err)
	require.False(t, dup1)

	id2, dup2, err := mp.DedupWithOutboxEntryID(
		context.Background(), r, newLogger(),
		"msg-B", "stream:v1", []byte(`{}`), &entry2)
	require.NoError(t, err)
	require.False(t, dup2)
	require.NotEqual(t, id1, id2)
}
