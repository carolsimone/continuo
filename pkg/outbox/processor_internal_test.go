package outbox

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func internalTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// plainPublisher implements only Publish (no BatchPublisher), like the
// executor/k8s multi-step publishers.
type plainPublisher struct {
	calls   int
	failIdx map[int]error // 1-based call number -> error
}

func (p *plainPublisher) Publish(_ context.Context, _ *Entry) error {
	p.calls++
	if err, ok := p.failIdx[p.calls]; ok {
		return err
	}
	return nil
}

// batchPublisher implements both Publish and PublishBatch so we can assert the
// processor prefers the pipelined path.
type batchPublisher struct {
	plainPublisher
	batchCalls   int
	batchEntries int
	returnErrs   func(entries []*Entry) []error
}

func (b *batchPublisher) PublishBatch(_ context.Context, entries []*Entry) []error {
	b.batchCalls++
	b.batchEntries += len(entries)
	if b.returnErrs != nil {
		return b.returnErrs(entries)
	}
	return make([]error, len(entries))
}

func entriesN(n int) []*Entry {
	out := make([]*Entry, n)
	for i := range out {
		out[i] = &Entry{ID: uuid.New(), StreamName: "x:v1"}
	}
	return out
}

func newProc(pub Publisher) *Processor {
	return NewProcessor(nil, "x_outbox", pub, nil, internalTestLogger(), ProcessorConfig{})
}

// TestPublish_UsesBatchPublisherWhenAvailable asserts the pipelined path is
// taken in a single PublishBatch call for the whole batch.
func TestPublish_UsesBatchPublisherWhenAvailable(t *testing.T) {
	pub := &batchPublisher{}
	p := newProc(pub)
	entries := entriesN(5)

	errs := p.publish(context.Background(), entries)

	require.Len(t, errs, 5)
	for _, e := range errs {
		assert.NoError(t, e)
	}
	assert.Equal(t, 1, pub.batchCalls, "exactly one pipelined batch call")
	assert.Equal(t, 5, pub.batchEntries, "all entries dispatched in one batch")
	assert.Equal(t, 0, pub.plainPublisher.calls, "per-entry Publish must not be used")
}

// TestPublish_FallsBackToPerEntryWhenNoBatchPublisher asserts a Publish-only
// publisher (executor/k8s style) keeps its sequential semantics.
func TestPublish_FallsBackToPerEntryWhenNoBatchPublisher(t *testing.T) {
	pub := &plainPublisher{}
	p := newProc(pub)
	entries := entriesN(3)

	errs := p.publish(context.Background(), entries)

	require.Len(t, errs, 3)
	assert.Equal(t, 3, pub.calls, "one Publish per entry")
}

// TestPublish_PerEntryErrorsAreAlignedByIndex asserts the returned error slice
// is positionally aligned with the entries.
func TestPublish_PerEntryErrorsAreAlignedByIndex(t *testing.T) {
	boom := errors.New("boom")
	pub := &plainPublisher{failIdx: map[int]error{2: boom}}
	p := newProc(pub)
	entries := entriesN(3)

	errs := p.publish(context.Background(), entries)

	require.Len(t, errs, 3)
	assert.NoError(t, errs[0])
	assert.ErrorIs(t, errs[1], boom)
	assert.NoError(t, errs[2])
}

// TestPublish_BatchErrorsArePassedThrough asserts per-entry errors from
// PublishBatch survive unchanged.
func TestPublish_BatchErrorsArePassedThrough(t *testing.T) {
	boom := errors.New("xadd failed")
	pub := &batchPublisher{returnErrs: func(entries []*Entry) []error {
		errs := make([]error, len(entries))
		errs[1] = boom
		return errs
	}}
	p := newProc(pub)
	entries := entriesN(4)

	errs := p.publish(context.Background(), entries)

	require.Len(t, errs, 4)
	assert.NoError(t, errs[0])
	assert.ErrorIs(t, errs[1], boom)
}

// TestPublish_FallsBackOnMismatchedBatchSlice guards against a misbehaving
// BatchPublisher that returns a slice of the wrong length: the processor must
// fall back to per-entry Publish rather than risk an index-out-of-range panic.
func TestPublish_FallsBackOnMismatchedBatchSlice(t *testing.T) {
	pub := &batchPublisher{returnErrs: func(entries []*Entry) []error {
		return []error{nil} // wrong length on purpose
	}}
	p := newProc(pub)
	entries := entriesN(3)

	errs := p.publish(context.Background(), entries)

	require.Len(t, errs, 3, "result length must always match entries")
	assert.Equal(t, 3, pub.plainPublisher.calls, "fell back to per-entry publish")
}
