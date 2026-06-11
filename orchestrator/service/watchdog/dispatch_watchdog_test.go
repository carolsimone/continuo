package watchdog_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/orchestrator/service/watchdog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Fakes — the watchdog speaks domain-typed ports, never gRPC wire types.
// ---------------------------------------------------------------------------

type cancelCall struct {
	scheduleName string
	cancelledBy  string
	reason       string
}

type fakeStuckReader struct {
	candidates []ports.StuckSchedule
	err        error
	lastCutoff time.Time
	calls      int
	ctxErr     error // captured deadline state at call time
}

func (f *fakeStuckReader) ListStuckCandidates(ctx context.Context, cutoff time.Time) ([]ports.StuckSchedule, error) {
	f.calls++
	f.lastCutoff = cutoff
	_, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		f.ctxErr = errors.New("ListStuckCandidates called without a deadline")
	}
	return f.candidates, f.err
}

type fakeCanceller struct {
	calls []cancelCall
	err   error
}

func (f *fakeCanceller) CancelSchedule(_ context.Context, scheduleName, cancelledBy, reason string) error {
	f.calls = append(f.calls, cancelCall{scheduleName, cancelledBy, reason})
	return f.err
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func newWatchdogForTest(reader ports.StuckScheduleReader, canceller ports.ScheduleCanceller, now time.Time, noProgressFor time.Duration) *watchdog.Watchdog {
	w := watchdog.NewWatchdog(
		watchdog.Config{Enabled: true, Interval: time.Second, NoProgressFor: noProgressFor},
		reader,
		canceller,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	watchdog.SetClockForTest(w, fixedClock{t: now})
	return w
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestWatchdog_StuckCandidate_CallsCancelSchedule(t *testing.T) {
	now := time.Now()
	reader := &fakeStuckReader{
		candidates: []ports.StuckSchedule{{ScheduleName: "stuck-schedule", RunID: "run-uuid-1"}},
	}
	canceller := &fakeCanceller{}
	w := newWatchdogForTest(reader, canceller, now, 30*time.Minute)

	require.NoError(t, watchdog.TickForTest(w, context.Background()))
	require.Len(t, canceller.calls, 1)
	assert.Equal(t, "stuck-schedule", canceller.calls[0].scheduleName)
	assert.Equal(t, "watchdog", canceller.calls[0].cancelledBy)
	assert.Contains(t, canceller.calls[0].reason, "watchdog:")
}

// The watchdog derives the cutoff as now-NoProgressFor and delegates stuck
// detection to state's single indexed query. Asserting the cutoff is the only
// per-tick decision the watchdog still makes locally.
func TestWatchdog_PassesCutoffEqualToNowMinusNoProgress(t *testing.T) {
	now := time.Now()
	reader := &fakeStuckReader{}
	w := newWatchdogForTest(reader, &fakeCanceller{}, now, 30*time.Minute)

	require.NoError(t, watchdog.TickForTest(w, context.Background()))
	require.Equal(t, 1, reader.calls)
	assert.WithinDuration(t, now.Add(-30*time.Minute), reader.lastCutoff, time.Millisecond)
}

// O(1) RPCs per tick: exactly one ListStuckCandidates call regardless of how
// many candidates come back. This is the fix for the prior N+1 ListTasks
// fan-out (one RPC per running schedule).
func TestWatchdog_IssuesOneReaderCallPerTick(t *testing.T) {
	now := time.Now()
	reader := &fakeStuckReader{
		candidates: []ports.StuckSchedule{
			{ScheduleName: "a", RunID: "r-a"},
			{ScheduleName: "b", RunID: "r-b"},
			{ScheduleName: "c", RunID: "r-c"},
		},
	}
	canceller := &fakeCanceller{}
	w := newWatchdogForTest(reader, canceller, now, 30*time.Minute)

	require.NoError(t, watchdog.TickForTest(w, context.Background()))
	assert.Equal(t, 1, reader.calls, "one ListStuckCandidates RPC per tick, not per schedule")
	assert.Len(t, canceller.calls, 3, "every candidate is cancelled")
}

// Each tick must run under a deadline so a slow state service never stalls the
// loop across ticks.
func TestWatchdog_TickRunsUnderDeadline(t *testing.T) {
	reader := &fakeStuckReader{}
	w := newWatchdogForTest(reader, &fakeCanceller{}, time.Now(), 30*time.Minute)

	require.NoError(t, watchdog.TickForTest(w, context.Background()))
	require.NoError(t, reader.ctxErr, "tick must bound the reader call with a deadline")
}

func TestWatchdog_NoCandidates_NoCancel(t *testing.T) {
	reader := &fakeStuckReader{candidates: nil}
	canceller := &fakeCanceller{}
	w := newWatchdogForTest(reader, canceller, time.Now(), 30*time.Minute)

	require.NoError(t, watchdog.TickForTest(w, context.Background()))
	assert.Empty(t, canceller.calls)
}

// A cancel failure for one candidate must not abort the sweep of the others.
func TestWatchdog_CancelError_ContinuesSweep(t *testing.T) {
	reader := &fakeStuckReader{
		candidates: []ports.StuckSchedule{
			{ScheduleName: "a", RunID: "r-a"},
			{ScheduleName: "b", RunID: "r-b"},
		},
	}
	canceller := &fakeCanceller{err: errors.New("boom")}
	w := newWatchdogForTest(reader, canceller, time.Now(), 30*time.Minute)

	require.NoError(t, watchdog.TickForTest(w, context.Background()))
	assert.Len(t, canceller.calls, 2, "both candidates attempted despite per-call errors")
}

func TestWatchdog_ReaderError_TickReturnsError(t *testing.T) {
	reader := &fakeStuckReader{err: errors.New("state unavailable")}
	canceller := &fakeCanceller{}
	w := newWatchdogForTest(reader, canceller, time.Now(), 30*time.Minute)

	err := watchdog.TickForTest(w, context.Background())
	require.Error(t, err)
	assert.Empty(t, canceller.calls)
}

func TestWatchdog_DisabledConfig_RunReturnsImmediately(t *testing.T) {
	w := watchdog.NewWatchdog(
		watchdog.Config{Enabled: false},
		&fakeStuckReader{},
		&fakeCanceller{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := w.Run(ctx)
	assert.NoError(t, err)
}
