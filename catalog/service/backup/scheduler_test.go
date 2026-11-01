package backup

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSchedule fires every period, aligned to UTC midnight, offset into it.
type fakeSchedule struct{ period, offset time.Duration }

func (f fakeSchedule) Next(after time.Time) time.Time {
	after = after.UTC()
	slot := after.Truncate(f.period).Add(f.offset)
	for !slot.After(after) {
		slot = slot.Add(f.period)
	}
	return slot
}

// fakeScheduleParser reads the two specs the scheduler tests use: "0 2 * * *"
// (daily at 02:00 UTC) and "0 * * * *" (hourly). It refuses any other spec.
type fakeScheduleParser struct{}

func (fakeScheduleParser) Parse(spec string) (ports.BackupSchedule, error) {
	switch strings.TrimSpace(spec) {
	case "0 2 * * *":
		return fakeSchedule{period: 24 * time.Hour, offset: 2 * time.Hour}, nil
	case "0 * * * *":
		return fakeSchedule{period: time.Hour}, nil
	}
	return nil, errors.New("not a schedule the fake reads")
}

// schedulerFixture runs a Scheduler over a fake clock: its sleep moves the
// clock instead of waiting, and each export records the tenant and the time
// it ran at. The test stops the scheduler by cancelling ctx from onExport.
type schedulerFixture struct {
	clock   *fakeClock
	tenants *fakeTenants
	metrics *fakeMetrics
	s       *Scheduler

	mu      sync.Mutex
	exports []string // "<tenant>@<RFC 3339 time>"
	// onExport runs after each export with the number of exports so far.
	onExport func(n int)
	// fail, when set, fails the export of that tenant.
	fail map[string]bool
	// exportTakes is how far each export moves the clock.
	exportTakes time.Duration
	// sleepExtra, when set, is added to the clock after the sleep that ends
	// at that wall time: the process slept through it.
	sleepExtra map[time.Time]time.Duration
}

func newSchedulerFixture(t *testing.T, spec string, start time.Time, tenants ...string) *schedulerFixture {
	t.Helper()
	f := &schedulerFixture{clock: newFakeClock(start), tenants: &fakeTenants{tenants: tenants}, metrics: &fakeMetrics{}, fail: map[string]bool{}}
	graph := fakeGraph{export: func(ctx context.Context, tenantID string, out ports.RecordWriter) error {
		f.mu.Lock()
		f.exports = append(f.exports, tenantID+"@"+f.clock.Now().Format(time.RFC3339))
		n := len(f.exports)
		fail := f.fail[tenantID]
		f.mu.Unlock()
		f.clock.Advance(f.exportTakes)
		if f.onExport != nil {
			defer f.onExport(n)
		}
		if fail {
			return errors.New("neo4j: connection reset")
		}
		return smallGraph(1).export(ctx, tenantID, out)
	}}
	ex := NewExporter(newFakeLease(), graph, newFakeStore(), f.metrics, 7, f.clock.Now, quietLogger())
	s, err := NewScheduler(spec, fakeScheduleParser{}, f.tenants, ex, f.clock.Now, quietLogger())
	require.NoError(t, err)
	s.sleep = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		f.clock.Advance(d)
		f.mu.Lock()
		extra := f.sleepExtra[f.clock.Now()]
		f.mu.Unlock()
		f.clock.Advance(extra)
		return nil
	}
	f.s = s
	return f
}

// run runs the scheduler until it returns, failing the test after a while.
func (f *schedulerFixture) run(t *testing.T, ctx context.Context) {
	t.Helper()
	done := make(chan struct{})
	go func() { f.s.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the scheduler did not stop")
	}
}

func (f *schedulerFixture) ran() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.exports...)
}

// Down across 02:00: a scheduler that starts at 02:30 runs no export for the
// slot it missed; its first export is at the next day's slot, and every next
// slot is computed after the run.
func TestScheduler_NextSlotAndMissedSlotNotCaughtUp(t *testing.T) {
	// 02:30 is long after the slot; 02:00:30 and 02:03 are soon after it, but
	// catalog was not running at 02:00 either way.
	for _, start := range []time.Time{
		time.Date(2026, 10, 30, 2, 30, 0, 0, time.UTC),
		time.Date(2026, 10, 30, 2, 0, 30, 0, time.UTC),
		time.Date(2026, 10, 30, 2, 3, 0, 0, time.UTC),
	} {
		t.Run(start.Format("15:04:05"), func(t *testing.T) {
			f := newSchedulerFixture(t, "0 2 * * *", start, "a")
			ctx, cancel := context.WithCancel(context.Background())
			f.onExport = func(n int) {
				if n == 2 {
					cancel()
				}
			}
			f.run(t, ctx)
			assert.Equal(t, []string{"a@2026-10-31T02:00:00Z", "a@2026-11-01T02:00:00Z"}, f.ran())
		})
	}
}

// The schedule runs in UTC whatever zone the clock reads in.
func TestScheduler_TheScheduleIsUTC(t *testing.T) {
	paris, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	f := newSchedulerFixture(t, "0 2 * * *", time.Date(2026, 10, 30, 1, 0, 0, 0, paris), "a")
	ctx, cancel := context.WithCancel(context.Background())
	f.onExport = func(int) { cancel() }
	f.run(t, ctx)
	require.Len(t, f.ran(), 1)
	assert.Equal(t, "a@2026-10-30T03:00:00+01:00", f.ran()[0], "02:00 UTC")
}

// A slot every tenant is exported in, one after the other; one tenant's
// failure does not stop the others.
func TestScheduler_RunsEveryTenant(t *testing.T) {
	f := newSchedulerFixture(t, "0 2 * * *", time.Date(2026, 10, 30, 1, 0, 0, 0, time.UTC), "a", "b", "c")
	f.fail["b"] = true
	ctx, cancel := context.WithCancel(context.Background())
	f.onExport = func(n int) {
		if n == 3 {
			cancel()
		}
	}
	f.run(t, ctx)
	assert.Equal(t, []string{"a@2026-10-30T02:00:00Z", "b@2026-10-30T02:00:00Z", "c@2026-10-30T02:00:00Z"}, f.ran())
	assert.Equal(t, []string{ports.ExportSuccess, ports.ExportFailure, ports.ExportSuccess}, f.metrics.outcomes())
}

// Exports that run past the next slot skip it: the slot after the run is
// computed from the time the run ended.
func TestScheduler_ARunThatOutlastsTheNextSlotSkipsIt(t *testing.T) {
	// 62 minutes ends the run just after 02:00; 90 minutes long after it.
	for _, takes := range []time.Duration{62 * time.Minute, 90 * time.Minute} {
		t.Run(takes.String(), func(t *testing.T) {
			f := newSchedulerFixture(t, "0 * * * *", time.Date(2026, 10, 30, 0, 30, 0, 0, time.UTC), "a")
			f.exportTakes = takes
			ctx, cancel := context.WithCancel(context.Background())
			f.onExport = func(n int) {
				if n == 2 {
					cancel()
				}
			}
			f.run(t, ctx)
			assert.Equal(t, []string{"a@2026-10-30T01:00:00Z", "a@2026-10-30T03:00:00Z"}, f.ran(),
				"02:00 passed while the 01:00 export ran, and is not run late")
		})
	}
}

// A slot the process slept through (a suspended host) is not run late when
// it wakes: the next slot runs.
func TestScheduler_ASlotSleptThroughIsNotRunLate(t *testing.T) {
	start := time.Date(2026, 10, 30, 1, 0, 0, 0, time.UTC)
	f := newSchedulerFixture(t, "0 2 * * *", start, "a")
	f.sleepExtra = map[time.Time]time.Duration{start.Add(maxSleep): 3 * time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	f.onExport = func(int) { cancel() }
	f.run(t, ctx)
	assert.Equal(t, []string{"a@2026-10-31T02:00:00Z"}, f.ran())
}

// A tenant list that fails exports nothing for the slot; the next slot runs.
func TestScheduler_ATenantListFailureWaitsForTheNextSlot(t *testing.T) {
	f := newSchedulerFixture(t, "0 2 * * *", time.Date(2026, 10, 30, 1, 0, 0, 0, time.UTC), "a")
	f.tenants.err = errors.New("neo4j: connection refused")
	f.s.sleep = wrapSleep(f.s.sleep, func() {
		f.tenants.mu.Lock()
		defer f.tenants.mu.Unlock()
		if f.tenants.calls == 1 {
			f.tenants.err = nil
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	f.onExport = func(int) { cancel() }
	f.run(t, ctx)
	assert.Equal(t, []string{"a@2026-10-31T02:00:00Z"}, f.ran())
}

func wrapSleep(sleep func(context.Context, time.Duration) error, after func()) func(context.Context, time.Duration) error {
	return func(ctx context.Context, d time.Duration) error {
		err := sleep(ctx, d)
		after()
		return err
	}
}

// A cancelled context stops the scheduler while it waits.
func TestScheduler_StopsWhenTheContextEnds(t *testing.T) {
	f := newSchedulerFixture(t, "0 2 * * *", time.Date(2026, 10, 30, 1, 0, 0, 0, time.UTC), "a")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.run(t, ctx)
	assert.Empty(t, f.ran())
}

// The scheduler's own sleep returns at the end of its wait, or as soon as
// the context ends.
func TestScheduler_SleepEndsWithTheContext(t *testing.T) {
	require.NoError(t, sleepCtx(context.Background(), time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	require.ErrorIs(t, sleepCtx(ctx, time.Hour), context.Canceled)
	assert.Less(t, time.Since(start), time.Second)
}

// A spec the parser refuses stops the scheduler from being built, and the
// error names the spec and the parser's reason.
func TestScheduler_RefusesASpecTheParserRefuses(t *testing.T) {
	ex := NewExporter(newFakeLease(), smallGraph(1), newFakeStore(), &fakeMetrics{}, 7, time.Now, quietLogger())

	s, err := NewScheduler("nightly", fakeScheduleParser{}, &fakeTenants{}, ex, time.Now, quietLogger())

	require.Error(t, err)
	assert.Nil(t, s)
	assert.Contains(t, err.Error(), `"nightly"`)
	assert.Contains(t, err.Error(), "not a schedule the fake reads")
}
