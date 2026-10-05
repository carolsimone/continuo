package trimmer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/trim"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var now = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

const retention = 72 * time.Hour

func cutoffID() trim.StreamID { return trim.IDAt(now.Add(-retention)) }

// fakeInspector serves per-stream snapshots and needed entries from memory and
// records the trims and deletes it is asked for.
type fakeInspector struct {
	streams map[string]*fakeStream
	trimmed map[string][]trim.StreamID
	deleted []string
}

type fakeStream struct {
	snap    trim.Snapshot
	unknown []string
	needed  map[string][]trim.Entry
}

func newFakeInspector() *fakeInspector {
	return &fakeInspector{streams: map[string]*fakeStream{}, trimmed: map[string][]trim.StreamID{}}
}

func (f *fakeInspector) add(stream string, snap trim.Snapshot, needed map[string][]trim.Entry) {
	snap.Stream = stream
	f.streams[stream] = &fakeStream{snap: snap, needed: needed}
}

var _ ports.StreamInspector = (*fakeInspector)(nil)

func (f *fakeInspector) Snapshot(_ context.Context, stream string, _ []string) (trim.Snapshot, []string, bool, error) {
	s, ok := f.streams[stream]
	if !ok {
		return trim.Snapshot{}, nil, false, nil
	}
	return s.snap, s.unknown, true, nil
}

func (f *fakeInspector) NeededEntries(_ context.Context, stream, group string, _, _ trim.StreamID, limit int) ([]trim.Entry, error) {
	entries := append([]trim.Entry(nil), f.streams[stream].needed[group]...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID.Less(entries[j].ID) })
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

func (f *fakeInspector) TrimBefore(_ context.Context, stream string, minID trim.StreamID) (int64, error) {
	f.trimmed[stream] = append(f.trimmed[stream], minID)
	return 1, nil
}

func (f *fakeInspector) DeleteIfExists(_ context.Context, stream string) (bool, error) {
	f.deleted = append(f.deleted, stream)
	return true, nil
}

// fakeRepo is the in-memory DeadLetterRepository the trimmer stores into; only
// InsertBatch is implemented, the rest of the port is never reached.
type fakeRepo struct {
	repository.DeadLetterRepository
	rows      map[string]deadletter.DeadLetter
	insertErr error
}

func newFakeRepo() *fakeRepo { return &fakeRepo{rows: map[string]deadletter.DeadLetter{}} }

func (r *fakeRepo) InsertBatch(_ context.Context, dls []deadletter.DeadLetter) (int, error) {
	if r.insertErr != nil {
		return 0, r.insertErr
	}
	n := 0
	for _, dl := range dls {
		if _, ok := r.rows[dl.DedupKey]; ok {
			continue
		}
		r.rows[dl.DedupKey] = dl
		n++
	}
	return n, nil
}

func (r *fakeRepo) count() int                             { return len(r.rows) }
func (r *fakeRepo) byKey(key string) deadletter.DeadLetter { return r.rows[key] }

type fakeLock struct{ ok bool }

func (l *fakeLock) TryAcquire(context.Context) (func(), bool, error) { return func() {}, l.ok, nil }

type recordingObserver struct {
	quarantined int
	trimmed     int64
	succeeded   int
}

func (o *recordingObserver) Quarantined(_, _ string, n int) { o.quarantined += n }
func (o *recordingObserver) Trimmed(_ string, n int64)      { o.trimmed += n }
func (o *recordingObserver) TrimSucceeded(time.Time)        { o.succeeded++ }

type fixedClock struct{}

func (fixedClock) Now() time.Time { return now }

type harness struct {
	trimmer *Trimmer
	repo    *fakeRepo
	lock    *fakeLock
	obs     *recordingObserver
}

func newHarnessWithBudget(ins *fakeInspector, groups map[string][]string, budget int, retired ...string) *harness {
	h := &harness{repo: newFakeRepo(), lock: &fakeLock{ok: true}, obs: &recordingObserver{}}
	h.trimmer = New(Config{Retention: retention, Budget: budget, Groups: groups, Retired: retired},
		ins, h.repo, h.lock, fixedClock{}, h.obs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return h
}

func newHarness(ins *fakeInspector, groups map[string][]string, retired []string) *harness {
	return newHarnessWithBudget(ins, groups, DefaultBudget, retired...)
}

func TestTrimmer_QuarantinesThenTrimsToCutoff(t *testing.T) {
	old1 := trim.Entry{ID: trim.StreamID{Ms: 10}, Fields: map[string]string{"k": "1"}}
	old2 := trim.Entry{ID: trim.StreamID{Ms: 20}, Fields: map[string]string{"k": "2"}}
	ins := newFakeInspector()
	ins.add("s:v1", trim.Snapshot{Present: []trim.Group{{Name: "lag", LastDelivered: trim.StreamID{Ms: 5}}}}, map[string][]trim.Entry{"lag": {old1, old2}})
	h := newHarness(ins, map[string][]string{"s:v1": {"lag"}}, nil)

	require.NoError(t, h.trimmer.RunOnce(context.Background()))

	assert.Equal(t, 2, h.repo.count())
	dl := h.repo.byKey(deadletter.QuarantineKey("s:v1", "lag", old1.ID.String()))
	assert.Equal(t, deadletter.SourceQuarantine, dl.Source)
	assert.Equal(t, model.DeadLetterKindTrimmed, dl.FailureKind)
	assert.True(t, dl.Redrivable)
	assert.Equal(t, old1.ID.Time(), dl.OriginalAt)
	assert.Equal(t, []trim.StreamID{cutoffID()}, ins.trimmed["s:v1"])
	assert.Equal(t, 2, h.obs.quarantined)
	assert.Equal(t, 1, h.obs.succeeded)
}

func TestTrimmer_BudgetExhaustedNeverTrimsPastUnstored(t *testing.T) {
	var entries []trim.Entry
	for i := uint64(1); i <= 5; i++ {
		entries = append(entries, trim.Entry{ID: trim.StreamID{Ms: i}, Fields: map[string]string{"k": "v"}})
	}
	ins := newFakeInspector()
	ins.add("s:v1", trim.Snapshot{Present: []trim.Group{{Name: "lag"}}}, map[string][]trim.Entry{"lag": entries})
	h := newHarnessWithBudget(ins, map[string][]string{"s:v1": {"lag"}}, 3)

	require.NoError(t, h.trimmer.RunOnce(context.Background()))

	assert.Equal(t, 3, h.repo.count(), "only the budget is stored")
	assert.Equal(t, []trim.StreamID{{Ms: 4}}, ins.trimmed["s:v1"], "trim stops at the first entry not stored")
}

func TestTrimmer_StoreFailureTrimsNothing(t *testing.T) {
	ins := newFakeInspector()
	ins.add("s:v1", trim.Snapshot{Present: []trim.Group{{Name: "lag"}}},
		map[string][]trim.Entry{"lag": {{ID: trim.StreamID{Ms: 1}, Fields: map[string]string{"k": "v"}}}})
	h := newHarness(ins, map[string][]string{"s:v1": {"lag"}}, nil)
	h.repo.insertErr = errors.New("connection refused")

	err := h.trimmer.RunOnce(context.Background())
	assert.Error(t, err)
	assert.Empty(t, ins.trimmed["s:v1"], "the cap never deletes before the payload is stored")
	assert.Zero(t, h.obs.succeeded)
}

func TestTrimmer_LockHeldByPeerDoesNothing(t *testing.T) {
	ins := newFakeInspector()
	ins.add("s:v1", trim.Snapshot{}, nil)
	h := newHarness(ins, map[string][]string{"s:v1": nil}, nil)
	h.lock.ok = false
	require.NoError(t, h.trimmer.RunOnce(context.Background()))
	assert.Empty(t, ins.trimmed)
}

func TestTrimmer_DeletesRetiredStreams(t *testing.T) {
	ins := newFakeInspector()
	h := newHarness(ins, map[string][]string{}, []string{"old:v1"})
	require.NoError(t, h.trimmer.RunOnce(context.Background()))
	assert.Equal(t, []string{"old:v1"}, ins.deleted)
}

func TestTrimmer_MissingStreamIsSkipped(t *testing.T) {
	ins := newFakeInspector() // no streams exist
	h := newHarness(ins, map[string][]string{"s:v1": {"g"}}, nil)
	require.NoError(t, h.trimmer.RunOnce(context.Background()))
	assert.Empty(t, ins.trimmed)
	assert.Equal(t, 1, h.obs.succeeded)
}

// A contract group missing from Redis means the stream is trimmed by age only:
// nothing is quarantined and the trim stops at the cutoff.
func TestTrimmer_MissingGroupTrimsByAgeOnly(t *testing.T) {
	ins := newFakeInspector()
	ins.add("s:v1", trim.Snapshot{
		Present: []trim.Group{{Name: "a", LastDelivered: trim.StreamID{Ms: 5}}},
		Missing: []string{"never"},
	}, map[string][]trim.Entry{"a": {{ID: trim.StreamID{Ms: 10}, Fields: map[string]string{"k": "v"}}}})
	h := newHarness(ins, map[string][]string{"s:v1": {"a", "never"}}, nil)
	require.NoError(t, h.trimmer.RunOnce(context.Background()))
	assert.Equal(t, []trim.StreamID{cutoffID()}, ins.trimmed["s:v1"])
}

// A group the contract does not list never reaches the plan, so it cannot hold
// trimming back: with no present contract group the stream is trimmed to the
// cutoff and nothing is quarantined.
func TestTrimmer_UnknownGroupDoesNotHoldTrimmingBack(t *testing.T) {
	ins := newFakeInspector()
	ins.add("s:v1", trim.Snapshot{}, nil)
	ins.streams["s:v1"].unknown = []string{"orphan"}
	h := newHarness(ins, map[string][]string{"s:v1": {"a"}}, nil)
	require.NoError(t, h.trimmer.RunOnce(context.Background()))
	assert.Zero(t, h.repo.count())
	assert.Equal(t, []trim.StreamID{cutoffID()}, ins.trimmed["s:v1"])
}
