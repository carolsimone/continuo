package backup

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/service/ports"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeClock is a clock tests move by hand. It is safe for concurrent use.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(at time.Time) *fakeClock { return &fakeClock{now: at} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// leaseCall is one call that reached a fakeLease.
type leaseCall struct {
	Op       string // Acquire, Renew or Release
	TenantID string
	Kind     ports.LeaseKind
	Holder   string
	TTL      time.Duration
}

// fakeLease is an ExportLease in memory: one lease per tenant. acquireErr,
// renewErr and releaseErr, when set, decide what a call returns instead.
type fakeLease struct {
	mu         sync.Mutex
	held       map[string]string // tenant -> holder
	calls      []leaseCall
	acquireErr error
	// acquireCommitsThenFails, when set, makes Acquire take the lease and
	// then return it, as an Acquire whose answer was lost after its commit.
	acquireCommitsThenFails error
	renewErr                func(n int) error // n counts the Renew calls, from 1
	releaseErr              error
	renews                  int
	// onAcquire, when set, runs as Acquire is called.
	onAcquire func()
	// renewBlocks makes every Renew wait until its context ends, as a
	// renewal against a database that never answers does.
	renewBlocks bool
	// releaseCtxErrs records the error of each Release's context.
	releaseCtxErrs []error
}

func newFakeLease() *fakeLease { return &fakeLease{held: map[string]string{}} }

func (l *fakeLease) Acquire(_ context.Context, tenantID string, kind ports.LeaseKind, holder string, ttl time.Duration) error {
	if l.onAcquire != nil {
		l.onAcquire()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, leaseCall{"Acquire", tenantID, kind, holder, ttl})
	if l.acquireErr != nil {
		return l.acquireErr
	}
	if _, ok := l.held[tenantID]; ok {
		return fmt.Errorf("tenant %q: %w", tenantID, ports.ErrLeaseHeld)
	}
	l.held[tenantID] = holder
	return l.acquireCommitsThenFails
}

func (l *fakeLease) Renew(ctx context.Context, tenantID, holder string, ttl time.Duration) error {
	if l.renewBlocks {
		<-ctx.Done()
		return ctx.Err()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.renews++
	l.calls = append(l.calls, leaseCall{Op: "Renew", TenantID: tenantID, Holder: holder, TTL: ttl})
	if l.renewErr != nil {
		if err := l.renewErr(l.renews); err != nil {
			return err
		}
	}
	if l.held[tenantID] != holder {
		return fmt.Errorf("tenant %q: %w", tenantID, ports.ErrLeaseLost)
	}
	return nil
}

func (l *fakeLease) Release(ctx context.Context, tenantID, holder string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, leaseCall{Op: "Release", TenantID: tenantID, Holder: holder})
	l.releaseCtxErrs = append(l.releaseCtxErrs, ctx.Err())
	if l.releaseErr != nil {
		return l.releaseErr
	}
	if l.held[tenantID] == holder {
		delete(l.held, tenantID)
	}
	return nil
}

// ops returns the operations that reached the lease, in order.
func (l *fakeLease) ops() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, c := range l.calls {
		out = append(out, c.Op)
	}
	return out
}

func (l *fakeLease) callsOf(op string) []leaseCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []leaseCall
	for _, c := range l.calls {
		if c.Op == op {
			out = append(out, c)
		}
	}
	return out
}

func (l *fakeLease) isHeld(tenantID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.held[tenantID]
	return ok
}

// fakeGraph is a GraphExporter whose export is the function the test gives.
type fakeGraph struct {
	export func(ctx context.Context, tenantID string, out ports.RecordWriter) error
}

func (g fakeGraph) Export(ctx context.Context, tenantID string, out ports.RecordWriter) error {
	return g.export(ctx, tenantID, out)
}

// header is the header of an export of tenantID holding nodes :Node nodes and
// rels :DEPENDS_ON relationships.
func header(tenantID string, nodes, rels int64) export.Header {
	h := export.Header{
		FormatVersion: export.FormatVersion, TenantID: tenantID,
		ExportedAt:     time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC),
		CatalogVersion: "test", Nodes: map[string]int64{}, Relationships: map[string]int64{},
	}
	if nodes > 0 {
		h.Nodes["Node"] = nodes
	}
	if rels > 0 {
		h.Relationships["DEPENDS_ON"] = rels
	}
	return h
}

func node(tenantID string, i int) export.NodeRecord {
	return export.NodeRecord{Labels: []string{"Node"}, Props: map[string]any{
		"tenant_id": tenantID, "unique_id": fmt.Sprintf("svc.t%d", i), "active": true,
	}}
}

func dependsOn(from, to int) export.RelRecord {
	return export.RelRecord{
		Type:  "DEPENDS_ON",
		From:  export.NodeRef{Label: "Node", Key: []any{fmt.Sprintf("svc.t%d", from)}},
		To:    export.NodeRef{Label: "Node", Key: []any{fmt.Sprintf("svc.t%d", to)}},
		Props: map[string]any{},
	}
}

// smallGraph exports n nodes and a chain of n-1 relationships between them.
func smallGraph(n int) fakeGraph {
	return fakeGraph{export: func(_ context.Context, tenantID string, out ports.RecordWriter) error {
		if err := out.WriteHeader(header(tenantID, int64(n), int64(max(n-1, 0)))); err != nil {
			return err
		}
		for i := range n {
			if err := out.WriteNode(node(tenantID, i)); err != nil {
				return err
			}
		}
		for i := 1; i < n; i++ {
			if err := out.WriteRel(dependsOn(i, i-1)); err != nil {
				return err
			}
		}
		return nil
	}}
}

// fakeKey is the key a fakeStore gives tenantID's export taken at at: the
// fake lays out its keys its own way, as the service never reads one.
func fakeKey(tenantID string, at time.Time) string {
	return tenantID + "@" + at.UTC().Format(fakeKeyTime)
}

const fakeKeyTime = "20060102T150405Z"

// storedExport is one export a fakeStore holds: its header and its records,
// in the order they were written, and the size Close reported.
type storedExport struct {
	header  export.Header
	records []any
	size    int64
}

// fakeStore is a BackupStore in memory, holding records. Each error field,
// when set, is what that call returns; deleteErr is per key.
type fakeStore struct {
	mu          sync.Mutex
	exports     map[string]storedExport
	sinks       []*fakeSink
	sources     []*fakeSource
	reads       []string // the keys Validate and OpenRead read, in order
	deleted     []string
	lists       int
	openErr     error
	listErr     error
	validateErr error
	readErr     func(n int) error // n counts the reads, from 1
	deleteErr   map[string]error
	// sink configures each sink Open returns.
	sink func(s *fakeSink)
}

func newFakeStore() *fakeStore {
	return &fakeStore{exports: map[string]storedExport{}, deleteErr: map[string]error{}}
}

func (s *fakeStore) Locate(key string) (string, time.Time, error) {
	tenantID, stamp, ok := strings.Cut(key, "@")
	if !ok || tenantID == "" {
		return "", time.Time{}, fmt.Errorf("%w: %q", ports.ErrNotAnExportKey, key)
	}
	at, err := time.Parse(fakeKeyTime, stamp)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("%w: %q", ports.ErrNotAnExportKey, key)
	}
	return tenantID, at, nil
}

func (s *fakeStore) CheckTenant(tenantID string) error {
	if tenantID == "" || strings.Contains(tenantID, "@") {
		return fmt.Errorf("%w: %q", ports.ErrInvalidTenant, tenantID)
	}
	return nil
}

func (s *fakeStore) Open(ctx context.Context, tenantID string, at time.Time) (ports.ExportSink, string, error) {
	if err := s.CheckTenant(tenantID); err != nil {
		return nil, "", err
	}
	key := fakeKey(tenantID, at)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.openErr != nil {
		return nil, "", s.openErr
	}
	w := &fakeSink{store: s, key: key, ctx: ctx}
	if s.sink != nil {
		s.sink(w)
	}
	s.sinks = append(s.sinks, w)
	return w, key, nil
}

// read records a read of key and returns the export stored there.
func (s *fakeStore) read(key string) (storedExport, error) {
	if _, _, err := s.Locate(key); err != nil {
		return storedExport{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, key)
	if s.readErr != nil {
		if err := s.readErr(len(s.reads)); err != nil {
			return storedExport{}, err
		}
	}
	e, ok := s.exports[key]
	if !ok {
		return storedExport{}, fmt.Errorf("%w: %s", ports.ErrBackupNotFound, key)
	}
	return e, nil
}

func (s *fakeStore) OpenRead(_ context.Context, key string) (ports.ExportSource, error) {
	e, err := s.read(key)
	if err != nil {
		return nil, err
	}
	src := &fakeSource{header: e.header, records: slices.Clone(e.records)}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sources = append(s.sources, src)
	return src, nil
}

func (s *fakeStore) Validate(_ context.Context, key, tenantID string) (export.Header, error) {
	e, err := s.read(key)
	if err != nil {
		return export.Header{}, err
	}
	if s.validateErr != nil {
		return export.Header{}, s.validateErr
	}
	if e.header.TenantID != tenantID {
		return export.Header{}, fmt.Errorf("the export is of tenant %q, not %q: %w", e.header.TenantID, tenantID, export.ErrTenantMismatch)
	}
	return e.header, nil
}

func (s *fakeStore) List(_ context.Context, tenantID string) ([]ports.BackupObject, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	if s.listErr != nil {
		return nil, s.listErr
	}
	var out []ports.BackupObject
	for key, e := range s.exports {
		owner, at, err := s.Locate(key)
		if err != nil || owner != tenantID {
			continue
		}
		out = append(out, ports.BackupObject{Key: key, TenantID: owner, TakenAt: at, Size: e.size})
	}
	slices.SortFunc(out, func(a, b ports.BackupObject) int { return b.TakenAt.Compare(a.TakenAt) })
	return out, nil
}

func (s *fakeStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.deleteErr[key]; err != nil {
		return err
	}
	s.deleted = append(s.deleted, key)
	delete(s.exports, key)
	return nil
}

// put stores an export of tenantID taken at at holding n :Node nodes and a
// chain of n-1 relationships, and returns its key.
func (s *fakeStore) put(tenantID string, at time.Time, n int) string {
	key := fakeKey(tenantID, at)
	h := header(tenantID, int64(n), int64(max(n-1, 0)))
	h.ExportedAt = at
	var records []any
	for i := range n {
		records = append(records, node(tenantID, i))
	}
	for i := 1; i < n; i++ {
		records = append(records, dependsOn(i, i-1))
	}
	s.putExport(key, storedExport{header: h, records: records, size: int64(len(records))})
	return key
}

func (s *fakeStore) putExport(key string, e storedExport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exports[key] = e
}

func (s *fakeStore) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.exports))
}

func (s *fakeStore) stored(key string) (storedExport, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.exports[key]
	return e, ok
}

func (s *fakeStore) lastSink() *fakeSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sinks) == 0 {
		return nil
	}
	return s.sinks[len(s.sinks)-1]
}

// fakeSinkRecordBytes is the size a fakeSink gives each record it stores.
const fakeSinkRecordBytes = 100

// fakeSink is the ExportSink of a fakeStore: it stores its records at Close,
// with a size of fakeSinkRecordBytes per record. writeErr, closeErr and
// abortErr, when set, are what those calls return; onWrite, when set, runs
// before each record; blockWrites makes every record wait until the context
// the sink was opened with ends, as a write to a store that never answers
// does.
type fakeSink struct {
	store       *fakeStore
	key         string
	ctx         context.Context
	blockWrites bool
	header      export.Header
	records     []any
	closed      bool
	aborted     bool
	aborts      int
	writeErr    error
	closeErr    error
	abortErr    error
	onWrite     func()
}

func (w *fakeSink) write(rec any) error {
	if w.onWrite != nil {
		w.onWrite()
	}
	if w.blockWrites {
		<-w.ctx.Done()
		return w.ctx.Err()
	}
	if w.closed || w.aborted {
		return fmt.Errorf("%w: %s", ports.ErrExportClosed, w.key)
	}
	if w.writeErr != nil {
		return w.writeErr
	}
	if h, ok := rec.(export.Header); ok {
		w.header = h
		return nil
	}
	w.records = append(w.records, rec)
	return nil
}

func (w *fakeSink) WriteHeader(h export.Header) error   { return w.write(h) }
func (w *fakeSink) WriteNode(n export.NodeRecord) error { return w.write(n) }
func (w *fakeSink) WriteRel(r export.RelRecord) error   { return w.write(r) }

func (w *fakeSink) Close() (int64, error) {
	if w.closed || w.aborted {
		return 0, fmt.Errorf("%w: %s", ports.ErrExportClosed, w.key)
	}
	if w.closeErr != nil {
		return 0, w.closeErr
	}
	w.closed = true
	size := int64(fakeSinkRecordBytes * (len(w.records) + 1))
	w.store.putExport(w.key, storedExport{header: w.header, records: slices.Clone(w.records), size: size})
	return size, nil
}

func (w *fakeSink) Abort() error {
	w.aborts++
	if w.closed {
		return nil
	}
	if w.abortErr != nil {
		return w.abortErr
	}
	w.aborted = true
	return nil
}

// fakeSource is the ExportSource of a fakeStore: it hands back the records of
// a stored export.
type fakeSource struct {
	header  export.Header
	records []any
	closed  bool
}

func (s *fakeSource) Header() export.Header { return s.header }

func (s *fakeSource) Next() (any, error) {
	if len(s.records) == 0 {
		return nil, io.EOF
	}
	rec := s.records[0]
	s.records = s.records[1:]
	return rec, nil
}

func (s *fakeSource) Close() error {
	s.closed = true
	return nil
}

// observation is one ExportObserved call.
type observation struct {
	Outcome     string
	Duration    time.Duration
	Bytes       int64
	Nodes, Rels int64
}

// fakeMetrics records every observation.
type fakeMetrics struct {
	mu  sync.Mutex
	obs []observation
}

func (m *fakeMetrics) ExportObserved(outcome string, d time.Duration, bytes int64, nodes, rels int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.obs = append(m.obs, observation{outcome, d, bytes, nodes, rels})
}

func (m *fakeMetrics) outcomes() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, o := range m.obs {
		out = append(out, o.Outcome)
	}
	return out
}

func (m *fakeMetrics) last() observation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.obs[len(m.obs)-1]
}

// fakeTenants lists the tenants the test gives, or fails with err.
type fakeTenants struct {
	mu      sync.Mutex
	tenants []string
	err     error
	calls   int
}

func (f *fakeTenants) Tenants(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return slices.Clone(f.tenants), f.err
}
