package backup

import (
	"bytes"
	"context"
	"errors"
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
	renewErr   func(n int) error // n counts the Renew calls, from 1
	releaseErr error
	renews     int
	// onAcquire, when set, runs as Acquire is called.
	onAcquire func()
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
	return nil
}

func (l *fakeLease) Renew(_ context.Context, tenantID, holder string, ttl time.Duration) error {
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

// fakeStore is a BackupStore in memory. Each error field, when set, is what
// that call returns; deleteErr is per key.
type fakeStore struct {
	mu        sync.Mutex
	objects   map[string][]byte
	writers   []*fakeWriter
	reads     []string
	deleted   []string
	lists     int
	openErr   error
	listErr   error
	readErr   func(n int) error // n counts the Read calls, from 1
	deleteErr map[string]error
	// writer configures each writer Open returns.
	writer func(w *fakeWriter)
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: map[string][]byte{}, deleteErr: map[string]error{}}
}

func (s *fakeStore) Open(_ context.Context, tenantID string, at time.Time) (ports.BackupWriter, string, error) {
	key, err := export.ObjectKey(tenantID, at)
	if err != nil {
		return nil, "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.openErr != nil {
		return nil, "", s.openErr
	}
	w := &fakeWriter{store: s, key: key}
	if s.writer != nil {
		s.writer(w)
	}
	s.writers = append(s.writers, w)
	return w, key, nil
}

func (s *fakeStore) List(_ context.Context, tenantID string) ([]ports.BackupObject, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	if s.listErr != nil {
		return nil, s.listErr
	}
	prefix, err := export.ObjectPrefix(tenantID)
	if err != nil {
		return nil, err
	}
	var out []ports.BackupObject
	for _, key := range slices.Sorted(maps.Keys(s.objects)) {
		if strings.HasPrefix(key, prefix) {
			out = append(out, ports.BackupObject{Key: key, Size: int64(len(s.objects[key]))})
		}
	}
	slices.Reverse(out)
	return out, nil
}

func (s *fakeStore) Read(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, key)
	if s.readErr != nil {
		if err := s.readErr(len(s.reads)); err != nil {
			return nil, err
		}
	}
	body, ok := s.objects[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ports.ErrBackupNotFound, key)
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(body))), nil
}

func (s *fakeStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.deleteErr[key]; err != nil {
		return err
	}
	s.deleted = append(s.deleted, key)
	delete(s.objects, key)
	return nil
}

func (s *fakeStore) put(key string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = body
}

func (s *fakeStore) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.objects))
}

func (s *fakeStore) object(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[key]
	return b, ok
}

func (s *fakeStore) lastWriter() *fakeWriter {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.writers) == 0 {
		return nil
	}
	return s.writers[len(s.writers)-1]
}

// fakeWriter is the BackupWriter of a fakeStore: it stores its bytes at Close.
// writeErr, closeErr and abortErr, when set, are what those calls return;
// onWrite, when set, runs before each Write.
type fakeWriter struct {
	store    *fakeStore
	key      string
	buf      bytes.Buffer
	closed   bool
	aborted  bool
	aborts   int
	writeErr error
	closeErr error
	abortErr error
	onWrite  func()
}

func (w *fakeWriter) Write(p []byte) (int, error) {
	if w.onWrite != nil {
		w.onWrite()
	}
	if w.closed || w.aborted {
		return 0, errors.New("fakeWriter: write after Close or Abort")
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return w.buf.Write(p)
}

func (w *fakeWriter) Close() error {
	if w.closed || w.aborted {
		return errors.New("fakeWriter: Close after Close or Abort")
	}
	if w.closeErr != nil {
		return w.closeErr
	}
	w.closed = true
	w.store.put(w.key, bytes.Clone(w.buf.Bytes()))
	return nil
}

func (w *fakeWriter) Abort() error {
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
