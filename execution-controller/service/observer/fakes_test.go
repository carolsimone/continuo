package observer

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
)

// fakeCache serves a fixed observation per Job name; notSynced makes it
// report a cache that has not listed everything yet.
type fakeCache struct {
	mu        sync.Mutex
	obs       map[string]model.JobObservation
	notSynced bool
}

func (c *fakeCache) Synced() bool { return !c.notSynced }
func (c *fakeCache) Observe(jobName string) model.JobObservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.obs[jobName]
}

// fakeQueue records what is queued; Get hands out ready, then reports the
// queue shut down.
type fakeQueue struct {
	mu      sync.Mutex
	added   []string
	after   map[string]time.Duration
	ready   []string
	retried []string
	forgot  []string
	done    []string
}

func (q *fakeQueue) Add(jobName string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.added = append(q.added, jobName)
}
func (q *fakeQueue) AddAfter(jobName string, d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.after == nil {
		q.after = map[string]time.Duration{}
	}
	q.after[jobName] = d
}
func (q *fakeQueue) Get() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.ready) == 0 {
		return "", true
	}
	name := q.ready[0]
	q.ready = q.ready[1:]
	return name, false
}
func (q *fakeQueue) Done(name string) { q.mu.Lock(); q.done = append(q.done, name); q.mu.Unlock() }
func (q *fakeQueue) Retry(name string) {
	q.mu.Lock()
	q.retried = append(q.retried, name)
	q.mu.Unlock()
}
func (q *fakeQueue) Forget(name string) {
	q.mu.Lock()
	q.forgot = append(q.forgot, name)
	q.mu.Unlock()
}
func (q *fakeQueue) ShutDown() {}

// fakeMetrics keeps the attempts marked as awaiting a pod stop.
type fakeMetrics struct{ waiting map[string]bool }

func (m *fakeMetrics) AwaitingPodStop(jobName string, waiting bool) {
	if m.waiting == nil {
		m.waiting = map[string]bool{}
	}
	m.waiting[jobName] = waiting
}

var errBoom = errors.New("boom")

// fakeObserved serves one deployment by Job name, or an error.
type fakeObserved struct {
	deps  map[string]*model.Deployment
	err   error
	names []string
}

func (f *fakeObserved) FindByJobName(_ context.Context, jobName string) (*model.Deployment, error) {
	if f.err != nil {
		return nil, f.err
	}
	if d, ok := f.deps[jobName]; ok {
		return d, nil
	}
	return nil, sql.ErrNoRows
}
func (f *fakeObserved) ListObservedJobNames(context.Context) ([]string, error)     { return f.names, f.err }
func (f *fakeObserved) StateAge(context.Context, uuid.UUID) (time.Duration, error) { return 0, f.err }

// fakeLegacy counts Clear calls and reports n keys deleted.
type fakeLegacy struct {
	clears int
	n      int64
	err    error
}

func (l *fakeLegacy) Clear(context.Context) (int64, error) {
	l.clears++
	return l.n, l.err
}
