//go:build integration

package observer

import (
	"context"
	"sync"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
)

func (c *fakeCache) set(jobName string, o model.JobObservation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.obs == nil {
		c.obs = map[string]model.JobObservation{}
	}
	c.obs[jobName] = o
}

// fakeLookup answers a live read with what the cache shows, unless live holds
// what the API shows for a Job. onRead, when set, runs before each live read.
type fakeLookup struct {
	cache  *fakeCache
	live   map[string]model.JobObservation
	onRead func()
}

func (l *fakeLookup) ObserveLive(_ context.Context, jobName string) (model.JobObservation, error) {
	if l.onRead != nil {
		l.onRead()
	}
	if o, ok := l.live[jobName]; ok {
		return o, nil
	}
	return l.cache.Observe(jobName), nil
}

// fakeTerminator records the Jobs it deleted.
type fakeTerminator struct {
	deleted []string
	err     error
}

func (t *fakeTerminator) DeleteJob(_ context.Context, jobName string) error {
	if t.err != nil {
		return t.err
	}
	t.deleted = append(t.deleted, jobName)
	return nil
}

// fakeLogs returns a fixed log and counts reads.
type fakeLogs struct {
	full, tail string
	err        error
	reads      int
}

func (l *fakeLogs) GetPodLogs(_ context.Context, _ string, _ int64) (string, string, error) {
	l.reads++
	if l.err != nil {
		return "", "", l.err
	}
	tail := l.tail
	if tail == "" {
		tail = l.full
	}
	return l.full, tail, nil
}

// fakeUploader keeps every upload.
type fakeUploader struct{ uploaded map[string]string }

func (u *fakeUploader) UploadLog(_ context.Context, key, content string) error {
	if u.uploaded == nil {
		u.uploaded = map[string]string{}
	}
	u.uploaded[key] = content
	return nil
}

// fakeStopMetrics counts the attempts reported stopped and the Jobs reaped,
// by reason.
type fakeStopMetrics struct {
	mu      sync.Mutex
	stopped int
	reaped  []string
}

func (m *fakeStopMetrics) AttemptStopped() { m.mu.Lock(); m.stopped++; m.mu.Unlock() }
func (m *fakeStopMetrics) JobReaped(reason string) {
	m.mu.Lock()
	m.reaped = append(m.reaped, reason)
	m.mu.Unlock()
}
