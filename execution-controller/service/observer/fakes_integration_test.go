//go:build integration

package observer

import (
	"context"

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

// fakeLookup answers JobExists from a set.
type fakeLookup struct{ exists map[string]bool }

func (l *fakeLookup) JobExists(_ context.Context, jobName string) (bool, error) {
	return l.exists[jobName], nil
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
