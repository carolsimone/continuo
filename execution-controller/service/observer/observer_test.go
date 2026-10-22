package observer

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func unitObserver(q *fakeQueue, observed *fakeObserved) *Observer {
	return New(Config{StartTimeout: time.Minute, Workers: 1}, Deps{
		Observed: observed, Cache: &fakeCache{}, Queue: q, Metrics: &fakeMetrics{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func TestProcessNext_RetriesANameThatFailedAndForgetsOneThatDidNot(t *testing.T) {
	q := &fakeQueue{ready: []string{"job-a"}}
	o := unitObserver(q, &fakeObserved{err: errBoom})
	require.True(t, o.processNext(context.Background()))
	assert.Equal(t, []string{"job-a"}, q.retried, "a failure is retried with backoff")
	assert.Equal(t, []string{"job-a"}, q.done)

	q = &fakeQueue{ready: []string{"job-b"}}
	o = unitObserver(q, &fakeObserved{})
	require.True(t, o.processNext(context.Background()))
	assert.Equal(t, []string{"job-b"}, q.forgot, "a name with no deployment is processed and forgotten")
	assert.False(t, o.processNext(context.Background()), "a shut-down queue stops the worker")
}

func TestRun_QueuesEveryStartedDeploymentAndStopsWithItsContext(t *testing.T) {
	q := &fakeQueue{}
	o := unitObserver(q, &fakeObserved{names: []string{"job-a", "job-b"}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- o.Run(ctx) }()
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.added) == 2
	}, 5*time.Second, 10*time.Millisecond, "Run resyncs as it starts")
	cancel()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop with its context")
	}
}

func TestProcess_WaitsForTheCacheToSync(t *testing.T) {
	q := &fakeQueue{}
	o := unitObserver(q, &fakeObserved{err: errBoom})
	o.deps.Cache = &fakeCache{notSynced: true}
	require.NoError(t, o.Process(context.Background(), "job-a"), "nothing is read before the cache syncs")
	assert.Equal(t, time.Second, q.after["job-a"], "the name is queued again a second later")
}
