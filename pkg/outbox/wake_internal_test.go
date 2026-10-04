package outbox

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// requireWake fails the test unless w signals within a second.
func requireWake(t *testing.T, w *PostgresWaker, msg string) {
	t.Helper()
	select {
	case <-w.Wake():
	case <-time.After(time.Second):
		t.Fatal(msg)
	}
}

func TestWaker_CoalescesSignalsAndWakesAfterAReconnect(t *testing.T) {
	// Unbuffered: each send returns only once forward has taken the value.
	notify := make(chan *pq.Notification)
	w := newWaker(notify, func() error { return nil }, nil)
	defer func() { _ = w.Close() }()

	w.signal()
	w.signal()
	require.Len(t, w.wake, 1, "a signal that arrives while one is pending merges into it")
	<-w.Wake()

	notify <- &pq.Notification{Channel: "t"}
	requireWake(t, w, "no wake after a notification")

	notify <- nil // pq sends nil after re-establishing a lost connection
	requireWake(t, w, "no wake after a reconnect")
}

// Closing a pq listener can wait minutes on a connection attempt or a ping on
// a dead socket. CloseContext returns once its context is done, and a later
// call returns the result of the same close once it finishes.
func TestWaker_CloseContextReturnsAtItsDeadlineWhileTheCloseBlocks(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	w := newWaker(make(chan *pq.Notification), func() error { return nil }, func() error {
		calls.Add(1)
		<-release
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := w.CloseContext(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second, "CloseContext returns at its deadline")

	close(release)
	require.NoError(t, w.Close(), "a later call waits for the close already running")
	require.Equal(t, int32(1), calls.Load())
}

// Close and CloseContext close the listener once; every call returns that
// close's result.
func TestWaker_CloseIsIdempotent(t *testing.T) {
	boom := errors.New("listener already closed")
	var calls atomic.Int32
	w := newWaker(make(chan *pq.Notification), func() error { return nil }, func() error {
		calls.Add(1)
		return boom
	})

	require.ErrorIs(t, w.Close(), boom)
	require.ErrorIs(t, w.Close(), boom)
	require.ErrorIs(t, w.CloseContext(context.Background()), boom)
	require.Equal(t, int32(1), calls.Load())
}

// A waker without a listener stops forwarding through done, and closing it
// twice is safe.
func TestWaker_WithoutAListenerClosesTwice(t *testing.T) {
	w := newWaker(make(chan *pq.Notification), func() error { return nil }, nil)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close())
	select {
	case <-w.done:
	default:
		t.Fatal("done is not closed")
	}
}
