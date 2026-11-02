package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// stubListener stands in for a pq listener. Listen returns at once, Close
// runs closeFn (when set) and then closes the notification channel, as pq does
// once its goroutine ends.
type stubListener struct {
	dsn       string
	cb        pq.EventCallbackType
	notify    chan *pq.Notification
	closeFn   func() error
	closeOnce sync.Once
	closed    atomic.Bool
}

func (s *stubListener) Listen(string) error                          { return nil }
func (s *stubListener) NotificationChannel() <-chan *pq.Notification { return s.notify }
func (s *stubListener) Ping() error                                  { return nil }

func (s *stubListener) Close() error {
	var err error
	if s.closeFn != nil {
		err = s.closeFn()
	}
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.notify)
	})
	return err
}

// stubListeners substitutes newListener for the test. Each listener it builds
// gets an unbuffered notification channel and the close function closeFn
// returns for it; every listener built is recorded in order.
type stubListeners struct {
	mu    sync.Mutex
	built []*stubListener
}

func (s *stubListeners) all() []*stubListener {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*stubListener(nil), s.built...)
}

func useStubListeners(t *testing.T, closeFn func() func() error) *stubListeners {
	t.Helper()
	stubs := &stubListeners{}
	orig := newListener
	newListener = func(dsn string, cb pq.EventCallbackType) listener {
		l := &stubListener{dsn: dsn, cb: cb, notify: make(chan *pq.Notification)}
		if closeFn != nil {
			l.closeFn = closeFn()
		}
		stubs.mu.Lock()
		stubs.built = append(stubs.built, l)
		stubs.mu.Unlock()
		return l
	}
	t.Cleanup(func() { newListener = orig })
	return stubs
}

func staticDSN(dsn string) func() (string, error) {
	return func() (string, error) { return dsn, nil }
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// requireWake fails the test unless w signals within a second.
func requireWake(t *testing.T, w *PostgresWaker, msg string) {
	t.Helper()
	select {
	case <-w.Wake():
	case <-time.After(time.Second):
		t.Fatal(msg)
	}
}

// requireNoWake fails the test if w signals within 100 ms.
func requireNoWake(t *testing.T, w *PostgresWaker, msg string) {
	t.Helper()
	select {
	case <-w.Wake():
		t.Fatal(msg)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWaker_CoalescesSignalsAndWakesAfterAReconnect(t *testing.T) {
	stubs := useStubListeners(t, nil)
	w, err := NewPostgresWaker(context.Background(), staticDSN("a"), "t", discardLogger())
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	notify := stubs.all()[0].notify

	w.signal()
	w.signal()
	require.Len(t, w.wake, 1, "a signal that arrives while one is pending merges into it")
	<-w.Wake()

	notify <- &pq.Notification{Channel: "t"}
	requireWake(t, w, "no wake after a notification")

	notify <- nil // pq sends nil after re-establishing a lost connection
	requireWake(t, w, "no wake after a reconnect")
}

// A failed connection attempt makes the waker read the DSN again. When it
// changed (a rotated password file), the waker closes the listener, builds a
// new one from the fresh DSN, forwards that listener's notifications, and
// signals once the new listener connects, because notifications sent in the
// gap are lost.
func TestWakerRebuildsTheListenerWithAFreshDSN(t *testing.T) {
	stubs := useStubListeners(t, nil)
	dsns := []string{"a", "b"}
	var calls atomic.Int32
	dsn := func() (string, error) {
		i := int(calls.Add(1)) - 1
		return dsns[min(i, len(dsns)-1)], nil
	}
	w, err := NewPostgresWaker(context.Background(), dsn, "t", discardLogger())
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	first := stubs.all()[0]
	require.Equal(t, "a", first.dsn)
	first.cb(pq.ListenerEventConnectionAttemptFailed, errors.New("password authentication failed"))

	built := stubs.all()
	require.Len(t, built, 2, "a new listener is built")
	require.Equal(t, "b", built[1].dsn, "the new listener uses the fresh DSN")
	require.True(t, first.closed.Load(), "the old listener is closed")

	requireNoWake(t, w, "no wake before the new listener connects")
	built[1].cb(pq.ListenerEventConnected, nil)
	requireWake(t, w, "no wake once the new listener connected")

	built[1].notify <- &pq.Notification{Channel: "t"}
	requireWake(t, w, "no wake for a notification on the new listener")

	// The DSN is unchanged now, so a further failure leaves the reconnect to
	// the listener itself, and a failure the old listener reports is ignored.
	built[1].cb(pq.ListenerEventConnectionAttemptFailed, errors.New("connection refused"))
	first.cb(pq.ListenerEventConnectionAttemptFailed, errors.New("connection refused"))
	require.Len(t, stubs.all(), 2, "no listener is built while the DSN is unchanged")
}

// A DSN that cannot be read (a password file gone missing) keeps the current
// listener, which goes on retrying.
func TestWakerKeepsTheListenerWhenTheDSNCannotBeRead(t *testing.T) {
	stubs := useStubListeners(t, nil)
	var calls atomic.Int32
	dsn := func() (string, error) {
		if calls.Add(1) == 1 {
			return "a", nil
		}
		return "", errors.New("POSTGRES_PASSWORD_FILE: no such file")
	}
	w, err := NewPostgresWaker(context.Background(), dsn, "t", discardLogger())
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	stubs.all()[0].cb(pq.ListenerEventConnectionAttemptFailed, errors.New("refused"))
	require.Len(t, stubs.all(), 1)
	require.False(t, stubs.all()[0].closed.Load())
}

// A DSN that cannot be read at boot fails NewPostgresWaker.
func TestNewPostgresWaker_FailsWhenTheDSNCannotBeRead(t *testing.T) {
	stubs := useStubListeners(t, nil)
	_, err := NewPostgresWaker(context.Background(), func() (string, error) {
		return "", errors.New("POSTGRES_PASSWORD_FILE: no such file")
	}, "t", discardLogger())
	require.ErrorContains(t, err, "POSTGRES_PASSWORD_FILE")
	require.Empty(t, stubs.all())
}

// Closing a pq listener can wait minutes on a connection attempt or a ping on
// a dead socket. CloseContext returns once its context is done, and a later
// call returns the result of the same close once it finishes.
func TestWaker_CloseContextReturnsAtItsDeadlineWhileTheCloseBlocks(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	useStubListeners(t, func() func() error {
		return func() error {
			calls.Add(1)
			<-release
			return nil
		}
	})
	w, err := NewPostgresWaker(context.Background(), staticDSN("a"), "t", discardLogger())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = w.CloseContext(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second, "CloseContext returns at its deadline")

	close(release)
	require.NoError(t, w.Close(), "a later call waits for the close already running")
	require.Equal(t, int32(1), calls.Load())
}

// Close and CloseContext close the listener once; every call returns that
// close's result, and no listener is built after the waker is closed.
func TestWaker_CloseIsIdempotent(t *testing.T) {
	boom := errors.New("listener already closed")
	var calls atomic.Int32
	stubs := useStubListeners(t, func() func() error {
		return func() error {
			calls.Add(1)
			return boom
		}
	})
	dsns := []string{"a", "b"}
	var dsnCalls atomic.Int32
	w, err := NewPostgresWaker(context.Background(), func() (string, error) {
		i := int(dsnCalls.Add(1)) - 1
		return dsns[min(i, len(dsns)-1)], nil
	}, "t", discardLogger())
	require.NoError(t, err)

	require.ErrorIs(t, w.Close(), boom)
	require.ErrorIs(t, w.Close(), boom)
	require.ErrorIs(t, w.CloseContext(context.Background()), boom)
	require.Equal(t, int32(1), calls.Load())

	stubs.all()[0].cb(pq.ListenerEventConnectionAttemptFailed, errors.New("refused"))
	require.Len(t, stubs.all(), 1, "a closed waker builds no listener")
}
