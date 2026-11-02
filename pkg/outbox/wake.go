package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lib/pq"
)

// Waker signals that new rows may be waiting. Its channel holds at most one
// pending signal; signals that arrive while one is pending merge into it.
type Waker interface {
	Wake() <-chan struct{}
}

// pingInterval is how long the listener may hear nothing before it checks its
// connection; a dead connection is then re-established.
const pingInterval = 90 * time.Second

// listenerMinReconnect and listenerMaxReconnect bound the listener's wait
// between connection attempts: it starts at the minimum and doubles up to the
// maximum. The maximum equals FallbackTick, so once Postgres answers again
// wake-ups resume within about one fallback poll; while Postgres is down each
// relay pod tries to connect once every 5 seconds.
const (
	listenerMinReconnect = time.Second
	listenerMaxReconnect = FallbackTick
)

// listener is the part of a pq listener the waker uses.
type listener interface {
	Listen(channel string) error
	NotificationChannel() <-chan *pq.Notification
	Ping() error
	Close() error
}

// newListener builds a listener connection for dsn that reports its
// connection events to cb. Tests substitute it.
var newListener = func(dsn string, cb pq.EventCallbackType) listener {
	return pq.NewListener(dsn, listenerMinReconnect, listenerMaxReconnect, cb)
}

// PostgresWaker listens on the Postgres channel named by its table argument,
// which a table's trigger notifies: every outbox table has a statement-level
// AFTER INSERT trigger that calls pg_notify on the channel named after the
// table, and execution-controller's deployments table notifies the channel
// named by its adapter. Notifications carry an empty payload, and Postgres
// delivers one when the notifying transaction commits. After a reconnect the
// waker signals once, because notifications sent while it was disconnected are
// lost.
//
// A pq listener reconnects with the connection string it was built with. When
// a connection attempt fails, the waker reads its connection string again; if
// it changed (a rotated password file), the waker closes the listener and
// builds a new one from the fresh string. The relay's fallback poll covers the
// wake-ups missed while the listener is replaced.
type PostgresWaker struct {
	wake   chan struct{}
	table  string
	dsn    func() (string, error)
	logger *slog.Logger

	// mu guards current, currentDSN, generation and closing.
	mu         sync.Mutex
	current    listener
	currentDSN string
	// generation counts the listeners built; the first one is 1.
	generation uint64
	closing    bool

	// replaced receives a signal each time current is replaced, so forward
	// reads the new listener's notifications at once.
	replaced chan struct{}
	// established receives the outcome of the Listen call of a listener the
	// waker has not closed; NewPostgresWaker waits for the first one.
	established chan error

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error
}

var _ Waker = (*PostgresWaker)(nil)

// NewPostgresWaker reads dsn, opens a dedicated listener connection (outside
// any pool) and listens on table's channel. It returns once the LISTEN is
// established. While Postgres is unreachable it keeps waiting: the listener
// retries with backoff (1 s doubling to 5 s) and logs every failed attempt.
// When ctx is done before the LISTEN is established, NewPostgresWaker closes
// the listener and returns ctx.Err(), so a shutdown during boot does not wait
// for Postgres. dsn is read again after every failed connection attempt.
func NewPostgresWaker(ctx context.Context, dsn func() (string, error), table string, logger *slog.Logger) (*PostgresWaker, error) {
	first, err := dsn()
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", table, err)
	}
	w := &PostgresWaker{
		wake:        make(chan struct{}, 1),
		table:       table,
		dsn:         dsn,
		logger:      logger,
		replaced:    make(chan struct{}, 1),
		established: make(chan error, 1),
		closed:      make(chan struct{}),
	}
	w.mu.Lock()
	w.start(first)
	w.mu.Unlock()
	go w.forward()
	select {
	case err := <-w.established:
		if err != nil {
			_ = w.Close()
			return nil, fmt.Errorf("listen on %s: %w", table, err)
		}
	case <-ctx.Done():
		// Closing waits for a connection attempt in progress to end;
		// CloseContext with a done ctx returns at once and leaves that close
		// running. Closing the listener also ends the Listen call.
		_ = w.CloseContext(ctx)
		return nil, ctx.Err()
	}
	return w, nil
}

// start builds a listener for dsn, makes it current and listens on the
// waker's channel. The caller holds w.mu.
func (w *PostgresWaker) start(dsn string) {
	w.generation++
	gen := w.generation
	l := newListener(dsn, func(ev pq.ListenerEventType, err error) { w.event(gen, ev, err) })
	w.current, w.currentDSN = l, dsn
	go func() {
		err := l.Listen(w.table)
		w.mu.Lock()
		stale := w.closing || w.current != l
		w.mu.Unlock()
		if stale {
			// The waker closed this listener; its replacement reports instead.
			return
		}
		if err != nil {
			w.logger.Warn("Outbox listener cannot listen", "table", w.table, "error", err)
		}
		select {
		case w.established <- err:
		default:
		}
	}()
}

// event handles a connection event of the listener of generation gen.
func (w *PostgresWaker) event(gen uint64, ev pq.ListenerEventType, err error) {
	if err != nil {
		w.logger.Warn("Outbox listener connection problem", "table", w.table, "event", ev.String(), "error", err)
	}
	switch ev {
	case pq.ListenerEventReconnected:
		w.logger.Info("Outbox listener reconnected", "table", w.table)
	case pq.ListenerEventConnected:
		if gen > 1 {
			// A replacement listener's first connection: notifications sent
			// while it was being replaced are lost.
			w.logger.Info("Outbox listener reconnected with a new connection string", "table", w.table)
			w.signal()
		}
	case pq.ListenerEventConnectionAttemptFailed:
		w.replaceIfDSNChanged(gen)
	}
}

// replaceIfDSNChanged reads the connection string again and, when it differs
// from the one the current listener (of generation gen) was built with,
// replaces that listener with one built from the fresh string. A failure of an
// already replaced listener, or one reported while the waker closes, changes
// nothing; so does a connection string that cannot be read, and the current
// listener keeps retrying.
func (w *PostgresWaker) replaceIfDSNChanged(gen uint64) {
	fresh, err := w.dsn()
	if err != nil {
		w.logger.Warn("Outbox listener cannot read its connection string", "table", w.table, "error", err)
		return
	}
	w.mu.Lock()
	if w.closing || gen != w.generation || fresh == w.currentDSN {
		w.mu.Unlock()
		return
	}
	old := w.current
	w.start(fresh)
	w.mu.Unlock()
	select {
	case w.replaced <- struct{}{}:
	default:
	}
	// pq calls this from the old listener's goroutine while that listener
	// holds no lock, so the close returns at once and its goroutine ends.
	_ = old.Close()
}

// currentListener returns the listener the waker forwards from.
func (w *PostgresWaker) currentListener() listener {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.current
}

// forward turns notifications, including the nil one pq sends after a
// reconnect, into wake signals, and pings a quiet connection. It follows each
// replacement listener, and returns when the current listener's notification
// channel is closed, which pq does once the waker closes that listener.
func (w *PostgresWaker) forward() {
	l := w.currentListener()
	for {
		select {
		case <-w.replaced:
			l = w.currentListener()
		case _, ok := <-l.NotificationChannel():
			if !ok {
				next := w.currentListener()
				if next == l {
					return
				}
				l = next
				continue
			}
			w.signal()
		case <-time.After(pingInterval):
			// The ping runs on its own goroutine: its reply arrives on the same
			// connection as the notifications, which only flow while forward
			// keeps reading them.
			go func(l listener) { _ = l.Ping() }(l)
		}
	}
}

func (w *PostgresWaker) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Wake returns the channel that receives a signal after rows were committed.
func (w *PostgresWaker) Wake() <-chan struct{} { return w.wake }

// Close closes the listener connection and stops forwarding, waiting for the
// close to finish. CloseContext bounds that wait.
func (w *PostgresWaker) Close() error {
	return w.CloseContext(context.Background())
}

// CloseContext closes the listener connection and stops forwarding. Once it
// starts no listener is replaced. The listener is closed first: pq's goroutine
// then finishes and closes the notification channel, which ends forward.
// forward keeps reading that channel until then, so pq's goroutine never
// blocks on a full channel.
//
// Closing a pq listener waits for a connection attempt or a ping in progress,
// which a dead socket can hold for minutes, so the close runs on its own
// goroutine and CloseContext returns when ctx is done even if the close has
// not finished. The close starts only once; every call waits for that close
// and returns its result.
func (w *PostgresWaker) CloseContext(ctx context.Context) error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closing = true
		l := w.current
		w.mu.Unlock()
		go func() {
			defer close(w.closed)
			w.closeErr = l.Close()
		}()
	})
	select {
	case <-w.closed:
		return w.closeErr
	case <-ctx.Done():
		return fmt.Errorf("close outbox listener: %w", ctx.Err())
	}
}
