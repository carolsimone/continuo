package outbox

import (
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

// PostgresWaker listens on the Postgres channel named after an outbox table.
// Each outbox table has a statement-level AFTER INSERT trigger that calls
// pg_notify on the channel named after the table, with an empty payload, and
// Postgres delivers the notification when the inserting transaction commits.
// After a reconnect the waker signals once, because notifications sent while
// it was disconnected are lost.
type PostgresWaker struct {
	listener  *pq.Listener
	wake      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

var _ Waker = (*PostgresWaker)(nil)

// NewPostgresWaker opens a dedicated listener connection (outside any pool)
// and listens on table's channel. It returns once the LISTEN is established.
// While Postgres is unreachable it keeps waiting: the listener retries with
// backoff (1 s doubling to 30 s) and logs every failed attempt.
func NewPostgresWaker(dsn, table string, logger *slog.Logger) (*PostgresWaker, error) {
	l := pq.NewListener(dsn, time.Second, 30*time.Second, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			logger.Warn("Outbox listener connection problem", "table", table, "event", ev.String(), "error", err)
			return
		}
		if ev == pq.ListenerEventReconnected {
			logger.Info("Outbox listener reconnected", "table", table)
		}
	})
	if err := l.Listen(table); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("listen on %s: %w", table, err)
	}
	w := newWaker(l.Notify, l.Ping)
	w.listener = l
	return w, nil
}

func newWaker(notify <-chan *pq.Notification, ping func() error) *PostgresWaker {
	w := &PostgresWaker{wake: make(chan struct{}, 1), done: make(chan struct{})}
	go w.forward(notify, ping)
	return w
}

// forward turns notifications, including the nil one pq sends after a
// reconnect, into wake signals, and pings a quiet connection. It returns when
// notify is closed, which pq does once the listener is closed, or when done is
// closed on a waker without a listener.
func (w *PostgresWaker) forward(notify <-chan *pq.Notification, ping func() error) {
	for {
		select {
		case <-w.done:
			return
		case _, ok := <-notify:
			if !ok {
				return
			}
			w.signal()
		case <-time.After(pingInterval):
			// The ping runs on its own goroutine: its reply arrives on the same
			// connection as the notifications, which only flow while forward
			// keeps reading notify.
			go func() { _ = ping() }()
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

// Close closes the listener connection and stops forwarding. The listener is
// closed first: pq's goroutine then finishes and closes notify, which ends
// forward. forward keeps reading notify until then, so pq's goroutine never
// blocks on a full notify channel. A waker without a listener stops forward
// through done. Calls after the first return the first call's result.
func (w *PostgresWaker) Close() error {
	w.closeOnce.Do(func() {
		if w.listener != nil {
			w.closeErr = w.listener.Close()
			return
		}
		close(w.done)
	})
	return w.closeErr
}
