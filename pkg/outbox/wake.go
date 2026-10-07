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

// PostgresWaker listens on the Postgres channel named by its table argument,
// which a table's trigger notifies: every outbox table has a statement-level
// AFTER INSERT trigger that calls pg_notify on the channel named after the
// table, and execution-controller's deployments table notifies the channel
// named by its adapter. Notifications carry an empty payload, and Postgres
// delivers one when the notifying transaction commits. After a reconnect the
// waker signals once, because notifications sent while it was disconnected are
// lost.
type PostgresWaker struct {
	wake chan struct{}
	done chan struct{}
	// closeListener closes the listener connection; on a waker without a
	// listener it is nil and closing stops forward through done.
	closeListener func() error
	closeOnce     sync.Once
	closed        chan struct{}
	closeErr      error
}

var _ Waker = (*PostgresWaker)(nil)

// NewPostgresWaker opens a dedicated listener connection (outside any pool)
// and listens on table's channel. It returns once the LISTEN is established.
// While Postgres is unreachable it keeps waiting: the listener retries with
// backoff (1 s doubling to 5 s) and logs every failed attempt. When ctx is
// done before the LISTEN is established, NewPostgresWaker closes the listener
// and returns ctx.Err(), so a shutdown during boot does not wait for Postgres.
func NewPostgresWaker(ctx context.Context, dsn, table string, logger *slog.Logger) (*PostgresWaker, error) {
	l := pq.NewListener(dsn, listenerMinReconnect, listenerMaxReconnect, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			logger.Warn("Outbox listener connection problem", "table", table, "event", ev.String(), "error", err)
			return
		}
		if ev == pq.ListenerEventReconnected {
			logger.Info("Outbox listener reconnected", "table", table)
		}
	})
	listened := make(chan error, 1)
	go func() { listened <- l.Listen(table) }()
	select {
	case err := <-listened:
		if err != nil {
			_ = l.Close()
			return nil, fmt.Errorf("listen on %s: %w", table, err)
		}
	case <-ctx.Done():
		// Close waits for a connection attempt in progress to end, so it runs
		// on its own goroutine; closing the listener also ends the Listen call.
		go func() { _ = l.Close() }()
		return nil, ctx.Err()
	}
	return newWaker(l.Notify, l.Ping, l.Close), nil
}

// newWaker starts forwarding notify into wake signals. closeListener closes
// the listener that feeds notify; when it is nil, closing the waker stops
// forward through done instead.
func newWaker(notify <-chan *pq.Notification, ping func() error, closeListener func() error) *PostgresWaker {
	w := &PostgresWaker{
		wake:          make(chan struct{}, 1),
		done:          make(chan struct{}),
		closeListener: closeListener,
		closed:        make(chan struct{}),
	}
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

// Close closes the listener connection and stops forwarding, waiting for the
// close to finish. CloseContext bounds that wait.
func (w *PostgresWaker) Close() error {
	return w.CloseContext(context.Background())
}

// CloseContext closes the listener connection and stops forwarding. The
// listener is closed first: pq's goroutine then finishes and closes notify,
// which ends forward. forward keeps reading notify until then, so pq's
// goroutine never blocks on a full notify channel. A waker without a listener
// stops forward through done.
//
// Closing a pq listener waits for a connection attempt or a ping in progress,
// which a dead socket can hold for minutes, so the close runs on its own
// goroutine and CloseContext returns when ctx is done even if the close has
// not finished. The close starts only once; every call waits for that close
// and returns its result.
func (w *PostgresWaker) CloseContext(ctx context.Context) error {
	w.closeOnce.Do(func() {
		go func() {
			defer close(w.closed)
			if w.closeListener != nil {
				w.closeErr = w.closeListener()
				return
			}
			close(w.done)
		}()
	})
	select {
	case <-w.closed:
		return w.closeErr
	case <-ctx.Done():
		return fmt.Errorf("close outbox listener: %w", ctx.Err())
	}
}
