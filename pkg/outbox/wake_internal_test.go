package outbox

import (
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
	w := newWaker(notify, func() error { return nil })
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
