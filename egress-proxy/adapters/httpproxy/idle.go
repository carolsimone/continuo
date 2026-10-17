package httpproxy

import (
	"sync"
	"sync/atomic"
	"time"
)

// longAgo is a deadline already past: setting it on a connection ends a read
// or write blocked on it at once.
var longAgo = time.Unix(1, 0)

// idleTimer calls expire once no byte has moved, either way, for idle. Each
// touch records a byte. The check runs on the monotonic clock.
type idleTimer struct {
	start    time.Time
	last     atomic.Int64 // the latest byte, as an offset from start
	stopOnce sync.Once
	stopped  chan struct{}
	done     chan struct{}
}

// startIdleTimer starts the check. It looks every quarter of idle, and at
// least every 5 ms, so expire runs between idle and 1.25 idle after the
// latest byte.
func startIdleTimer(idle time.Duration, expire func()) *idleTimer {
	t := &idleTimer{start: time.Now(), stopped: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(t.done)
		tick := time.NewTicker(max(idle/4, 5*time.Millisecond))
		defer tick.Stop()
		for {
			select {
			case <-t.stopped:
				return
			case <-tick.C:
				if time.Since(t.start)-time.Duration(t.last.Load()) >= idle {
					expire()
					return
				}
			}
		}
	}()
	return t
}

// touch records that a byte moved now.
func (t *idleTimer) touch() { t.last.Store(int64(time.Since(t.start))) }

// stop ends the check. Once it returns, expire is not running and never
// runs. stop may be called more than once.
func (t *idleTimer) stop() {
	t.stopOnce.Do(func() { close(t.stopped) })
	<-t.done
}
