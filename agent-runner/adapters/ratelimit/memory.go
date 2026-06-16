package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/carolsimone/continuo/agent-runner/service/ports"
)

// Memory is a process-local sliding-window rate limiter. It caps user messages
// per rolling one-minute window. With more than one agent-runner replica the
// limit becomes per-replica rather than global; use Redis for a shared limit.
type Memory struct {
	mu        sync.Mutex
	perMinute int
	hits      map[string][]time.Time
	// now is the clock source, overridable in tests.
	now func() time.Time
}

var _ ports.RateLimiter = (*Memory)(nil)

// NewMemory creates an in-memory limiter allowing perMinute messages per user.
func NewMemory(perMinute int) *Memory {
	return &Memory{perMinute: perMinute, hits: map[string][]time.Time{}, now: time.Now}
}

// Allow reports whether userID may send another message now. It never returns
// an error; the signature matches ports.RateLimiter so it is interchangeable
// with the Redis-backed limiter.
func (m *Memory) Allow(_ context.Context, userID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	cutoff := now.Add(-time.Minute)
	kept := m.hits[userID][:0]
	for _, h := range m.hits[userID] {
		if h.After(cutoff) {
			kept = append(kept, h)
		}
	}
	if len(kept) >= m.perMinute {
		m.hits[userID] = kept
		return false, nil
	}
	m.hits[userID] = append(kept, now)
	return true, nil
}
