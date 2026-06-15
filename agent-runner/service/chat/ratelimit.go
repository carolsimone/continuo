package chat

import (
	"sync"
	"time"
)

// RateLimiter caps user messages per sliding one-minute window.
type RateLimiter struct {
	mu        sync.Mutex
	perMinute int
	hits      map[string][]time.Time
}

func NewRateLimiter(perMinute int) *RateLimiter {
	return &RateLimiter{perMinute: perMinute, hits: map[string][]time.Time{}}
}

func (r *RateLimiter) Allow(userID string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := now.Add(-time.Minute)
	kept := r.hits[userID][:0]
	for _, h := range r.hits[userID] {
		if h.After(cutoff) {
			kept = append(kept, h)
		}
	}
	if len(kept) >= r.perMinute {
		r.hits[userID] = kept
		return false
	}
	r.hits[userID] = append(kept, now)
	return true
}
