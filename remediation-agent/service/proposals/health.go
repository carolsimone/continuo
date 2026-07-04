package proposals

import "sync"

// ReconcileHealth reports whether the PR-outcome reconciler can currently read
// pull request state from GitHub. It flips to degraded when a pass hits a
// permission error (the token lacks Pull requests: Read) and clears once a pass
// reads cleanly. It is safe for concurrent use by the reconcile goroutine and
// an HTTP status handler.
type ReconcileHealth struct {
	mu       sync.RWMutex
	degraded bool
	reason   string
}

// NewReconcileHealth returns a healthy ReconcileHealth.
func NewReconcileHealth() *ReconcileHealth { return &ReconcileHealth{} }

// Degraded reports whether PR reads are currently failing on a permission error.
func (h *ReconcileHealth) Degraded() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.degraded
}

// Reason returns the human-actionable explanation while degraded, or "" when
// healthy.
func (h *ReconcileHealth) Reason() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.reason
}

// Snapshot returns the degraded flag and reason atomically.
func (h *ReconcileHealth) Snapshot() (degraded bool, reason string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.degraded, h.reason
}

// markDegraded records a permission failure and reports whether this was a
// transition from healthy, so the caller can log the actionable error once.
func (h *ReconcileHealth) markDegraded(reason string) (transitioned bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	transitioned = !h.degraded
	h.degraded = true
	h.reason = reason
	return transitioned
}

// markHealthy clears a prior degraded state and reports whether this was a
// transition from degraded, so the caller can log recovery once.
func (h *ReconcileHealth) markHealthy() (transitioned bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	transitioned = h.degraded
	h.degraded = false
	h.reason = ""
	return transitioned
}
