// Package liveness provides a process-wide readiness registry that tracks the
// health of background workers (Redis stream consumers, the outbox processor)
// and cached dependency probes. The HTTP health adapter reads the registry to
// answer /ready, so Kubernetes can stop routing traffic to a pod whose
// consumers have exited or whose backing stores are unreachable.
package liveness

import (
	"context"
	"sync"
	"time"
)

// Registry aggregates the liveness of named background workers and dependency
// probes. It is safe for concurrent use.
type Registry struct {
	mu      sync.Mutex
	workers map[string]error
	probes  []*Probe
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{workers: make(map[string]error)}
}

// RegisterWorker records a worker name as live (no error). Call before starting
// the worker goroutine so a missing worker is observable from the first probe.
func (r *Registry) RegisterWorker(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[name] = nil
}

// WorkerExited records that the named worker has stopped. A nil err means a
// clean stop in response to shutdown; a non-nil err marks the worker unhealthy
// and flips readiness until the process restarts.
func (r *Registry) WorkerExited(name string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[name] = err
}

// Probe is a cached health check over an external dependency. The check runs at
// most once per TTL; readiness reuses the cached result in between so /ready
// stays cheap under Kubernetes probe traffic.
type Probe struct {
	name  string
	ttl   time.Duration
	check func(ctx context.Context) error

	mu        sync.Mutex
	lastRun   time.Time
	lastErr   error
	hasResult bool
}

// AddProbe registers a cached dependency probe evaluated with the given TTL.
func (r *Registry) AddProbe(name string, ttl time.Duration, check func(ctx context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probes = append(r.probes, &Probe{name: name, ttl: ttl, check: check})
}

// evaluate returns the cached probe result, refreshing it if the TTL elapsed.
func (p *Probe) evaluate(ctx context.Context, now time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hasResult && now.Sub(p.lastRun) < p.ttl {
		return p.lastErr
	}
	p.lastErr = p.check(ctx)
	p.lastRun = now
	p.hasResult = true
	return p.lastErr
}

// Failure describes one unhealthy worker or probe.
type Failure struct {
	Name string
	Err  error
}

// Check returns the set of current failures. An empty slice means ready.
func (r *Registry) Check(ctx context.Context) []Failure {
	r.mu.Lock()
	workers := make(map[string]error, len(r.workers))
	for name, err := range r.workers {
		workers[name] = err
	}
	probes := make([]*Probe, len(r.probes))
	copy(probes, r.probes)
	r.mu.Unlock()

	var failures []Failure
	for name, err := range workers {
		if err != nil {
			failures = append(failures, Failure{Name: name, Err: err})
		}
	}
	now := time.Now()
	for _, p := range probes {
		if err := p.evaluate(ctx, now); err != nil {
			failures = append(failures, Failure{Name: p.name, Err: err})
		}
	}
	return failures
}

// Ready reports whether every worker is live and every dependency probe passes.
func (r *Registry) Ready(ctx context.Context) bool {
	return len(r.Check(ctx)) == 0
}
