// Package liveness provides a process-wide health registry that tracks the
// health of background workers (Redis stream consumers, the outbox processor)
// and cached dependency probes. The HTTP health adapter reads the registry to
// answer two distinct probes with deliberately different semantics:
//
//   - Readiness (Check / Ready): workers + worker heartbeats + dependency
//     probes. A dependency outage (Redis/Postgres unreachable) must pull the
//     pod out of the Service's endpoints so no traffic is routed to it.
//   - Liveness (LivenessCheck / Live): workers + worker heartbeats ONLY — no
//     dependency probes. A dependency outage must NOT restart the process:
//     the consumers are already retrying through it, and restarting a pod
//     whose backing store is briefly down just turns a recoverable outage into
//     CrashLoopBackOff and delays recovery. Only a dead or wedged worker
//     goroutine (a heartbeat gone stale, or a worker that exited with an error)
//     should restart the pod.
package liveness

import (
	"context"
	"sync"
	"time"
)

// Registry aggregates the liveness of named background workers, their read-loop
// heartbeat probes, and external dependency probes. It is safe for concurrent
// use. Worker registrations and worker heartbeat probes feed BOTH the readiness
// and liveness checks; dependency probes feed readiness ONLY.
type Registry struct {
	mu           sync.Mutex
	workers      map[string]error
	workerProbes []*Probe // liveness-relevant: consumer read-loop heartbeats
	depProbes    []*Probe // readiness-only: external dependency pings
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{workers: make(map[string]error)}
}

// RegisterWorker records a worker name as live (no error). Call before starting
// the worker goroutine so a missing worker is observable from the first probe.
// Worker registrations feed both readiness and liveness.
func (r *Registry) RegisterWorker(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[name] = nil
}

// WorkerExited records that the named worker has stopped. A nil err means a
// clean stop in response to shutdown; a non-nil err marks the worker unhealthy
// and flips BOTH readiness and liveness until the process restarts — so a
// consumer that exited on a permanent, unretryable error (e.g. a wrong-typed
// stream key that no retry can fix) surfaces loudly instead of silently
// consuming nothing.
func (r *Registry) WorkerExited(name string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers[name] = err
}

// Probe is a cached health check. The check runs at most once per TTL; both
// probes reuse the cached result in between so the endpoints stay cheap under
// Kubernetes probe traffic.
type Probe struct {
	name  string
	ttl   time.Duration
	check func(ctx context.Context) error

	mu        sync.Mutex
	lastRun   time.Time
	lastErr   error
	hasResult bool
}

// AddProbe registers a cached DEPENDENCY probe evaluated with the given TTL.
// Dependency probes feed readiness ONLY (not liveness), so a dependency outage
// stops traffic without restarting the pod. This is an alias for
// AddDependencyProbe kept for existing callers.
func (r *Registry) AddProbe(name string, ttl time.Duration, check func(ctx context.Context) error) {
	r.AddDependencyProbe(name, ttl, check)
}

// AddDependencyProbe registers a cached probe over an external dependency
// (Redis, Postgres, …). It feeds readiness ONLY, never liveness.
func (r *Registry) AddDependencyProbe(name string, ttl time.Duration, check func(ctx context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.depProbes = append(r.depProbes, &Probe{name: name, ttl: ttl, check: check})
}

// AddWorkerProbe registers a cached probe over a background worker's own health
// (e.g. a consumer read-loop heartbeat). Worker probes feed BOTH readiness and
// liveness, so a wedged consumer goroutine restarts the pod.
func (r *Registry) AddWorkerProbe(name string, ttl time.Duration, check func(ctx context.Context) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workerProbes = append(r.workerProbes, &Probe{name: name, ttl: ttl, check: check})
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

// gather evaluates the workers, the worker probes, and (only when includeDeps)
// the dependency probes, returning every failure. It is the shared core of
// Check (readiness, includeDeps=true) and LivenessCheck (includeDeps=false).
func (r *Registry) gather(ctx context.Context, includeDeps bool) []Failure {
	r.mu.Lock()
	workers := make(map[string]error, len(r.workers))
	for name, err := range r.workers {
		workers[name] = err
	}
	workerProbes := make([]*Probe, len(r.workerProbes))
	copy(workerProbes, r.workerProbes)
	var depProbes []*Probe
	if includeDeps {
		depProbes = make([]*Probe, len(r.depProbes))
		copy(depProbes, r.depProbes)
	}
	r.mu.Unlock()

	var failures []Failure
	for name, err := range workers {
		if err != nil {
			failures = append(failures, Failure{Name: name, Err: err})
		}
	}
	now := time.Now()
	for _, p := range workerProbes {
		if err := p.evaluate(ctx, now); err != nil {
			failures = append(failures, Failure{Name: p.name, Err: err})
		}
	}
	for _, p := range depProbes {
		if err := p.evaluate(ctx, now); err != nil {
			failures = append(failures, Failure{Name: p.name, Err: err})
		}
	}
	return failures
}

// Check returns the set of current READINESS failures: unhealthy workers,
// stale worker heartbeats, and failed dependency probes. An empty slice means
// ready.
func (r *Registry) Check(ctx context.Context) []Failure {
	return r.gather(ctx, true)
}

// LivenessCheck returns the set of current LIVENESS failures: unhealthy workers
// and stale worker heartbeats ONLY. Dependency probes are deliberately excluded
// so a transient Redis/Postgres outage does not restart a pod whose consumers
// are already retrying through it. An empty slice means live.
func (r *Registry) LivenessCheck(ctx context.Context) []Failure {
	return r.gather(ctx, false)
}

// Ready reports whether every worker is live, every heartbeat is fresh, and
// every dependency probe passes.
func (r *Registry) Ready(ctx context.Context) bool {
	return len(r.Check(ctx)) == 0
}

// Live reports whether every worker is live and every heartbeat is fresh,
// independent of external dependencies.
func (r *Registry) Live(ctx context.Context) bool {
	return len(r.LivenessCheck(ctx)) == 0
}
