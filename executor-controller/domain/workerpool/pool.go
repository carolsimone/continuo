// Package workerpool sizes the pools of reusable worker pods: how the
// executor's free capacity is shared between pools that want it, and how many
// pods one pool should run once it has its share.
//
// Both decisions are pure functions of what a reconcile tick observed, so the
// sizing rules can be exercised without a cluster and cannot drift with the
// order a tick happens to read things in.
package workerpool

import (
	"sort"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
)

// diagnosticReplicas is the most a pool whose workers cannot initialize may run.
// Its pods can execute nothing, so more of them reproduce one failure many times
// over; one pod keeps the failure inspectable without paying for the rest.
const diagnosticReplicas = 1

// ScaleInput is one pool's state at a reconcile tick.
type ScaleInput struct {
	// CurrentReplicas is what the pool's Deployment asks for right now.
	CurrentReplicas int
	// ActiveLeases is how many of the pool's tasks a worker currently holds.
	ActiveLeases int
	// AllocatedPending is the share of the executor's free capacity this tick
	// gave the pool's backlog.
	AllocatedPending int
	// LastActivityAt is when the pool last had work to do. Now and IdleTimeout
	// decide, against it, whether a pool with nothing to do is merely paused or
	// has been unused long enough to release its pods.
	LastActivityAt time.Time
	Now            time.Time
	IdleTimeout    time.Duration
	// InitializationFailed reports that the pool's workers cannot hydrate their
	// runtime artifact, which caps the pool at a single diagnostic pod.
	InitializationFailed bool
}

// DesiredReplicas returns how many pods the pool should run.
//
// A pool holding leases is never scaled down: those pods are running dbt against
// the warehouse, and dropping one does not stop its work — it strands it. So a
// busy pool only ever grows, to cover its leases plus whatever backlog this tick
// allocated it.
//
// With no leases, the pool is sized for its allocation; with no allocation
// either, it keeps its warm pods until the idle timeout elapses and then
// releases them, so a pause costs nothing but a lull does not force the next
// task to pay a cold start.
func DesiredReplicas(in ScaleInput) int {
	desired := desiredReplicas(in)
	if in.InitializationFailed && desired > diagnosticReplicas {
		return diagnosticReplicas
	}
	return desired
}

// desiredReplicas is the sizing rule before the diagnostic cap applies.
func desiredReplicas(in ScaleInput) int {
	if in.ActiveLeases > 0 {
		return max(in.CurrentReplicas, in.ActiveLeases+in.AllocatedPending)
	}
	if in.AllocatedPending > 0 {
		return in.AllocatedPending
	}
	if in.Now.Sub(in.LastActivityAt) >= in.IdleTimeout {
		return 0
	}
	return in.CurrentReplicas
}

// Allocate shares the executor's free capacity between the pools that want it,
// returning how many of each pool's pending tasks this tick may start.
//
// limit is the executor's configured ceiling and activeSlots is what Jobs-mode
// and worker-mode work already hold, so both paths draw on one budget and
// workers can never oversubscribe the cluster.
//
// The budget goes to the pool whose work has waited longest first. That is what
// keeps one pool's large backlog from starving another's older task: ranking by
// arrival rather than by size means every pool's oldest work advances before any
// pool's newest does.
//
// The ranking is internal — the caller's slice is copied, not reordered — so
// Allocate reads its input and returns its answer, and nothing else.
func Allocate(demand []model.PoolDemand, activeSlots, limit int) map[string]int {
	available := max(0, limit-activeSlots)

	ranked := make([]model.PoolDemand, len(demand))
	copy(ranked, demand)
	sort.SliceStable(ranked, func(i, j int) bool {
		return ranked[i].OldestReadyAt.Before(ranked[j].OldestReadyAt)
	})

	allocated := make(map[string]int, len(ranked))
	for _, pool := range ranked {
		n := min(pool.Pending, available)
		allocated[pool.PoolKey] = n
		available -= n
		if available == 0 {
			break
		}
	}
	return allocated
}
