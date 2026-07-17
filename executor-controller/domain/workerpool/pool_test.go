package workerpool_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/workerpool"
	"github.com/stretchr/testify/assert"
)

// at is a readable instant builder for the tests below.
func at(seconds int) time.Time { return time.Unix(int64(seconds), 0).UTC() }

// TestAllocateServesOldestReadyPoolFirst pins the fairness rule: when the
// backlog exceeds the free capacity, the pool whose work has waited longest is
// served before one whose work arrived later.
func TestAllocateServesOldestReadyPoolFirst(t *testing.T) {
	got := workerpool.Allocate([]model.PoolDemand{
		{PoolKey: "recent", Pending: 3, OldestReadyAt: at(500)},
		{PoolKey: "oldest", Pending: 3, OldestReadyAt: at(100)},
	}, 0, 4)

	assert.Equal(t, 3, got["oldest"], "the longest-waiting pool is served first")
	assert.Equal(t, 1, got["recent"], "the later pool takes only what is left")
}

// TestAllocateNeverExceedsTheConfiguredLimit proves the cap is the configured
// limit minus the slots already held, not a literal.
func TestAllocateNeverExceedsTheConfiguredLimit(t *testing.T) {
	for _, tc := range []struct {
		name        string
		activeSlots int
		limit       int
		want        int
	}{
		{"free capacity", 0, 2, 2},
		{"partly held", 3, 5, 2},
		{"fully held", 5, 5, 0},
		{"over-subscribed", 9, 5, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := workerpool.Allocate([]model.PoolDemand{
				{PoolKey: "pool", Pending: 100, OldestReadyAt: at(1)},
			}, tc.activeSlots, tc.limit)
			assert.Equal(t, tc.want, got["pool"])
		})
	}
}

// TestAllocateGivesNothingToAPoolWithNoBacklog keeps an idle pool from holding
// capacity a busy pool could use.
func TestAllocateGivesNothingToAPoolWithNoBacklog(t *testing.T) {
	got := workerpool.Allocate([]model.PoolDemand{
		{PoolKey: "idle", Pending: 0, OldestReadyAt: at(1)},
		{PoolKey: "busy", Pending: 2, OldestReadyAt: at(2)},
	}, 0, 10)

	assert.Equal(t, 0, got["idle"])
	assert.Equal(t, 2, got["busy"], "an idle pool ahead in the order does not starve a busy one")
}

// TestAllocateLeavesTheCallersOrderAlone pins that ranking the demand is
// internal: the caller's slice is read, never reordered underneath it.
func TestAllocateLeavesTheCallersOrderAlone(t *testing.T) {
	demand := []model.PoolDemand{
		{PoolKey: "recent", Pending: 1, OldestReadyAt: at(500)},
		{PoolKey: "oldest", Pending: 1, OldestReadyAt: at(100)},
	}

	workerpool.Allocate(demand, 0, 10)

	assert.Equal(t, "recent", demand[0].PoolKey, "the caller's slice keeps its order")
	assert.Equal(t, "oldest", demand[1].PoolKey)
}

// TestDesiredReplicasNeverReducesBusyPool proves a pool holding leases is never
// scaled down, however long it has been since the idle clock last moved: the
// pods it would drop are running dbt.
func TestDesiredReplicasNeverReducesBusyPool(t *testing.T) {
	got := workerpool.DesiredReplicas(workerpool.ScaleInput{
		CurrentReplicas: 5, ActiveLeases: 2, AllocatedPending: 0,
		LastActivityAt: time.Unix(0, 0), Now: time.Unix(1000, 0),
		IdleTimeout: time.Minute,
	})
	assert.Equal(t, 5, got)
}

// TestDesiredReplicasGrowsABusyPoolForItsBacklog proves a pool that is both busy
// and backlogged is sized for the sum, not for either half.
func TestDesiredReplicasGrowsABusyPoolForItsBacklog(t *testing.T) {
	got := workerpool.DesiredReplicas(workerpool.ScaleInput{
		CurrentReplicas: 2, ActiveLeases: 2, AllocatedPending: 3,
		LastActivityAt: at(0), Now: at(10), IdleTimeout: time.Minute,
	})
	assert.Equal(t, 5, got, "two leases plus three allocated tasks need five pods")
}

// TestDesiredReplicasRestartsAScaledDownPool proves a pool sitting at zero comes
// back when work is allocated to it: an idle pool is asleep, not retired.
func TestDesiredReplicasRestartsAScaledDownPool(t *testing.T) {
	got := workerpool.DesiredReplicas(workerpool.ScaleInput{
		CurrentReplicas: 0, ActiveLeases: 0, AllocatedPending: 2,
		LastActivityAt: at(0), Now: at(10_000), IdleTimeout: time.Minute,
	})
	assert.Equal(t, 2, got, "an allocation restarts a pool that had scaled to zero")
}

// TestDesiredReplicasScalesAnIdlePoolToZero proves an unused pool stops costing
// anything once its idle timeout has passed.
func TestDesiredReplicasScalesAnIdlePoolToZero(t *testing.T) {
	got := workerpool.DesiredReplicas(workerpool.ScaleInput{
		CurrentReplicas: 3, ActiveLeases: 0, AllocatedPending: 0,
		LastActivityAt: at(0), Now: at(60), IdleTimeout: time.Minute,
	})
	assert.Equal(t, 0, got, "the idle timeout elapsed exactly, so the pool releases its pods")
}

// TestDesiredReplicasHoldsAQuietPoolInsideItsIdleTimeout proves a pool that has
// merely paused keeps its warm pods, so the next task does not pay a cold start.
func TestDesiredReplicasHoldsAQuietPoolInsideItsIdleTimeout(t *testing.T) {
	got := workerpool.DesiredReplicas(workerpool.ScaleInput{
		CurrentReplicas: 3, ActiveLeases: 0, AllocatedPending: 0,
		LastActivityAt: at(0), Now: at(59), IdleTimeout: time.Minute,
	})
	assert.Equal(t, 3, got, "the idle timeout has not elapsed, so the warm pods stay")
}

// TestDesiredReplicasCapsAPoolThatCannotInitialize proves a pool whose workers
// cannot hydrate their runtime artifact runs one diagnostic pod rather than a
// full complement: its pods cannot execute anything, so replicating the failure
// buys nothing but noise.
func TestDesiredReplicasCapsAPoolThatCannotInitialize(t *testing.T) {
	got := workerpool.DesiredReplicas(workerpool.ScaleInput{
		CurrentReplicas: 4, ActiveLeases: 0, AllocatedPending: 6,
		LastActivityAt: at(0), Now: at(10), IdleTimeout: time.Minute,
		InitializationFailed: true,
	})
	assert.Equal(t, 1, got, "a failed pool is capped at one diagnostic pod")
}

// TestDesiredReplicasDoesNotCapAFailedPoolBelowItsLeases proves the diagnostic
// cap withdraws capacity a pool cannot use, never a pod that is working: a pool
// is marked failed by any one worker that could not hydrate, so a pool serving
// leases can be marked failed while its other pods run dbt. Capping it to the
// diagnostic would drop those pods and strand their work.
func TestDesiredReplicasDoesNotCapAFailedPoolBelowItsLeases(t *testing.T) {
	got := workerpool.DesiredReplicas(workerpool.ScaleInput{
		CurrentReplicas: 5, ActiveLeases: 2, AllocatedPending: 0,
		LastActivityAt: at(0), Now: at(10), IdleTimeout: time.Minute,
		InitializationFailed: true,
	})
	assert.Equal(t, 5, got, "a failed pool holding leases keeps the pods running its dbt")
}

// TestDesiredReplicasCapsAFailedPoolOnceItsLeasesDrain proves the cap is only
// deferred while a failed pool is working, not abandoned: the tick that finds it
// holding nothing withdraws its capacity.
func TestDesiredReplicasCapsAFailedPoolOnceItsLeasesDrain(t *testing.T) {
	got := workerpool.DesiredReplicas(workerpool.ScaleInput{
		CurrentReplicas: 5, ActiveLeases: 0, AllocatedPending: 0,
		LastActivityAt: at(0), Now: at(10), IdleTimeout: time.Minute,
		InitializationFailed: true,
	})
	assert.Equal(t, 1, got, "the leases are gone, so the pool winds down to its diagnostic pod")
}

// TestDesiredReplicasStillRetiresAnIdleFailedPool proves the diagnostic cap is a
// ceiling and not a floor: a failed pool nobody is waiting on scales to zero
// like any other idle pool.
func TestDesiredReplicasStillRetiresAnIdleFailedPool(t *testing.T) {
	got := workerpool.DesiredReplicas(workerpool.ScaleInput{
		CurrentReplicas: 4, ActiveLeases: 0, AllocatedPending: 0,
		LastActivityAt: at(0), Now: at(600), IdleTimeout: time.Minute,
		InitializationFailed: true,
	})
	assert.Equal(t, 0, got, "the cap bounds a failed pool, it does not keep one alive")
}
