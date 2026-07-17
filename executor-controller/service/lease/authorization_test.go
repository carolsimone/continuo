package lease_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/lease"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// otherPool is a pool key no seeded task belongs to, standing in for a worker
// authenticated against a different team's pool.
const otherPool = "marketing:sha-zzz"

// TestStart_FromAnotherPoolIsRejected proves the executor checks that the task
// a caller reports on actually belongs to the pool it authenticated as, rather
// than inferring ownership from the lease token alone.
func TestStart_FromAnotherPoolIsRejected(t *testing.T) {
	h := newHarness(t, 10)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	err := h.svc.Start(context.Background(), lease.StartInput{
		PoolKey: otherPool, DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
	})

	assert.ErrorIs(t, err, lease.ErrPoolMismatch)
	// The rejected report left the task exactly as it was.
	assert.Equal(t, model.StatusLeased, h.repo.statusOf(dep.ID()))
	assert.Empty(t, h.outbox.entries)
}

func TestHeartbeat_FromAnotherPoolIsRejected(t *testing.T) {
	h := newHarness(t, 10)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	err := h.svc.Heartbeat(context.Background(), lease.HeartbeatInput{
		PoolKey: otherPool, DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
	})

	assert.ErrorIs(t, err, lease.ErrPoolMismatch)
}

func TestComplete_FromAnotherPoolIsRejected(t *testing.T) {
	h := newHarness(t, 10)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	err := h.svc.Complete(context.Background(), lease.CompleteInput{
		PoolKey: otherPool, DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{Succeeded: true},
	})

	assert.ErrorIs(t, err, lease.ErrPoolMismatch)
	assert.Equal(t, model.StatusLeased, h.repo.statusOf(dep.ID()))
	assert.Empty(t, h.outbox.entries)
}

// TestAuthorize_StaleLeaseDominatesPoolMismatch is the reason the pool check is
// safe: a caller who does not hold the lease is fenced before ownership is
// considered, so it cannot learn whether a task it guessed at exists.
func TestAuthorize_StaleLeaseDominatesPoolMismatch(t *testing.T) {
	h := newHarness(t, 10)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	in := lease.HeartbeatInput{
		PoolKey: otherPool, DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: "guessed-token",
	}
	err := h.svc.Heartbeat(context.Background(), in)

	assert.ErrorIs(t, err, model.ErrStaleLease)
	assert.NotErrorIs(t, err, lease.ErrPoolMismatch)
}

// TestHeartbeat_OnACancelledTaskTellsTheWorkerToStop. Cancelling keeps the lease
// on the task, so a worker that outlives its pod's termination grace period and
// heartbeats once more still authorizes. It must be told its task was cancelled
// unambiguously rather than fail as an unexpected status.
func TestHeartbeat_OnACancelledTaskTellsTheWorkerToStop(t *testing.T) {
	h := newHarness(t, 10)
	ctx := context.Background()
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	loaded, err := h.repo.GetByID(ctx, dep.ID())
	require.NoError(t, err)
	require.NoError(t, loaded.Cancel("schedule cancelled", h.clock.now))
	require.NoError(t, h.repo.Save(ctx, loaded))

	err = h.svc.Heartbeat(ctx, lease.HeartbeatInput{
		PoolKey: poolKey, DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
	})

	assert.ErrorIs(t, err, lease.ErrCancelled)
}

// TestHeartbeat_OnACancelledTaskStillFencesAStranger keeps cancellation from
// leaking to a caller that does not hold the lease.
func TestHeartbeat_OnACancelledTaskStillFencesAStranger(t *testing.T) {
	h := newHarness(t, 10)
	ctx := context.Background()
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	loaded, err := h.repo.GetByID(ctx, dep.ID())
	require.NoError(t, err)
	require.NoError(t, loaded.Cancel("schedule cancelled", h.clock.now))
	require.NoError(t, h.repo.Save(ctx, loaded))

	err = h.svc.Heartbeat(ctx, lease.HeartbeatInput{
		PoolKey: poolKey, DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: "guessed-token",
	})

	assert.ErrorIs(t, err, model.ErrStaleLease)
	assert.NotErrorIs(t, err, lease.ErrCancelled)
}

// TestComplete_OnACancelledTaskTellsTheWorkerToStop. Cancelling keeps the lease
// on the task, so a worker that finishes its dbt run inside its pod's
// termination grace period still authorizes. Its report must be answered
// "cancelled" — which the worker treats as final — rather than reaching the
// aggregate's status guard and coming back as a fault the worker would retry
// against for the life of its pod.
func TestComplete_OnACancelledTaskTellsTheWorkerToStop(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result model.WorkerResult
	}{
		{"success", model.WorkerResult{Succeeded: true}},
		{"failure", model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "boom"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, 10)
			ctx := context.Background()
			dep := h.seedDue(workerCmd())
			grant := h.mustClaim(t)

			loaded, err := h.repo.GetByID(ctx, dep.ID())
			require.NoError(t, err)
			require.NoError(t, loaded.Cancel("schedule cancelled", h.clock.now))
			require.NoError(t, h.repo.Save(ctx, loaded))

			err = h.svc.Complete(ctx, lease.CompleteInput{
				PoolKey: poolKey, DeploymentID: dep.ID(), LeaseID: grant.LeaseID,
				Token: grant.Token, Result: tc.result,
			})

			assert.ErrorIs(t, err, lease.ErrCancelled)
			// The cancelled task is what it was: a late report settles nothing and
			// announces nothing.
			assert.Equal(t, model.StatusCancelled, h.repo.statusOf(dep.ID()))
			assert.Empty(t, h.outbox.entries)
		})
	}
}

// TestComplete_OnACancelledTaskStillFencesAStranger keeps cancellation from
// leaking to a caller that does not hold the lease, exactly as the heartbeat
// path does: the fence runs first, so a guessed token learns only that its lease
// is stale and never that the task exists.
func TestComplete_OnACancelledTaskStillFencesAStranger(t *testing.T) {
	h := newHarness(t, 10)
	ctx := context.Background()
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	loaded, err := h.repo.GetByID(ctx, dep.ID())
	require.NoError(t, err)
	require.NoError(t, loaded.Cancel("schedule cancelled", h.clock.now))
	require.NoError(t, h.repo.Save(ctx, loaded))

	err = h.svc.Complete(ctx, lease.CompleteInput{
		PoolKey: poolKey, DeploymentID: dep.ID(), LeaseID: grant.LeaseID,
		Token: "guessed-token", Result: model.WorkerResult{Succeeded: true},
	})

	assert.ErrorIs(t, err, model.ErrStaleLease)
	assert.NotErrorIs(t, err, lease.ErrCancelled)
}

// TestTask_ReturnsTheRowsOwnIdentity is what result locations are derived from.
// The identifiers come from the row, never from the caller, so a worker cannot
// name a task it does not hold.
func TestTask_ReturnsTheRowsOwnIdentity(t *testing.T) {
	h := newHarness(t, 10)
	cmd := workerCmd()
	dep := h.seedDue(cmd)
	grant := h.mustClaim(t)

	ref, err := h.svc.Task(context.Background(), lease.TaskInput{
		PoolKey: poolKey, DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
	})

	require.NoError(t, err)
	assert.Equal(t, poolKey, ref.PoolKey)
	assert.Equal(t, cmd.TaskID, ref.Command.TaskID)
	assert.Equal(t, cmd.ScheduleID, ref.Command.ScheduleID)
}

func TestTask_IsFencedByTheLeaseToken(t *testing.T) {
	h := newHarness(t, 10)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	_, err := h.svc.Task(context.Background(), lease.TaskInput{
		PoolKey: poolKey, DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: "guessed-token",
	})

	assert.ErrorIs(t, err, model.ErrStaleLease)
}

func TestTask_IsFencedByTheLeaseID(t *testing.T) {
	h := newHarness(t, 10)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	_, err := h.svc.Task(context.Background(), lease.TaskInput{
		PoolKey: poolKey, DeploymentID: dep.ID(), LeaseID: uuid.New(), Token: grant.Token,
	})

	assert.ErrorIs(t, err, model.ErrStaleLease)
}

func TestTask_FromAnotherPoolIsRejected(t *testing.T) {
	h := newHarness(t, 10)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	_, err := h.svc.Task(context.Background(), lease.TaskInput{
		PoolKey: otherPool, DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
	})

	assert.ErrorIs(t, err, lease.ErrPoolMismatch)
}
