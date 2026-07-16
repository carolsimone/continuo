package model_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sha256Hex mirrors what the lease application service persists: only the
// digest of the raw token ever reaches the aggregate or the database.
func sha256Hex(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// poolFixture is the worker pool key a worker-mode deployment is routed to.
func poolFixture() string { return "dbt:sha-abc" }

func argvFixture() []string { return []string{"dbt", "run", "--select", "orders"} }

// claimed builds a worker deployment that has been claimed by worker-1 and
// returns it with the raw lease token.
func claimed(t *testing.T) (*model.Deployment, string, uuid.UUID) {
	t.Helper()
	dep := model.NewWorkerDeployment(deployableCmd(), uuid.Nil, poolFixture(), time.Unix(10, 0))
	token := strings.Repeat("1", 64)
	leaseID := uuid.New()
	require.NoError(t, dep.Claim(leaseID, sha256Hex(token), "worker-1", "pod-a", "uid-a",
		time.Unix(20, 0), time.Unix(80, 0), argvFixture(), model.ExecutionPathNative))
	return dep, token, leaseID
}

func TestNewWorkerDeployment_Defaults(t *testing.T) {
	dep := model.NewWorkerDeployment(deployableCmd(), uuid.Nil, poolFixture(), time.Unix(10, 0))

	assert.Equal(t, model.StatusPending, dep.Status())
	assert.Equal(t, model.ExecutionModeWorkers, dep.ExecutionMode())
	assert.Equal(t, model.ModeProduction, dep.Mode(), "workers execute production records")
	assert.Equal(t, poolFixture(), dep.PoolKey())
	assert.Equal(t, time.Unix(10, 0), dep.NextAttemptAt(), "due immediately")
	assert.Nil(t, dep.ActiveLease(), "no lease until a worker claims it")
	assert.Nil(t, dep.Reservation().ReservedAt, "a pending task holds no slot")
	assert.Nil(t, dep.MessageProcessingID(), "uuid.Nil records no originating message")
}

func TestNewWorkerDeployment_KeepsMessageProcessingID(t *testing.T) {
	msgProcID := uuid.New()
	dep := model.NewWorkerDeployment(deployableCmd(), msgProcID, poolFixture(), time.Unix(10, 0))

	require.NotNil(t, dep.MessageProcessingID())
	assert.Equal(t, msgProcID, *dep.MessageProcessingID())
}

func TestNewDeployment_DefaultsToJobsExecutionMode(t *testing.T) {
	dep := model.NewDeployment(deployableCmd(), nil, time.Unix(10, 0))

	assert.Equal(t, model.ExecutionModeJobs, dep.ExecutionMode())
	assert.Empty(t, dep.PoolKey(), "a Jobs-mode deployment is routed to no pool")
}

// TestWorkerLeaseLifecycleAndStaleTokenFence walks claim -> start -> complete
// and asserts a stale token cannot drive the terminal transition.
func TestWorkerLeaseLifecycleAndStaleTokenFence(t *testing.T) {
	dep := model.NewWorkerDeployment(deployableCmd(), uuid.Nil, poolFixture(), time.Unix(10, 0))
	token := strings.Repeat("1", 64)
	leaseID := uuid.New()

	require.NoError(t, dep.Claim(leaseID, sha256Hex(token), "worker-1", "pod-a", "uid-a",
		time.Unix(20, 0), time.Unix(80, 0), argvFixture(), model.ExecutionPathNative))

	started, err := dep.AcknowledgeStart(leaseID, sha256Hex(token), time.Unix(21, 0))
	require.NoError(t, err)
	assert.True(t, started)

	assert.ErrorIs(t,
		dep.Complete(leaseID, sha256Hex("stale"), model.WorkerResult{Succeeded: true}, time.Unix(30, 0)),
		model.ErrStaleLease)

	require.NoError(t,
		dep.Complete(leaseID, sha256Hex(token), model.WorkerResult{Succeeded: true}, time.Unix(30, 0)))
	assert.Equal(t, model.StatusSucceeded, dep.Status())
	assert.NotNil(t, dep.SlotReleasedAt())
}

func TestClaim_RecordsLeaseIdentityAndReservesSlot(t *testing.T) {
	dep, token, leaseID := claimed(t)

	assert.Equal(t, model.StatusLeased, dep.Status())
	assert.Equal(t, argvFixture(), dep.ResolvedArgv())
	assert.Equal(t, model.ExecutionPathNative, dep.ExecutionPath())

	lease := dep.ActiveLease()
	require.NotNil(t, lease)
	assert.Equal(t, leaseID, lease.ID)
	assert.Equal(t, sha256Hex(token), lease.TokenSHA256)
	assert.Equal(t, "worker-1", lease.Owner)
	assert.Equal(t, "pod-a", lease.PodName)
	assert.Equal(t, "uid-a", lease.PodUID)
	assert.Equal(t, 1, lease.Attempt, "first claim is attempt 1")
	assert.Equal(t, time.Unix(80, 0), lease.ExpiresAt)
	assert.Equal(t, time.Unix(20, 0), lease.HeartbeatAt, "the claim itself is the first heartbeat")
	assert.Nil(t, lease.StartedAt)

	assert.Equal(t, time.Unix(20, 0), *dep.Reservation().ReservedAt, "a lease holds an execution slot")
	assert.Nil(t, dep.SlotReleasedAt())

	assert.NotContains(t, lease.TokenSHA256, token, "the raw token is never retained")
}

func TestClaim_NeverRetainsRawToken(t *testing.T) {
	dep, token, _ := claimed(t)

	// The aggregate is handed a digest and stores exactly that digest; no field
	// can hand the raw secret back out.
	assert.Equal(t, sha256Hex(token), dep.ActiveLease().TokenSHA256)
	assert.NotEqual(t, token, dep.ActiveLease().TokenSHA256)
}

func TestClaim_OnlyFromDuePending(t *testing.T) {
	t.Run("not yet due", func(t *testing.T) {
		dep := model.NewWorkerDeployment(deployableCmd(), uuid.Nil, poolFixture(), time.Unix(100, 0))
		err := dep.Claim(uuid.New(), sha256Hex("t"), "w", "p", "u",
			time.Unix(20, 0), time.Unix(80, 0), argvFixture(), model.ExecutionPathNative)
		require.Error(t, err, "a task whose backoff has not elapsed is not claimable")
	})

	t.Run("already leased", func(t *testing.T) {
		dep, _, _ := claimed(t)
		err := dep.Claim(uuid.New(), sha256Hex("t2"), "worker-2", "pod-b", "uid-b",
			time.Unix(25, 0), time.Unix(90, 0), argvFixture(), model.ExecutionPathNative)
		require.Error(t, err, "a leased task must not be handed to a second worker")
		assert.Equal(t, "worker-1", dep.ActiveLease().Owner, "the original lease stands")
	})

	t.Run("jobs mode", func(t *testing.T) {
		dep := model.NewDeployment(deployableCmd(), nil, time.Unix(10, 0))
		err := dep.Claim(uuid.New(), sha256Hex("t"), "w", "p", "u",
			time.Unix(20, 0), time.Unix(80, 0), argvFixture(), model.ExecutionPathNative)
		require.Error(t, err, "only worker-mode deployments are claimable")
	})
}

func TestAcknowledgeStart_DuplicateIsIdempotent(t *testing.T) {
	dep, token, leaseID := claimed(t)

	started, err := dep.AcknowledgeStart(leaseID, sha256Hex(token), time.Unix(21, 0))
	require.NoError(t, err)
	assert.True(t, started)
	assert.Equal(t, model.StatusRunning, dep.Status())
	assert.Equal(t, time.Unix(21, 0), *dep.ActiveLease().StartedAt)

	// A worker retrying its start call must not move started_at or error out.
	started, err = dep.AcknowledgeStart(leaseID, sha256Hex(token), time.Unix(29, 0))
	require.NoError(t, err)
	assert.False(t, started, "a duplicate start reports no change")
	assert.Equal(t, time.Unix(21, 0), *dep.ActiveLease().StartedAt, "started_at is not moved")
}

func TestAcknowledgeStart_FencesStaleLease(t *testing.T) {
	dep, token, leaseID := claimed(t)

	_, err := dep.AcknowledgeStart(uuid.New(), sha256Hex(token), time.Unix(21, 0))
	assert.ErrorIs(t, err, model.ErrStaleLease, "a different lease ID is stale")
	_, err = dep.AcknowledgeStart(leaseID, sha256Hex("stale"), time.Unix(21, 0))
	assert.ErrorIs(t, err, model.ErrStaleLease, "a wrong token is stale")
	assert.Equal(t, model.StatusLeased, dep.Status(), "a fenced call mutates nothing")
}

func TestHeartbeat_ExtendsExpiry(t *testing.T) {
	dep, _, _ := claimed(t)

	require.NoError(t, dep.Heartbeat(time.Unix(60, 0), time.Unix(120, 0)))
	assert.Equal(t, time.Unix(120, 0), dep.ActiveLease().ExpiresAt)
	assert.Equal(t, time.Unix(60, 0), dep.ActiveLease().HeartbeatAt)
	assert.Equal(t, model.StatusLeased, dep.Status(), "a heartbeat is not a state change")
}

func TestHeartbeat_AllowedWhileRunning(t *testing.T) {
	dep, token, leaseID := claimed(t)
	_, err := dep.AcknowledgeStart(leaseID, sha256Hex(token), time.Unix(21, 0))
	require.NoError(t, err)

	require.NoError(t, dep.Heartbeat(time.Unix(60, 0), time.Unix(120, 0)))
	assert.Equal(t, model.StatusRunning, dep.Status())
}

func TestHeartbeat_RejectedOnTerminalDeployment(t *testing.T) {
	dep, token, leaseID := claimed(t)
	require.NoError(t, dep.Complete(leaseID, sha256Hex(token),
		model.WorkerResult{Succeeded: true}, time.Unix(30, 0)))

	require.Error(t, dep.Heartbeat(time.Unix(60, 0), time.Unix(120, 0)),
		"a finished task holds no lease to extend")
}

func TestHeartbeat_NeverShortensExpiry(t *testing.T) {
	dep, _, _ := claimed(t)

	require.NoError(t, dep.Heartbeat(time.Unix(60, 0), time.Unix(120, 0)))
	require.Error(t, dep.Heartbeat(time.Unix(61, 0), time.Unix(90, 0)),
		"an out-of-order heartbeat must not pull the expiry back in")
	assert.Equal(t, time.Unix(120, 0), dep.ActiveLease().ExpiresAt)
}

func TestComplete_RecordsTerminalResultAndReleasesSlot(t *testing.T) {
	dep, token, leaseID := claimed(t)
	result := model.WorkerResult{
		Succeeded:        true,
		ExecutionSeconds: 12.5,
		LogS3URI:         "s3://logs/dbt.log",
		RunResultsS3URI:  "s3://logs/run_results.json",
	}

	require.NoError(t, dep.Complete(leaseID, sha256Hex(token), result, time.Unix(30, 0)))

	assert.Equal(t, model.StatusSucceeded, dep.Status())
	assert.Equal(t, time.Unix(30, 0), *dep.ActiveLease().FinishedAt)
	assert.Equal(t, time.Unix(30, 0), *dep.SlotReleasedAt())
	require.NotNil(t, dep.TerminalResult())
	assert.Equal(t, result, *dep.TerminalResult())
}

func TestComplete_DuplicateWithSameResultIsIdempotent(t *testing.T) {
	dep, token, leaseID := claimed(t)
	result := model.WorkerResult{Succeeded: true, ExecutionSeconds: 12.5}

	require.NoError(t, dep.Complete(leaseID, sha256Hex(token), result, time.Unix(30, 0)))
	require.NoError(t, dep.Complete(leaseID, sha256Hex(token), result, time.Unix(45, 0)),
		"a worker retrying its completion call succeeds idempotently")

	assert.Equal(t, time.Unix(30, 0), *dep.ActiveLease().FinishedAt, "the first completion stands")
	assert.Equal(t, time.Unix(30, 0), *dep.SlotReleasedAt())
}

func TestComplete_DuplicateWithDifferentResultIsRejected(t *testing.T) {
	dep, token, leaseID := claimed(t)

	require.NoError(t, dep.Complete(leaseID, sha256Hex(token),
		model.WorkerResult{Succeeded: true}, time.Unix(30, 0)))

	err := dep.Complete(leaseID, sha256Hex(token),
		model.WorkerResult{Succeeded: true, ExecutionSeconds: 99}, time.Unix(45, 0))
	require.Error(t, err, "a conflicting second result is not idempotent")
	assert.NotErrorIs(t, err, model.ErrStaleLease)
	assert.Equal(t, model.StatusSucceeded, dep.Status())
}

func TestComplete_StaleLeaseAfterTerminalIsFenced(t *testing.T) {
	dep, token, leaseID := claimed(t)
	require.NoError(t, dep.Complete(leaseID, sha256Hex(token),
		model.WorkerResult{Succeeded: true}, time.Unix(30, 0)))

	assert.ErrorIs(t, dep.Complete(uuid.New(), sha256Hex("other"),
		model.WorkerResult{Succeeded: true}, time.Unix(45, 0)), model.ErrStaleLease,
		"a superseded worker cannot report on a finished task")
}

func TestComplete_RequiresSucceededResult(t *testing.T) {
	dep, token, leaseID := claimed(t)

	err := dep.Complete(leaseID, sha256Hex(token),
		model.WorkerResult{Succeeded: false}, time.Unix(30, 0))
	require.Error(t, err, "a failed result travels through MarkRetryPending/MarkFailed")
	assert.Equal(t, model.StatusLeased, dep.Status())
}

func TestComplete_RequiresLease(t *testing.T) {
	dep := model.NewWorkerDeployment(deployableCmd(), uuid.Nil, poolFixture(), time.Unix(10, 0))

	assert.ErrorIs(t, dep.Complete(uuid.New(), sha256Hex("t"),
		model.WorkerResult{Succeeded: true}, time.Unix(30, 0)), model.ErrStaleLease,
		"an unclaimed task has no lease to complete")
}

func TestMarkRetryPending_ReleasesSlotAndSchedulesBackoff(t *testing.T) {
	dep, _, _ := claimed(t)

	require.NoError(t, dep.MarkRetryPending(time.Unix(90, 0), 30*time.Second))

	assert.Equal(t, model.StatusRetryPending, dep.Status())
	assert.Equal(t, time.Unix(120, 0), dep.NextAttemptAt(), "requeued after its backoff")
	assert.Equal(t, time.Unix(90, 0), *dep.SlotReleasedAt(), "the slot is freed in-transition")
	assert.Nil(t, dep.ActiveLease(), "the expired lease is dropped")
}

func TestMarkRetryPending_RejectedFromTerminal(t *testing.T) {
	dep, token, leaseID := claimed(t)
	require.NoError(t, dep.Complete(leaseID, sha256Hex(token),
		model.WorkerResult{Succeeded: true}, time.Unix(30, 0)))

	require.Error(t, dep.MarkRetryPending(time.Unix(90, 0), 30*time.Second))
}

func TestMarkFailed_ReleasesSlot(t *testing.T) {
	dep, _, _ := claimed(t)

	require.NoError(t, dep.MarkFailed(time.Unix(90, 0), "worker lease expired"))

	assert.Equal(t, model.StatusFailed, dep.Status())
	assert.Equal(t, "worker lease expired", *dep.ErrorMessage())
	assert.Equal(t, time.Unix(90, 0), *dep.SlotReleasedAt())
}

func TestMarkFailed_RejectedFromTerminal(t *testing.T) {
	dep, token, leaseID := claimed(t)
	require.NoError(t, dep.Complete(leaseID, sha256Hex(token),
		model.WorkerResult{Succeeded: true}, time.Unix(30, 0)))

	require.Error(t, dep.MarkFailed(time.Unix(90, 0), "too late"))
}

func TestCancel_ReleasesSlotFromLeased(t *testing.T) {
	dep, _, _ := claimed(t)

	require.NoError(t, dep.Cancel("schedule cancelled", time.Unix(50, 0)))

	assert.Equal(t, model.StatusCancelled, dep.Status())
	assert.Equal(t, "schedule cancelled", *dep.ErrorMessage())
	assert.Equal(t, time.Unix(50, 0), *dep.SlotReleasedAt())
}

// TestCancel_FromPendingHoldsNoSlot pins that cancelling work that never
// reserved a slot leaves slot_released_at NULL, satisfying the table's
// released-implies-reserved check.
func TestCancel_FromPendingHoldsNoSlot(t *testing.T) {
	dep := model.NewWorkerDeployment(deployableCmd(), uuid.Nil, poolFixture(), time.Unix(10, 0))

	require.NoError(t, dep.Cancel("schedule cancelled", time.Unix(50, 0)))

	assert.Equal(t, model.StatusCancelled, dep.Status())
	assert.Nil(t, dep.Reservation().ReservedAt)
	assert.Nil(t, dep.SlotReleasedAt(), "nothing to release")
}

func TestCancel_RejectedFromTerminal(t *testing.T) {
	dep, token, leaseID := claimed(t)
	require.NoError(t, dep.Complete(leaseID, sha256Hex(token),
		model.WorkerResult{Succeeded: true}, time.Unix(30, 0)))

	require.Error(t, dep.Cancel("too late", time.Unix(50, 0)))
	assert.Equal(t, model.StatusSucceeded, dep.Status())
}

// TestReserveForDispatch_HoldsSlotForAJob covers the Jobs path: the dispatcher
// reserves a slot and parks the row in 'dispatching' before it creates the
// Kubernetes Job, so a crash between create and commit cannot undercount.
func TestReserveForDispatch_HoldsSlotForAJob(t *testing.T) {
	dep := model.NewDeployment(deployableCmd(), nil, time.Unix(10, 0))

	require.NoError(t, dep.ReserveForDispatch(time.Unix(20, 0)))

	assert.Equal(t, model.StatusDispatching, dep.Status())
	assert.Equal(t, time.Unix(20, 0), *dep.Reservation().ReservedAt)
	assert.Nil(t, dep.SlotReleasedAt(), "a Kubernetes Job terminal event releases this slot")
}

func TestReserveForDispatch_OnlyFromDuePending(t *testing.T) {
	notDue := model.NewDeployment(deployableCmd(), nil, time.Unix(100, 0))
	require.Error(t, notDue.ReserveForDispatch(time.Unix(20, 0)))

	reserved := model.NewDeployment(deployableCmd(), nil, time.Unix(10, 0))
	require.NoError(t, reserved.ReserveForDispatch(time.Unix(20, 0)))
	require.Error(t, reserved.ReserveForDispatch(time.Unix(21, 0)), "a slot is reserved once")
}

// TestMarkDeployed_FromDispatching keeps the reserve-then-create Jobs flow
// intact: the Job is created while the row sits in 'dispatching'.
func TestMarkDeployed_FromDispatching(t *testing.T) {
	dep := model.NewDeployment(deployableCmd(), nil, time.Unix(10, 0))
	require.NoError(t, dep.ReserveForDispatch(time.Unix(20, 0)))

	require.NoError(t, dep.MarkDeployed(time.Unix(21, 0)))
	assert.Equal(t, model.StatusDeployed, dep.Status())
	assert.Equal(t, time.Unix(20, 0), *dep.Reservation().ReservedAt, "the slot stays held")
}

func TestWorkerTransitions_RejectedOnJobsDeployment(t *testing.T) {
	dep := model.NewDeployment(deployableCmd(), nil, time.Unix(10, 0))
	require.NoError(t, dep.MarkDeployed(time.Unix(20, 0)))

	assert.ErrorIs(t, dep.Complete(uuid.New(), sha256Hex("t"),
		model.WorkerResult{Succeeded: true}, time.Unix(30, 0)), model.ErrStaleLease)
	_, err := dep.AcknowledgeStart(uuid.New(), sha256Hex("t"), time.Unix(30, 0))
	assert.ErrorIs(t, err, model.ErrStaleLease)
	require.Error(t, dep.Heartbeat(time.Unix(30, 0), time.Unix(90, 0)))
}
