package model_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	dep, token, leaseID := claimed(t)

	require.NoError(t, dep.Heartbeat(leaseID, sha256Hex(token), time.Unix(60, 0), time.Unix(120, 0)))
	assert.Equal(t, time.Unix(120, 0), dep.ActiveLease().ExpiresAt)
	assert.Equal(t, time.Unix(60, 0), dep.ActiveLease().HeartbeatAt)
	assert.Equal(t, model.StatusLeased, dep.Status(), "a heartbeat is not a state change")
}

func TestHeartbeat_AllowedWhileRunning(t *testing.T) {
	dep, token, leaseID := claimed(t)
	_, err := dep.AcknowledgeStart(leaseID, sha256Hex(token), time.Unix(21, 0))
	require.NoError(t, err)

	require.NoError(t, dep.Heartbeat(leaseID, sha256Hex(token), time.Unix(60, 0), time.Unix(120, 0)))
	assert.Equal(t, model.StatusRunning, dep.Status())
}

func TestHeartbeat_RejectedOnTerminalDeployment(t *testing.T) {
	dep, token, leaseID := claimed(t)
	require.NoError(t, dep.Complete(leaseID, sha256Hex(token),
		model.WorkerResult{Succeeded: true}, time.Unix(30, 0)))

	require.Error(t, dep.Heartbeat(leaseID, sha256Hex(token), time.Unix(60, 0), time.Unix(120, 0)),
		"a finished task's lease is not extended")
}

func TestHeartbeat_NeverShortensExpiry(t *testing.T) {
	dep, token, leaseID := claimed(t)

	require.NoError(t, dep.Heartbeat(leaseID, sha256Hex(token), time.Unix(60, 0), time.Unix(120, 0)))
	require.Error(t, dep.Heartbeat(leaseID, sha256Hex(token), time.Unix(61, 0), time.Unix(90, 0)),
		"an out-of-order heartbeat must not pull the expiry back in")
	assert.Equal(t, time.Unix(120, 0), dep.ActiveLease().ExpiresAt)
}

func TestHeartbeat_FencesStaleLease(t *testing.T) {
	dep, token, leaseID := claimed(t)

	assert.ErrorIs(t, dep.Heartbeat(uuid.New(), sha256Hex(token), time.Unix(60, 0), time.Unix(120, 0)),
		model.ErrStaleLease, "a different lease ID is stale")
	assert.ErrorIs(t, dep.Heartbeat(leaseID, sha256Hex("stale"), time.Unix(60, 0), time.Unix(120, 0)),
		model.ErrStaleLease, "a wrong token is stale")
	assert.Equal(t, time.Unix(80, 0), dep.ActiveLease().ExpiresAt,
		"a superseded worker cannot extend the holder's lease")
	assert.Equal(t, time.Unix(20, 0), dep.ActiveLease().HeartbeatAt)
}

func TestHeartbeat_RequiresLease(t *testing.T) {
	dep := model.NewWorkerDeployment(deployableCmd(), uuid.Nil, poolFixture(), time.Unix(10, 0))

	assert.ErrorIs(t, dep.Heartbeat(uuid.New(), sha256Hex("t"), time.Unix(60, 0), time.Unix(120, 0)),
		model.ErrStaleLease, "an unclaimed task has no lease to extend")
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

// TestWorkerResult_KeepsACacheVerdictUnderTheKeyAWorkerWritesIt pins the name
// the wrapper's cache verdict travels and rests under. The report is decoded
// from a worker's JSON and stored as the JSON of this struct, so the tag is the
// whole contract: rename it and a worker still reports, the executor still
// accepts, and the verdict is read back as nothing.
func TestWorkerResult_KeepsACacheVerdictUnderTheKeyAWorkerWritesIt(t *testing.T) {
	reported := []byte(`{"succeeded":true,"cache_status":"accepted"}`)

	var decoded model.WorkerResult
	require.NoError(t, json.Unmarshal(reported, &decoded))
	require.Equal(t, "accepted", decoded.CacheStatus)

	stored, err := json.Marshal(decoded)
	require.NoError(t, err)
	assert.Contains(t, string(stored), `"cache_status":"accepted"`)

	var read model.WorkerResult
	require.NoError(t, json.Unmarshal(stored, &read))
	assert.Equal(t, decoded, read)
}

// TestComplete_RecordsTheCacheVerdictItWasReported reads the verdict back off
// the aggregate that stores it, which is where an audit of what a wrapper did
// with the promoted cache reads it from.
func TestComplete_RecordsTheCacheVerdictItWasReported(t *testing.T) {
	dep, token, leaseID := claimed(t)
	result := model.WorkerResult{Succeeded: true, CacheStatus: "accepted"}

	require.NoError(t, dep.Complete(leaseID, sha256Hex(token), result, time.Unix(30, 0)))

	require.NotNil(t, dep.TerminalResult())
	assert.Equal(t, "accepted", dep.TerminalResult().CacheStatus)
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
	assert.ErrorIs(t, dep.Heartbeat(uuid.New(), sha256Hex("t"), time.Unix(30, 0), time.Unix(90, 0)),
		model.ErrStaleLease)
}

// TestClaim_CountsAttemptsAcrossRequeue pins that the attempt counter survives
// the requeue that drops the lease, so a re-claimed task's lease reports the
// higher attempt. Because the counter lives on the Deployment and not the Lease,
// the second claim reads it back even though no lease bridges the two attempts.
func TestClaim_CountsAttemptsAcrossRequeue(t *testing.T) {
	dep, _, _ := claimed(t)
	assert.Equal(t, 1, dep.Attempt(), "the first claim is attempt 1")
	assert.Equal(t, 1, dep.ActiveLease().Attempt, "the lease projects the counter")

	require.NoError(t, dep.MarkRetryPending(time.Unix(90, 0), 30*time.Second))
	require.Nil(t, dep.ActiveLease(), "the requeue drops the lease")

	// The state the parked task is served from once its backoff elapses: pending
	// again, holding the attempts it already spent and no lease.
	requeued := model.Reconstitute(model.ReconstituteInput{
		ID: dep.ID(), Mode: model.ModeProduction, Command: deployableCmd(),
		Status: model.StatusPending, RetryCount: dep.RetryCount(), MaxRetries: dep.MaxRetries(),
		NextAttemptAt: dep.NextAttemptAt(), CreatedAt: time.Unix(10, 0),
		ExecutionMode: model.ExecutionModeWorkers, PoolKey: poolFixture(),
		Reservation: dep.Reservation(), Attempt: dep.Attempt(),
	})

	token := strings.Repeat("2", 64)
	require.NoError(t, requeued.Claim(uuid.New(), sha256Hex(token), "worker-2", "pod-b", "uid-b",
		time.Unix(130, 0), time.Unix(190, 0), argvFixture(), model.ExecutionPathNative))

	assert.Equal(t, 2, requeued.Attempt(), "the re-claim is attempt 2")
	assert.Equal(t, 2, requeued.ActiveLease().Attempt)
}

func TestReportFailure_RetryableParksForRequeue(t *testing.T) {
	dep, token, leaseID := claimed(t)
	result := model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "connection reset"}

	require.NoError(t, dep.ReportFailure(leaseID, sha256Hex(token), result, true,
		time.Unix(90, 0), 30*time.Second))

	assert.Equal(t, model.StatusRetryPending, dep.Status())
	assert.Equal(t, time.Unix(120, 0), dep.NextAttemptAt())
	assert.Equal(t, time.Unix(90, 0), *dep.SlotReleasedAt())
	assert.Nil(t, dep.ActiveLease())
}

func TestReportFailure_PermanentFailsAndRecordsResult(t *testing.T) {
	dep, token, leaseID := claimed(t)
	result := model.WorkerResult{
		Succeeded:    false,
		ErrorClass:   "dbt_unique_id_not_found",
		ErrorMessage: "node orders not in manifest",
	}

	require.NoError(t, dep.ReportFailure(leaseID, sha256Hex(token), result, false,
		time.Unix(90, 0), 30*time.Second))

	assert.Equal(t, model.StatusFailed, dep.Status())
	assert.Equal(t, "node orders not in manifest", *dep.ErrorMessage())
	assert.Equal(t, time.Unix(90, 0), *dep.SlotReleasedAt())
	require.NotNil(t, dep.TerminalResult())
	assert.Equal(t, result, *dep.TerminalResult())
}

// TestReportFailure_PermanentDecisionOverridesWorkerHint pins that the caller's
// decision alone selects the transition, and that the worker's disagreeing hint
// survives verbatim in the stored report. The executor fails the final allowed
// attempt permanently however transient the worker judged the failure to be.
func TestReportFailure_PermanentDecisionOverridesWorkerHint(t *testing.T) {
	dep, token, leaseID := claimed(t)
	result := model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "connection reset"}

	require.NoError(t, dep.ReportFailure(leaseID, sha256Hex(token), result, false,
		time.Unix(90, 0), 30*time.Second))

	assert.Equal(t, model.StatusFailed, dep.Status())
	require.NotNil(t, dep.TerminalResult())
	assert.True(t, dep.TerminalResult().Retryable,
		"the worker's hint is recorded as observed, not rewritten to match the transition")
}

// TestReportFailure_PermanentAbsorbsRedelivery pins that a worker's terminal
// report reaches the executor at least once, so the same permanent failure can
// arrive twice. The second one records the result already recorded rather than
// erroring, which is the acknowledgement the worker retried the report to get.
func TestReportFailure_PermanentAbsorbsRedelivery(t *testing.T) {
	dep, token, leaseID := claimed(t)
	result := model.WorkerResult{
		Succeeded: false, ErrorClass: "dbt_compilation_error", ErrorMessage: "syntax error",
	}
	require.NoError(t, dep.ReportFailure(leaseID, sha256Hex(token), result, false,
		time.Unix(90, 0), 30*time.Second))
	releasedAt := *dep.SlotReleasedAt()

	// The worker never saw the first report acknowledged and sends it again.
	require.NoError(t, dep.ReportFailure(leaseID, sha256Hex(token), result, false,
		time.Unix(95, 0), 30*time.Second))

	assert.Equal(t, model.StatusFailed, dep.Status())
	assert.Equal(t, result, *dep.TerminalResult(), "the recorded result is unchanged")
	assert.Equal(t, releasedAt, *dep.SlotReleasedAt(), "the slot is released once, at the first report")
	assert.Equal(t, time.Unix(90, 0), *dep.ActiveLease().FinishedAt,
		"the redelivery does not move the time the execution finished")
}

// TestReportFailure_RejectsAConflictingSecondResult pins that redelivery
// tolerance does not extend to rewriting a settled outcome: a second, different
// result on a finished lease is a contradiction, not a retry.
func TestReportFailure_RejectsAConflictingSecondResult(t *testing.T) {
	dep, token, leaseID := claimed(t)
	first := model.WorkerResult{Succeeded: false, ErrorMessage: "syntax error"}
	require.NoError(t, dep.ReportFailure(leaseID, sha256Hex(token), first, false,
		time.Unix(90, 0), 30*time.Second))

	second := model.WorkerResult{Succeeded: false, ErrorMessage: "a different failure"}
	err := dep.ReportFailure(leaseID, sha256Hex(token), second, false,
		time.Unix(95, 0), 30*time.Second)

	require.Error(t, err)
	assert.NotErrorIs(t, err, model.ErrStaleLease)
	assert.Equal(t, first, *dep.TerminalResult(), "the first result stands")
}

// TestReportFailure_RetryableRedeliveryIsFenced pins the other half of the
// at-least-once contract. Parking a task for requeue drops its lease, so a
// redelivered retryable report arrives on a task carrying no lease that may
// already have been re-claimed. The executor cannot tell it apart from a
// superseded worker's report, so it fences it rather than guess.
func TestReportFailure_RetryableRedeliveryIsFenced(t *testing.T) {
	dep, token, leaseID := claimed(t)
	result := model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "connection reset"}
	require.NoError(t, dep.ReportFailure(leaseID, sha256Hex(token), result, true,
		time.Unix(90, 0), 30*time.Second))
	require.Nil(t, dep.ActiveLease(), "parking the task dropped the lease that reported")

	err := dep.ReportFailure(leaseID, sha256Hex(token), result, true,
		time.Unix(95, 0), 30*time.Second)

	assert.ErrorIs(t, err, model.ErrStaleLease)
	assert.Equal(t, model.StatusRetryPending, dep.Status(), "the parked task is undisturbed")
	assert.Equal(t, time.Unix(120, 0), dep.NextAttemptAt(), "and its backoff is not extended")
}

// TestReportFailure_FencesStaleLease pins the boundary a superseded worker must
// not cross: reporting a failure on a task that was reaped and re-claimed must
// not drop the new holder's lease or free the slot it still occupies.
func TestReportFailure_FencesStaleLease(t *testing.T) {
	dep, holderToken, holderLeaseID := claimed(t)

	// The report a worker whose lease was reaped and reassigned sends in.
	stale := model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "stale report"}
	assert.ErrorIs(t, dep.ReportFailure(uuid.New(), sha256Hex(holderToken), stale, true,
		time.Unix(140, 0), 30*time.Second), model.ErrStaleLease, "a different lease ID is stale")
	assert.ErrorIs(t, dep.ReportFailure(holderLeaseID, sha256Hex("stale"), stale, false,
		time.Unix(140, 0), 30*time.Second), model.ErrStaleLease, "a wrong token is stale")

	assert.Equal(t, model.StatusLeased, dep.Status(), "a fenced report drives no transition")
	require.NotNil(t, dep.ActiveLease(), "the current holder keeps its lease")
	assert.Equal(t, holderLeaseID, dep.ActiveLease().ID)
	assert.Nil(t, dep.SlotReleasedAt(), "the holder still occupies its slot")
}

func TestReportFailure_RequiresFailedResult(t *testing.T) {
	dep, token, leaseID := claimed(t)

	err := dep.ReportFailure(leaseID, sha256Hex(token), model.WorkerResult{Succeeded: true}, false,
		time.Unix(90, 0), 30*time.Second)
	require.Error(t, err, "a succeeded result travels through Complete")
	assert.NotErrorIs(t, err, model.ErrStaleLease)
	assert.Equal(t, model.StatusLeased, dep.Status())
}

// TestRequeue_KeepsUnelapsedBackoff pins that a requeue cannot cancel the delay a
// retryable failure recorded. retry.task:v1 is consumed within moments of the
// failure that emitted it, so a requeue that reset the deadline to now would make
// the backoff dead code and let a transient failure re-claim in a hot loop.
func TestRequeue_KeepsUnelapsedBackoff(t *testing.T) {
	dep, token, leaseID := claimed(t)
	result := model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "connection reset"}
	require.NoError(t, dep.ReportFailure(leaseID, sha256Hex(token), result, true,
		time.Unix(90, 0), 30*time.Second))
	require.Equal(t, time.Unix(120, 0), dep.NextAttemptAt(), "the failure parked the task until 120")

	// The retry message lands at 91 — a second after the failure, and 29 seconds
	// before the task is due.
	require.NoError(t, dep.Requeue(1, "dbt-public-orders-r1", time.Unix(91, 0)))

	assert.Equal(t, model.StatusPending, dep.Status())
	assert.Equal(t, time.Unix(120, 0), dep.NextAttemptAt(),
		"the recorded backoff still has to elapse before the task is claimable")
}

// TestRequeue_ServesAnElapsedBackoffImmediately pins the other half: once the
// recorded backoff has passed, a requeue leaves the task due now rather than
// stranding it behind a deadline in the past.
func TestRequeue_ServesAnElapsedBackoffImmediately(t *testing.T) {
	dep, token, leaseID := claimed(t)
	result := model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "connection reset"}
	require.NoError(t, dep.ReportFailure(leaseID, sha256Hex(token), result, true,
		time.Unix(90, 0), 30*time.Second))

	// The retry message lands at 200, long after the backoff elapsed at 120.
	require.NoError(t, dep.Requeue(1, "dbt-public-orders-r1", time.Unix(200, 0)))

	assert.Equal(t, model.StatusPending, dep.Status())
	assert.Equal(t, time.Unix(200, 0), dep.NextAttemptAt(), "an elapsed backoff makes the task due now")
}

// TestExpireLease_FencesTheWorkerThatHeldIt pins the fence a reaped lease must
// leave behind. Failing a task permanently keeps its lease, because a worker's
// own report is finished on the lease that sent it and a redelivery must be
// absorbed. An expiry has no such report: the worker is unreachable and may yet
// send one, so the lease has to go — otherwise that late report authorizes, is
// rejected only by the status check, and the worker is answered a fault of the
// executor's own rather than told its lease is gone.
func TestExpireLease_FencesTheWorkerThatHeldIt(t *testing.T) {
	dep, token, leaseID := claimed(t)

	require.NoError(t, dep.ExpireLease(leaseID))

	assert.Nil(t, dep.ActiveLease(), "the expired lease is dropped")
	assert.ErrorIs(t, dep.Complete(leaseID, sha256Hex(token),
		model.WorkerResult{Succeeded: true}, time.Unix(90, 0)), model.ErrStaleLease,
		"the fenced worker's late report drives nothing")
	assert.ErrorIs(t, dep.Heartbeat(leaseID, sha256Hex(token),
		time.Unix(90, 0), time.Unix(150, 0)), model.ErrStaleLease,
		"the fenced worker cannot keep its lease alive")
}

// TestExpireLease_LeavesTheSlotToTheTransition pins that the fence alone settles
// nothing: it drops the lease and no more, so the caller's transition remains the
// one thing that releases the execution slot.
func TestExpireLease_LeavesTheSlotToTheTransition(t *testing.T) {
	dep, _, leaseID := claimed(t)

	require.NoError(t, dep.ExpireLease(leaseID))

	assert.Equal(t, model.StatusLeased, dep.Status(), "the fence decides no outcome")
	assert.Nil(t, dep.SlotReleasedAt(), "the slot is still held until a transition releases it")
}

// TestExpireLease_RejectsALeaseTheTaskNoLongerHolds pins that a reaper acting on
// a lease it read before another transaction replaced it cannot drop the lease
// the task now holds, which would fence a live worker mid-run.
func TestExpireLease_RejectsALeaseTheTaskNoLongerHolds(t *testing.T) {
	dep, _, _ := claimed(t)

	assert.ErrorIs(t, dep.ExpireLease(uuid.New()), model.ErrStaleLease)
	require.NotNil(t, dep.ActiveLease(), "the current lease survives")
	assert.Equal(t, "worker-1", dep.ActiveLease().Owner)
}

// TestExpireLease_RejectsATaskWithNoLease pins that expiring a lease that is not
// there is an error rather than a silent no-op: the caller read a lease to expire,
// so finding none means it is acting on state it no longer knows.
func TestExpireLease_RejectsATaskWithNoLease(t *testing.T) {
	dep := model.NewWorkerDeployment(deployableCmd(), uuid.Nil, poolFixture(), time.Unix(10, 0))

	assert.ErrorIs(t, dep.ExpireLease(uuid.New()), model.ErrStaleLease)
}
