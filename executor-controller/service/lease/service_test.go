package lease_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/lease"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	poolKey    = "finance:sha-abc"
	leaseTTL   = 60 * time.Second
	retryDelay = 30 * time.Second
)

// harness wires the lease service to in-memory repositories.
type harness struct {
	svc      *lease.Service
	repo     *fakeDeployments
	outbox   *recordingOutbox
	commands *fakeCommands
	clock    *fakeClock
}

func newHarness(t *testing.T, maxConcurrent int) *harness {
	t.Helper()
	h := &harness{
		repo:     newFakeDeployments(),
		outbox:   &recordingOutbox{},
		commands: &fakeCommands{argv: []string{"dbt", "run", "--select", "orders"}},
		clock:    &fakeClock{now: time.Unix(1000, 0).UTC()},
	}
	h.svc = lease.NewService(lease.Config{
		UnitOfWork: func() uow.UnitOfWork {
			return &uow.FakeUnitOfWork{Outbox: h.outbox, Deployments: h.repo}
		},
		Commands:                h.commands,
		Clock:                   h.clock,
		MaxConcurrentExecutions: maxConcurrent,
		LeaseTTL:                leaseTTL,
		RetryBackoff:            retryDelay,
		Logger:                  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return h
}

func workerCmd() command.DeployTask {
	return command.DeployTask{
		TaskID: uuid.New().String(), ScheduleID: uuid.New().String(), ScheduleName: "daily",
		ServiceName: "finance", SchemaName: "public", TableName: "orders",
		JobName: "dbt-public-orders", NodeType: "dbt-model", ImageTag: "sha-abc",
		TaskRetryCount: 0, TaskMaxRetries: 3,
	}
}

// seedDue puts one due worker task in the pool.
func (h *harness) seedDue(cmd command.DeployTask) *model.Deployment {
	dep := model.NewWorkerDeployment(cmd, uuid.New(), poolKey, h.clock.now)
	h.repo.add(dep)
	return dep
}

func claimInput() lease.ClaimInput {
	return lease.ClaimInput{PoolKey: poolKey, Owner: "worker-1", PodName: "pod-a", PodUID: "uid-a"}
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// mustClaim claims the one due task and fails the test if none was granted.
func (h *harness) mustClaim(t *testing.T) *lease.Grant {
	t.Helper()
	grant, err := h.svc.Claim(context.Background(), claimInput())
	require.NoError(t, err)
	require.NotNil(t, grant, "a due task in a pool with capacity must be granted")
	return grant
}

// --- claim -----------------------------------------------------------------

func TestClaim_GrantsDueTaskAndPinsItsCommand(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())

	grant := h.mustClaim(t)

	assert.Equal(t, dep.ID(), grant.DeploymentID)
	assert.Equal(t, 1, grant.Attempt, "the first claim is attempt 1")
	assert.Equal(t, []string{"dbt", "run", "--select", "orders"}, grant.Argv)
	assert.Equal(t, model.ExecutionPathNative, grant.ExecutionPath)
	assert.Equal(t, h.clock.now.Add(leaseTTL), grant.ExpiresAt)
	assert.Equal(t, "orders", grant.Command.TableName)
	assert.Equal(t, model.StatusLeased, h.repo.statusOf(dep.ID()))
	reservation := h.repo.rows[dep.ID()].dep.Reservation()
	assert.Equal(t, h.clock.now, *reservation.ReservedAt, "the claim takes an execution slot")
	assert.Nil(t, reservation.ReleasedAt, "and holds it while the worker runs")
	assert.Empty(t, h.outbox.entries, "a claim announces nothing until the worker starts")
}

// TestClaim_ReturnsTokenOnlyToItsHolder pins that the raw token is handed to the
// claiming worker and never stored: only its digest reaches the row, so a
// database read cannot recover a token that would let its reader fence the
// holder.
func TestClaim_ReturnsTokenOnlyToItsHolder(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())

	grant := h.mustClaim(t)

	require.NotEmpty(t, grant.Token)
	stored := h.repo.rows[dep.ID()].dep.ActiveLease()
	require.NotNil(t, stored)
	assert.Equal(t, sha256Hex(grant.Token), stored.TokenSHA256, "the row stores the digest")
	assert.NotEqual(t, grant.Token, stored.TokenSHA256, "the raw token is never stored")
	assert.Equal(t, grant.LeaseID, stored.ID)
}

func TestClaim_TokensAreUnique(t *testing.T) {
	h := newHarness(t, 4)
	h.seedDue(workerCmd())
	h.seedDue(workerCmd())

	first := h.mustClaim(t)
	second := h.mustClaim(t)

	assert.NotEqual(t, first.Token, second.Token)
	assert.NotEqual(t, first.LeaseID, second.LeaseID)
}

func TestClaim_NoDueWorkReturnsNoGrant(t *testing.T) {
	h := newHarness(t, 4)

	grant, err := h.svc.Claim(context.Background(), claimInput())

	require.NoError(t, err)
	assert.Nil(t, grant, "an empty pool grants nothing rather than erroring")
}

// TestClaim_ExhaustedCapacityGrantsNothing pins that a worker cannot claim past
// the executor's shared budget: Kubernetes Jobs and worker tasks draw on the same
// slots, so a full budget must leave due work waiting.
func TestClaim_ExhaustedCapacityGrantsNothing(t *testing.T) {
	h := newHarness(t, 2)
	dep := h.seedDue(workerCmd())
	h.repo.activeSlot = 2

	grant, err := h.svc.Claim(context.Background(), claimInput())

	require.NoError(t, err)
	assert.Nil(t, grant)
	assert.Equal(t, model.StatusPending, h.repo.statusOf(dep.ID()), "the task stays due for later")
	assert.Equal(t, 0, h.repo.saves, "a refused claim writes nothing")
}

// TestClaim_CountsCapacityUnderTheCapacityLock pins that a claim serializes
// against every other transaction that reserves a slot. Without the lock two
// concurrent claims read the same free slot and both take it.
func TestClaim_CountsCapacityUnderTheCapacityLock(t *testing.T) {
	h := newHarness(t, 4)
	h.seedDue(workerCmd())
	h.repo.lockCapacityErr = assert.AnError

	_, err := h.svc.Claim(context.Background(), claimInput())

	require.ErrorIs(t, err, assert.AnError, "a claim that cannot take the capacity lock must not proceed")
	assert.Equal(t, 0, h.repo.saves)
}

// TestClaim_RetryUsesPersistedCommand pins the write-once command across
// attempts. A task's row records what it was actually attempted with, so a
// configuration reload between attempts must not change the command of a task
// already in flight — the retry runs what the row holds, and the resolver is
// never consulted again.
func TestClaim_RetryUsesPersistedCommand(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())

	first := h.mustClaim(t)
	require.Equal(t, 1, h.commands.calls, "the first attempt resolves the command once")
	require.Equal(t, []string{"dbt", "run", "--select", "orders"}, first.Argv)

	// The task fails retryably and is requeued for its next attempt.
	require.NoError(t, h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: first.LeaseID, Token: first.Token,
		Result: model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "connection reset"},
	}))
	requeue(t, h, dep.ID())

	// A configuration reload changes what the resolver would return.
	h.commands.argv = []string{"custom-wrapper", "run", "--select", "orders"}

	second := h.mustClaim(t)

	assert.Equal(t, []string{"dbt", "run", "--select", "orders"}, second.Argv,
		"the retry runs the command the first attempt pinned, not a freshly resolved one")
	assert.Equal(t, model.ExecutionPathNative, second.ExecutionPath,
		"the pinned path survives with the argv it belongs to")
	assert.Equal(t, 1, h.commands.calls, "an already-resolved task never re-resolves")
	assert.Equal(t, []string{"dbt", "run", "--select", "orders"}, h.repo.storedArgv(dep.ID()),
		"the row still holds what it was first attempted with")
	assert.Equal(t, 2, second.Attempt, "the attempt counter survives the requeue")
}

// requeue returns a parked task to the pending queue the way the retry.task:v1
// handler does, so a test can drive a task through consecutive attempts.
func requeue(t *testing.T, h *harness, id uuid.UUID) {
	t.Helper()
	row := h.repo.rows[id]
	require.Equal(t, model.StatusRetryPending, row.dep.Status())
	// The parked task serves out its backoff before its next claim.
	h.clock.now = h.clock.now.Add(retryDelay + time.Second)
	require.NoError(t, row.dep.Requeue(1, "dbt-public-orders-r1", h.clock.now))
}

// --- start -----------------------------------------------------------------

func TestStart_AnnouncesRunningOnce(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	in := lease.StartInput{PoolKey: poolKey, DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token}
	require.NoError(t, h.svc.Start(context.Background(), in))
	assert.Equal(t, model.StatusRunning, h.repo.statusOf(dep.ID()))

	// The worker's start report is redelivered — it retried a timed-out request.
	require.NoError(t, h.svc.Start(context.Background(), in))

	assert.Equal(t, 1, h.outbox.countByStream(streams.TaskStatusUpdatedV1),
		"a duplicate start announces RUNNING once")
	var status pkgevents.TaskStatusUpdated
	require.NoError(t, json.Unmarshal(h.outbox.byStream(streams.TaskStatusUpdatedV1).Payload, &status))
	assert.Equal(t, "RUNNING", status.Status)
}

func TestStart_FencesStaleToken(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	err := h.svc.Start(context.Background(), lease.StartInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: "not-the-token",
	})

	require.ErrorIs(t, err, model.ErrStaleLease)
	assert.Equal(t, model.StatusLeased, h.repo.statusOf(dep.ID()), "a fenced start drives no transition")
	assert.Empty(t, h.outbox.entries, "a fenced start announces nothing")
}

// --- heartbeat -------------------------------------------------------------

func TestHeartbeat_ExtendsTheLease(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)
	h.clock.now = h.clock.now.Add(20 * time.Second)

	require.NoError(t, h.svc.Heartbeat(context.Background(), lease.HeartbeatInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
	}))

	stored := h.repo.rows[dep.ID()].dep.ActiveLease()
	assert.Equal(t, h.clock.now.Add(leaseTTL), stored.ExpiresAt, "the deadline moves out by a full TTL")
	assert.Equal(t, h.clock.now, stored.HeartbeatAt)
	assert.Empty(t, h.outbox.entries, "a heartbeat announces nothing")
}

func TestHeartbeat_FencesStaleToken(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)
	before := h.repo.rows[dep.ID()].dep.ActiveLease().ExpiresAt

	err := h.svc.Heartbeat(context.Background(), lease.HeartbeatInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: uuid.New(), Token: grant.Token,
	})

	require.ErrorIs(t, err, model.ErrStaleLease)
	assert.Equal(t, before, h.repo.rows[dep.ID()].dep.ActiveLease().ExpiresAt,
		"a superseded worker cannot keep a reassigned task's lease alive")
}

// --- completion ------------------------------------------------------------

func TestComplete_SuccessSettlesTaskOnce(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)
	require.NoError(t, h.svc.Start(context.Background(), lease.StartInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
	}))
	h.outbox.entries = nil

	in := lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{
			Succeeded: true, ExecutionSeconds: 12.5,
			LogS3URI: "s3://continuo/dbt-runs/daily/task-1/lease-1/dbt.log",
		},
	}
	require.NoError(t, h.svc.Complete(context.Background(), in))

	// The worker's terminal report is redelivered — it retried a timed-out request.
	require.NoError(t, h.svc.Complete(context.Background(), in))

	assert.Equal(t, model.StatusSucceeded, h.repo.statusOf(dep.ID()))
	assert.Equal(t, []string{
		streams.TaskStatusUpdatedV1, streams.TaskExecutionRecordedV1, streams.NodeUpdatedV1,
	}, h.outbox.streamNames(), "a duplicate success settles the task exactly once")

	var exec pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(h.outbox.byStream(streams.TaskExecutionRecordedV1).Payload, &exec))
	assert.Equal(t, grant.LeaseID.String(), exec.ExecutionID)
	assert.Equal(t, "dbt-runs/daily/task-1/lease-1/dbt.log", exec.LogS3Key)
}

// TestComplete_SuccessWithFailedUploadIsNotRerun pins that a warehouse result is
// never rerun to recover artifacts: the dbt work already happened, so a success
// whose log upload failed settles SUCCEEDED with no log key rather than retrying.
func TestComplete_SuccessWithFailedUploadIsNotRerun(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	require.NoError(t, h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{Succeeded: true, ExecutionSeconds: 12.5, UploadSeconds: 0},
	}))

	assert.Equal(t, model.StatusSucceeded, h.repo.statusOf(dep.ID()))
	assert.Equal(t, 0, h.outbox.countByStream(streams.RetryTaskV1), "a succeeded model is never re-materialized")

	var exec pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(h.outbox.byStream(streams.TaskExecutionRecordedV1).Payload, &exec))
	assert.Empty(t, exec.LogS3Key)
}

func TestComplete_FencesStaleToken(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	err := h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: "not-the-token",
		Result: model.WorkerResult{Succeeded: true},
	})

	require.ErrorIs(t, err, model.ErrStaleLease)
	assert.Equal(t, model.StatusLeased, h.repo.statusOf(dep.ID()), "a fenced report drives no transition")
	assert.Empty(t, h.outbox.entries, "a fenced report announces nothing")
	assert.Nil(t, h.repo.rows[dep.ID()].dep.SlotReleasedAt(), "the holder still occupies its slot")
}

// TestComplete_StaleReportCannotDisplaceANewHolder pins the boundary a reaped
// worker must not cross. Its lease was expired and the task re-claimed; its late
// report must not drop the new holder's lease or free the slot that holder still
// occupies.
func TestComplete_StaleReportCannotDisplaceANewHolder(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	reaped := h.mustClaim(t)

	// The reaper expires the lease and the task is re-claimed by another worker.
	require.NoError(t, h.repo.rows[dep.ID()].dep.MarkRetryPending(h.clock.now, retryDelay))
	requeue(t, h, dep.ID())
	holder := h.mustClaim(t)
	require.NotEqual(t, reaped.LeaseID, holder.LeaseID)

	err := h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: reaped.LeaseID, Token: reaped.Token,
		Result: model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "stale report"},
	})

	require.ErrorIs(t, err, model.ErrStaleLease)
	assert.Equal(t, model.StatusLeased, h.repo.statusOf(dep.ID()))
	assert.Equal(t, holder.LeaseID, h.repo.rows[dep.ID()].dep.ActiveLease().ID, "the current holder keeps its lease")
	assert.Nil(t, h.repo.rows[dep.ID()].dep.SlotReleasedAt(), "the new holder still occupies its slot")
	assert.Empty(t, h.outbox.entries)
}

// --- failure classification ------------------------------------------------

// TestComplete_RetryableFailureParksAndAsksForRetry pins the retryable path: the
// attempt is announced and recorded, the task parks for requeue, its slot is
// released, and no node.updated settles the node it is about to re-run.
func TestComplete_RetryableFailureParksAndAsksForRetry(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	require.NoError(t, h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{
			Succeeded: false, Retryable: true,
			ErrorClass: "warehouse_unavailable", ErrorMessage: "connection reset",
		},
	}))

	row := h.repo.rows[dep.ID()].dep
	assert.Equal(t, model.StatusRetryPending, row.Status())
	assert.Equal(t, h.clock.now.Add(retryDelay), row.NextAttemptAt(), "the task serves a backoff before re-claiming")
	assert.Equal(t, h.clock.now, *row.SlotReleasedAt(), "parking releases the slot inside the transition")
	assert.Nil(t, row.ActiveLease(), "the parked task carries no lease")

	assert.Equal(t, []string{
		streams.TaskStatusUpdatedV1, streams.TaskExecutionRecordedV1, streams.RetryTaskV1,
	}, h.outbox.streamNames(), "the node has not settled, so it is not announced")

	var retry pkgevents.TaskRetry
	require.NoError(t, json.Unmarshal(h.outbox.byStream(streams.RetryTaskV1).Payload, &retry))
	assert.Equal(t, dep.ID().String(), retry.ExecutorDeploymentID,
		"the retry names the row to re-attempt, so the parked row is never stranded")
	assert.Equal(t, 1, retry.RetryCount)

	var exec pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(h.outbox.byStream(streams.TaskExecutionRecordedV1).Payload, &exec))
	assert.Equal(t, grant.LeaseID.String(), exec.ExecutionID,
		"the execution is identified by the lease that ran it, captured before parking dropped it")
}

// TestComplete_FinalAttemptIsPermanentDespiteWorkerHint pins that the executor,
// not the worker, owns the retry budget: the last allowed attempt fails
// permanently however transient the worker judged the failure to be.
func TestComplete_FinalAttemptIsPermanentDespiteWorkerHint(t *testing.T) {
	h := newHarness(t, 4)
	cmd := workerCmd()
	// TaskMaxRetries counts retries, so the budget is the initial attempt plus 3
	// more; TaskRetryCount 3 is that final allowed attempt.
	cmd.TaskRetryCount = 3
	cmd.TaskMaxRetries = 3
	dep := h.seedDue(cmd)
	grant := h.mustClaim(t)

	require.NoError(t, h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "connection reset"},
	}))

	assert.Equal(t, model.StatusFailed, h.repo.statusOf(dep.ID()))
	assert.Equal(t, []string{
		streams.TaskStatusUpdatedV1, streams.TaskExecutionRecordedV1, streams.NodeUpdatedV1,
	}, h.outbox.streamNames(), "an exhausted budget settles the node and asks for no retry")

	row := h.repo.rows[dep.ID()].dep
	require.NotNil(t, row.TerminalResult())
	assert.True(t, row.TerminalResult().Retryable,
		"the worker's hint is recorded as observed, not rewritten to match the decision")
}

// TestComplete_RetryBudgetMatchesTheJobsPath pins that a worker task settles
// after the same total number of attempts as the equivalent Kubernetes Job.
// TaskMaxRetries counts retries beyond the initial attempt, so a failed attempt
// whose zero-based index is below the budget parks for another try and the one
// at the budget fails permanently. For TaskMaxRetries=2 that is the initial
// attempt plus two retries — three in all — the same total the Jobs path reaches
// with retry_count < max_retries.
func TestComplete_RetryBudgetMatchesTheJobsPath(t *testing.T) {
	for _, tc := range []struct {
		name       string
		retryCount int
		wantStatus model.Status
	}{
		{"initial attempt parks for retry", 0, model.StatusRetryPending},
		{"first retry parks for retry", 1, model.StatusRetryPending},
		{"final retry fails permanently", 2, model.StatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, 4)
			cmd := workerCmd()
			cmd.TaskRetryCount = tc.retryCount
			cmd.TaskMaxRetries = 2
			dep := h.seedDue(cmd)
			grant := h.mustClaim(t)

			require.NoError(t, h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
				DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
				Result: model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "connection reset"},
			}))
			assert.Equal(t, tc.wantStatus, h.repo.statusOf(dep.ID()))
		})
	}
}

// TestComplete_UnfixableErrorClassesAreNeverRetried pins the classes no rerun can
// fix. Retrying them burns the budget and delays the failure the operator needs
// to see, so the worker's retryable hint is overruled.
func TestComplete_UnfixableErrorClassesAreNeverRetried(t *testing.T) {
	for _, class := range []string{
		"runtime_manifest_rejected",
		"runtime_manifest_unverified",
		"dbt_unique_id_not_found",
		"dbt_selector_not_unique",
	} {
		t.Run(class, func(t *testing.T) {
			h := newHarness(t, 4)
			dep := h.seedDue(workerCmd()) // retry budget is untouched: 0 of 3
			grant := h.mustClaim(t)

			require.NoError(t, h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
				DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
				Result: model.WorkerResult{
					Succeeded: false, Retryable: true,
					ErrorClass: class, ErrorMessage: "unfixable by rerunning",
				},
			}))

			assert.Equal(t, model.StatusFailed, h.repo.statusOf(dep.ID()))
			assert.Equal(t, 0, h.outbox.countByStream(streams.RetryTaskV1),
				"no rerun fixes this class, so the budget is not spent on one")
			assert.Equal(t, 1, h.outbox.countByStream(streams.NodeUpdatedV1),
				"the node settles so the schedule advances")
		})
	}
}

// TestComplete_WorkerHintFalseIsPermanent pins that a worker that says a failure
// is not transient is believed, even with budget left.
func TestComplete_WorkerHintFalseIsPermanent(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	require.NoError(t, h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{Succeeded: false, Retryable: false, ErrorMessage: "compilation error"},
	}))

	assert.Equal(t, model.StatusFailed, h.repo.statusOf(dep.ID()))
	assert.Equal(t, 0, h.outbox.countByStream(streams.RetryTaskV1))
}

// TestComplete_DuplicatePermanentFailureSettlesTaskOnce pins that a worker's
// terminal report reaches the executor at least once, so the same permanent
// failure can arrive twice. The second one is acknowledged and announces
// nothing: a second fan-out would tell the run the node failed twice.
func TestComplete_DuplicatePermanentFailureSettlesTaskOnce(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	in := lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{
			Succeeded: false, Retryable: false,
			ErrorClass: "dbt_compilation_error", ErrorMessage: "syntax error",
		},
	}
	require.NoError(t, h.svc.Complete(context.Background(), in))
	releasedAt := *h.repo.rows[dep.ID()].dep.SlotReleasedAt()

	// The worker's terminal report is redelivered — it retried a timed-out request.
	h.clock.now = h.clock.now.Add(5 * time.Second)
	require.NoError(t, h.svc.Complete(context.Background(), in),
		"a redelivered permanent failure is acknowledged, not rejected")

	assert.Equal(t, model.StatusFailed, h.repo.statusOf(dep.ID()))
	assert.Equal(t, []string{
		streams.TaskStatusUpdatedV1, streams.TaskExecutionRecordedV1, streams.NodeUpdatedV1,
	}, h.outbox.streamNames(), "a duplicate permanent failure settles the task exactly once")
	assert.Equal(t, releasedAt, *h.repo.rows[dep.ID()].dep.SlotReleasedAt(),
		"the slot is released once, at the first report")
}

// TestComplete_DuplicateRetryableFailureIsFenced pins the semantics a worker
// sees when it redelivers a retryable failure. Parking the task for requeue
// dropped the lease that reported, and the task may already have been re-claimed
// at a higher attempt, so the executor cannot tell the redelivery apart from a
// superseded worker's report: it fences both. The report was already recorded
// and its retry already asked for, so the fenced redelivery announces nothing.
func TestComplete_DuplicateRetryableFailureIsFenced(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	in := lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{
			Succeeded: false, Retryable: true,
			ErrorClass: "warehouse_unavailable", ErrorMessage: "connection reset",
		},
	}
	require.NoError(t, h.svc.Complete(context.Background(), in))
	require.Equal(t, 1, h.outbox.countByStream(streams.RetryTaskV1))

	// The worker's terminal report is redelivered — it retried a timed-out request.
	h.clock.now = h.clock.now.Add(5 * time.Second)
	err := h.svc.Complete(context.Background(), in)

	require.ErrorIs(t, err, model.ErrStaleLease,
		"the lease the report names is gone, so the report is fenced rather than re-applied")
	row := h.repo.rows[dep.ID()].dep
	assert.Equal(t, model.StatusRetryPending, row.Status(), "the parked task is undisturbed")
	assert.Equal(t, 1, h.outbox.countByStream(streams.RetryTaskV1),
		"a duplicate retryable failure asks for one retry, not two")
	assert.Equal(t, []string{
		streams.TaskStatusUpdatedV1, streams.TaskExecutionRecordedV1, streams.RetryTaskV1,
	}, h.outbox.streamNames(), "the redelivery announces nothing")
}

// TestComplete_RecordsTheExecutionsTiming pins that a worker execution reports
// when it ran, as the Jobs path does: without these the run's read model shows a
// worker-run node with no start or finish.
func TestComplete_RecordsTheExecutionsTiming(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	h.clock.now = h.clock.now.Add(2 * time.Second)
	startedAt := h.clock.now
	require.NoError(t, h.svc.Start(context.Background(), lease.StartInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
	}))

	h.clock.now = h.clock.now.Add(30 * time.Second)
	completedAt := h.clock.now
	require.NoError(t, h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{Succeeded: true, ExecutionSeconds: 30},
	}))

	var exec pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(h.outbox.byStream(streams.TaskExecutionRecordedV1).Payload, &exec))
	assert.Equal(t, startedAt.Format(time.RFC3339), exec.StartedAt,
		"the execution starts when the worker reported its dbt process running")
	assert.Equal(t, completedAt.Format(time.RFC3339), exec.CompletedAt)
}

// TestComplete_RetryableFailureRecordsTimingCapturedBeforeParking pins that the
// timing survives the transition that drops the lease holding it: parking the
// task for requeue nils the lease, so a fan-out reading it afterwards would find
// no start to report.
func TestComplete_RetryableFailureRecordsTimingCapturedBeforeParking(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)

	h.clock.now = h.clock.now.Add(2 * time.Second)
	startedAt := h.clock.now
	require.NoError(t, h.svc.Start(context.Background(), lease.StartInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
	}))

	h.clock.now = h.clock.now.Add(10 * time.Second)
	require.NoError(t, h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{Succeeded: false, Retryable: true, ErrorMessage: "connection reset"},
	}))

	require.Nil(t, h.repo.rows[dep.ID()].dep.ActiveLease(), "parking the task dropped the lease")
	var exec pkgevents.TaskExecutionRecorded
	require.NoError(t, json.Unmarshal(h.outbox.byStream(streams.TaskExecutionRecordedV1).Payload, &exec))
	assert.Equal(t, startedAt.Format(time.RFC3339), exec.StartedAt,
		"the start was captured from the lease before parking dropped it")
	assert.Equal(t, h.clock.now.Format(time.RFC3339), exec.CompletedAt)
}

// TestComplete_RollsBackOnOutboxFailure pins that a report that cannot announce
// itself does not settle the row: the transition and its announcements commit
// together or not at all.
func TestComplete_RollsBackOnOutboxFailure(t *testing.T) {
	h := newHarness(t, 4)
	dep := h.seedDue(workerCmd())
	grant := h.mustClaim(t)
	h.outbox.err = assert.AnError

	err := h.svc.Complete(context.Background(), lease.CompleteInput{PoolKey: poolKey,
		DeploymentID: dep.ID(), LeaseID: grant.LeaseID, Token: grant.Token,
		Result: model.WorkerResult{Succeeded: true},
	})

	require.Error(t, err)
	assert.Empty(t, h.outbox.entries)
}
