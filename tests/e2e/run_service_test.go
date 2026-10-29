package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestE2E_RunService_ARetriedStartRunReturnsTheSameRun proves that a run-starting
// command runs once per request id, against the deployed state:
//
//	StartRun(e2e-schedule, request id A) → a run
//	→ StartRun with the same request id → the same run id, though the schedule
//	  now has an active run, and no second run
//	→ StartRun with the same request id and another operation → ALREADY_EXISTS
//	→ StartRun with a fresh request id while the first run is active →
//	  FAILED_PRECONDITION, and still one run and one command record
//	→ CancelRun of the run, which ends cancelled once execution-controller
//	  has stopped its Jobs
//
// The run's tasks are not waited for: the idempotency is decided in the
// transaction that creates the run, before any task runs. The test cancels
// the run and waits for it to end, so it leaves no Job running.
func TestE2E_RunService_ARetriedStartRunReturnsTheSameRun(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer cleanupTestData(t, ctx, clients, testScheduleName)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)
	cleanupTestData(t, ctx, clients, testScheduleName)

	waitForStateLiveRelease(t, ctx, clients, seedTopology(t, ctx, clients))

	requestID := uuid.NewString()
	first, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: testScheduleName})
	require.NoError(t, err, "StartRun(%s) with request id %s", testScheduleName, requestID)
	runID, err := uuid.Parse(first.GetRunId())
	require.NoError(t, err, "StartRun must return the run's id")
	t.Logf("StartRun(%s) with request id %s created run %s", testScheduleName, requestID, runID)

	// ── The same request id returns the same run ─────────────────────────────
	again, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: testScheduleName})
	require.NoError(t, err, "a retry answers with the first call's run, though the schedule now has an active run")
	assert.Equal(t, runID.String(), again.GetRunId(), "a retry returns the run the first call created")
	assert.Equal(t, 1, runsOfSchedule(t, ctx, clients, testScheduleName), "a retry creates no second run")
	assert.Equal(t, 1, commandRecordsOf(t, ctx, clients, requestID), "the request id is recorded once")
	t.Logf("✅ the retry returned run %s again and created no run", again.GetRunId())

	// ── The same request id with other parameters is refused ─────────────────
	_, err = clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: testScheduleName, Operation: "build"})
	require.Error(t, err, "a request id reused with another operation must be refused")
	assert.Equal(t, codes.AlreadyExists, status.Code(err), "a request id reused with other parameters is ALREADY_EXISTS: %v", err)
	assert.Equal(t, 1, runsOfSchedule(t, ctx, clients, testScheduleName), "a refused reuse creates no run")
	t.Logf("✅ the same request id with operation build was refused: %v", err)

	// ── A fresh request id is a new command, and the schedule is busy ────────
	fresh := uuid.NewString()
	_, err = clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: fresh, ScheduleName: testScheduleName})
	require.Error(t, err, "a fresh request id while the first run is active must be refused")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "a schedule with an active run refuses a new command: %v", err)
	assert.Equal(t, 1, runsOfSchedule(t, ctx, clients, testScheduleName), "a refused start creates no run")
	assert.Zero(t, commandRecordsOf(t, ctx, clients, fresh), "a refused command stores no record, so its id may be sent again")
	t.Logf("✅ a fresh request id while run %s is active was refused: %v", runID, err)

	// ── The run is cancelled and ends before the test does ───────────────────
	_, err = clients.runClient.CancelRun(ctx, &runv1.CancelRunRequest{
		RunId: runID.String(), CancelledBy: "e2e", CancellationReason: "run service e2e test ends",
	})
	require.NoError(t, err, "CancelRun(%s)", runID)
	verifySchedulerCancelled(t, ctx, clients, runID)
}

// runsOfSchedule counts the runs of one schedule in state's Postgres.
func runsOfSchedule(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) int {
	t.Helper()
	var n int
	require.NoError(t, clients.stateDB.GetContext(ctx, &n,
		`SELECT count(*) FROM runs WHERE tenant_id = $1 AND schedule_name = $2`,
		events.DefaultTenantID, scheduleName))
	return n
}

// commandRecordsOf counts the command_requests rows of one request id.
func commandRecordsOf(t *testing.T, ctx context.Context, clients *testClients, requestID string) int {
	t.Helper()
	var n int
	require.NoError(t, clients.stateDB.GetContext(ctx, &n,
		`SELECT count(*) FROM command_requests WHERE tenant_id = $1 AND request_id = $2`,
		events.DefaultTenantID, requestID))
	return n
}
