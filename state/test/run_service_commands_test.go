package test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
)

// commandRecords counts the command records stored under requestID, in every
// tenant.
func commandRecords(t *testing.T, requestID string) int {
	t.Helper()
	return countOf(t, stateDB, `SELECT count(*) FROM command_requests WHERE request_id = $1`, requestID)
}

// runStatusOf reads the status of runID.
func runStatusOf(t *testing.T, runID string) string {
	t.Helper()
	var s string
	require.NoError(t, stateDB.Get(&s, `SELECT status FROM runs WHERE run_id = $1`, runID))
	return s
}

// endCancelled records runID cancelled, as the report of its last stopped
// attempt would, and releases its tasks' node leases.
func endCancelled(t *testing.T, runID string) {
	t.Helper()
	_, err := stateDB.Exec(`UPDATE runs SET status = 'cancelled', completed_at = now() WHERE run_id = $1`, runID)
	require.NoError(t, err)
	releaseLeasesOf(t, stateDB, uuid.MustParse(runID))
}

// startRun starts a run of schedule through RunService under a request id of
// its own.
func startRun(t *testing.T, schedule string) string {
	t.Helper()
	resp, err := runClient.StartRun(context.Background(), &runv1.StartRunRequest{RequestId: uuid.NewString(), ScheduleName: schedule})
	require.NoError(t, err)
	return resp.GetRunId()
}

// requireReused asserts err is the refusal of a request id sent again with
// other parameters.
func requireReused(t *testing.T, err error) {
	t.Helper()
	require.Equal(t, codes.AlreadyExists, status.Code(err), "%v", err)
	assert.Equal(t, "request_id reused with different parameters", status.Convert(err).Message())
}

func TestStartRun_ARetryReturnsTheSameRun(t *testing.T) {
	ctx := context.Background()
	const schedule = "rs-start-retry"
	seedActiveSchedule(t, schedule)
	promoteSchedules(t, stateDB, schedule)
	requestID := uuid.NewString()

	first, err := runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: schedule})
	require.NoError(t, err)
	t.Cleanup(func() { finishRun(t, first.GetRunId()) })
	again, err := runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: schedule})
	require.NoError(t, err, "a retry answers with the first call's run, though the schedule now has an active run")
	assert.Equal(t, first.GetRunId(), again.GetRunId())
	spelled, err := runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: schedule, Operation: "run"})
	require.NoError(t, err, "an empty operation is read as run")
	assert.Equal(t, first.GetRunId(), spelled.GetRunId())

	assert.Equal(t, 1, countOf(t, stateDB, `SELECT count(*) FROM runs WHERE schedule_name = $1`, schedule))
	assert.Equal(t, 1, commandRecords(t, requestID))

	_, err = runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: schedule, Operation: "build"})
	requireReused(t, err)
	assert.Equal(t, 1, countOf(t, stateDB, `SELECT count(*) FROM runs WHERE schedule_name = $1`, schedule))
}

// A refused call stores nothing: the same request id sent once the refusal
// no longer holds is evaluated afresh and starts a run.
func TestStartRun_ARefusedCallStoresNothing(t *testing.T) {
	ctx := context.Background()
	const schedule = "rs-start-refused"
	seedActiveSchedule(t, schedule)
	promoteSchedules(t, stateDB, schedule)
	active := startRun(t, schedule)
	requestID := uuid.NewString()

	_, err := runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: schedule})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	assert.Zero(t, commandRecords(t, requestID))

	finishRun(t, active)
	retry, err := runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: schedule})
	require.NoError(t, err)
	t.Cleanup(func() { finishRun(t, retry.GetRunId()) })
	assert.NotEqual(t, active, retry.GetRunId())
	assert.Equal(t, 1, commandRecords(t, requestID))
}

// A request id is a tenant's own: the same id sent for two tenants starts a
// run in each.
func TestStartRun_TheSameRequestIDInTwoTenantsIsTwoRuns(t *testing.T) {
	ctx := context.Background()
	other := newEdgeTenant(t, stateDB)
	_, err := stateDB.Exec(`UPDATE runs SET status = 'failed', completed_at = now() WHERE tenant_id = $1 AND run_id = $2`, other.id, other.runID)
	require.NoError(t, err)
	releaseLeasesOf(t, stateDB, other.runID)
	const schedule = "rs-two-tenants"
	seedActiveSchedule(t, schedule)
	promoteSchedules(t, stateDB, schedule)
	requestID := uuid.New()
	alice := identity.FromUserID("okta|alice")

	mine, err := runCommands.StartRun(ctx, svchandlers.StartRunCommand{
		TenantID: pkgevents.DefaultTenantID, RequestID: requestID, ScheduleName: schedule, Initiator: alice,
	})
	require.NoError(t, err)
	t.Cleanup(func() { finishRun(t, mine.RunID.String()) })
	theirs, err := runCommands.StartRun(ctx, svchandlers.StartRunCommand{
		TenantID: other.id, RequestID: requestID, ScheduleName: other.schedule, Initiator: alice,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = stateDB.Exec(`DELETE FROM state_outbox WHERE aggregate_id = $1`, theirs.RunID) })

	assert.False(t, mine.Replayed)
	assert.False(t, theirs.Replayed, "another tenant's record of the id is not a repeat")
	assert.NotEqual(t, mine.RunID, theirs.RunID)
	var tenantOf string
	require.NoError(t, stateDB.Get(&tenantOf, `SELECT tenant_id FROM runs WHERE run_id = $1`, theirs.RunID))
	assert.Equal(t, other.id, tenantOf)
	assert.Equal(t, 2, commandRecords(t, requestID.String()))
}

// failedSource starts a run of schedule and records it failed with its task
// failed, so a rerun or a rebase can restart it.
func failedSource(t *testing.T, schedule string) string {
	t.Helper()
	src := startRun(t, schedule)
	_, err := stateDB.Exec(`UPDATE tasks SET status = 'failed', attempt = 2 WHERE run_id = $1`, src)
	require.NoError(t, err)
	finishRun(t, src)
	return src
}

func TestTriggerRerun_ARetryReturnsTheSameRun(t *testing.T) {
	ctx := context.Background()
	const schedule = "rs-rerun-retry"
	seedActiveSchedule(t, schedule)
	promoteSchedules(t, stateDB, schedule)
	src := failedSource(t, schedule)
	requestID := uuid.NewString()

	first, err := runClient.TriggerRerun(ctx, &runv1.TriggerRerunRequest{RequestId: requestID, SourceRunId: src})
	require.NoError(t, err)
	t.Cleanup(func() { finishRun(t, first.GetRunId()) })
	again, err := runClient.TriggerRerun(ctx, &runv1.TriggerRerunRequest{RequestId: requestID, SourceRunId: src})
	require.NoError(t, err)
	assert.Equal(t, first.GetRunId(), again.GetRunId())
	assert.Equal(t, schedule, first.GetScheduleName())
	assert.Equal(t, schedule, again.GetScheduleName(), "a replay answers with the run's schedule name")
	assert.Equal(t, 1, countOf(t, stateDB, `SELECT count(*) FROM runs WHERE source_run_id = $1`, src))

	_, err = runClient.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{RequestId: requestID, SourceRunId: src})
	requireReused(t, err)
}

func TestTriggerRebase_ARetryReturnsTheSameRun(t *testing.T) {
	ctx := context.Background()
	const schedule = "rs-rebase-retry"
	seedActiveSchedule(t, schedule)
	live := promoteSchedules(t, stateDB, schedule)
	src := failedSource(t, schedule)
	requestID := uuid.NewString()
	req := &runv1.TriggerRebaseRequest{RequestId: requestID, SourceRunId: src, ExpectedReleaseId: live.ReleaseID}

	first, err := runClient.TriggerRebase(ctx, req)
	require.NoError(t, err)
	t.Cleanup(func() { finishRun(t, first.GetRunId()) })
	again, err := runClient.TriggerRebase(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, first.GetRunId(), again.GetRunId())
	assert.Equal(t, schedule, again.GetScheduleName())
	assert.Equal(t, "rebase", schedulerKind(t, first.GetRunId()))

	_, err = runClient.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{
		RequestId: requestID, SourceRunId: src, ExpectedReleaseId: live.ReleaseID, IncludeChangedUpstream: true,
	})
	requireReused(t, err)
	assert.Equal(t, 1, countOf(t, stateDB, `SELECT count(*) FROM runs WHERE source_run_id = $1`, src))
}

func TestTriggerSingleNodeRun_ReuseForAnotherNodeIsAlreadyExists(t *testing.T) {
	ctx := context.Background()
	const schedule = "rs-single"
	promoteRelease(t, stateDB,
		scheduledNode("mart", "rs_single_a", "dbt-model", schedule),
		scheduledNode("mart", "rs_single_b", "dbt-model", schedule))
	requestID := uuid.NewString()
	nodeRun := func(table string) *runv1.TriggerSingleNodeRunRequest {
		return &runv1.TriggerSingleNodeRunRequest{
			RequestId: requestID, ServiceName: "service-1", SchemaName: "mart", TableName: table, MetadataSource: "latest",
		}
	}

	first, err := runClient.TriggerSingleNodeRun(ctx, nodeRun("rs_single_a"))
	require.NoError(t, err)
	t.Cleanup(func() { finishRun(t, first.GetRunId()) })
	assert.True(t, strings.HasPrefix(first.GetScheduleName(), "single-node-run-"), first.GetScheduleName())
	again, err := runClient.TriggerSingleNodeRun(ctx, nodeRun("rs_single_a"))
	require.NoError(t, err)
	assert.Equal(t, first.GetRunId(), again.GetRunId())
	assert.Equal(t, first.GetScheduleName(), again.GetScheduleName())

	_, err = runClient.TriggerSingleNodeRun(ctx, nodeRun("rs_single_b"))
	requireReused(t, err)
	assert.Equal(t, 1, commandRecords(t, requestID))
	assert.Equal(t, 1, countOf(t, stateDB, `SELECT count(*) FROM tasks WHERE node_key LIKE 'mart.rs_single_%'`))
}

// A retry of a cancel by schedule name answers with the run its first call
// cancelled, and leaves the run the schedule started since then running.
func TestCancelActiveRun_ARetryAfterCronStartedTheNextRunCancelsNothingMore(t *testing.T) {
	ctx := context.Background()
	const schedule = "rs-cancel-active"
	seedActiveSchedule(t, schedule)
	promoteSchedules(t, stateDB, schedule)
	r1 := startRun(t, schedule)
	requestID := uuid.NewString()
	cancel := &runv1.CancelActiveRunRequest{RequestId: requestID, ScheduleName: schedule, CancelledBy: "bob"}

	first, err := runClient.CancelActiveRun(ctx, cancel)
	require.NoError(t, err)
	assert.Equal(t, r1, first.GetRunId())
	assert.Equal(t, "cancelling", runStatusOf(t, r1))
	endCancelled(t, r1)

	h := newBuiltHarness(stateDB)
	r2, outcome, err := svchandlers.NewStartRunHandler(defaultPlanner, h.logger).Handle(ctx, h.factory(), pkgevents.DefaultTenantID, schedule,
		run.KindCron, nil, identity.System(), model.OperationRun)
	require.NoError(t, err)
	require.Equal(t, svchandlers.OutcomeRunStarted, outcome)
	t.Cleanup(func() { finishRun(t, r2.String()) })

	retry, err := runClient.CancelActiveRun(ctx, cancel)
	require.NoError(t, err)
	assert.Equal(t, r1, retry.GetRunId())
	assert.Equal(t, "running", runStatusOf(t, r2.String()))
	got, err := runClient.GetRun(ctx, &runv1.GetRunRequest{RunId: r2.String()})
	require.NoError(t, err)
	assert.Nil(t, got.GetRun().GetCancelledAt())
	assert.Equal(t, 1, commandRecords(t, requestID))
}

// A cancel of a schedule with no active run stores nothing, so the same
// request id cancels the run the schedule starts later.
func TestCancelActiveRun_NoActiveRunStoresNothing(t *testing.T) {
	ctx := context.Background()
	const schedule = "rs-cancel-none"
	seedActiveSchedule(t, schedule)
	promoteSchedules(t, stateDB, schedule)
	requestID := uuid.NewString()
	cancel := &runv1.CancelActiveRunRequest{RequestId: requestID, ScheduleName: schedule}

	_, err := runClient.CancelActiveRun(ctx, cancel)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	assert.Zero(t, commandRecords(t, requestID))

	r := startRun(t, schedule)
	t.Cleanup(func() { endCancelled(t, r) })
	got, err := runClient.CancelActiveRun(ctx, cancel)
	require.NoError(t, err)
	assert.Equal(t, r, got.GetRunId())
	assert.Equal(t, 1, commandRecords(t, requestID))
}

// A cancel of a run that is cancelling or cancelled answers with the run as
// it is and writes nothing; a run that ended otherwise is FailedPrecondition.
func TestCancelRun_ACancelledRunIsReturnedAsItIs(t *testing.T) {
	ctx := context.Background()
	const schedule = "rs-cancel-run"
	seedActiveSchedule(t, schedule)
	promoteSchedules(t, stateDB, schedule)
	r := startRun(t, schedule)
	outbox := func() int {
		return countOf(t, stateDB, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1`, r)
	}

	first, err := runClient.CancelRun(ctx, &runv1.CancelRunRequest{RunId: r, CancelledBy: "bob", CancellationReason: "wrong data"})
	require.NoError(t, err)
	assert.Equal(t, r, first.GetRun().GetRunId())
	assert.Equal(t, "cancelling", first.GetRun().GetStatus())
	assert.Equal(t, "bob", first.GetRun().GetCancelledBy())
	assert.Equal(t, "wrong data", first.GetRun().GetCancellationReason())
	assert.Equal(t, schedule, first.GetRun().GetScheduleName())
	afterFirst := outbox()

	again, err := runClient.CancelRun(ctx, &runv1.CancelRunRequest{RunId: r, CancelledBy: "carol"})
	require.NoError(t, err)
	assert.Equal(t, "cancelling", again.GetRun().GetStatus())
	assert.Equal(t, "bob", again.GetRun().GetCancelledBy(), "a repeated cancel changes nothing")
	assert.Equal(t, afterFirst, outbox())

	endCancelled(t, r)
	done, err := runClient.CancelRun(ctx, &runv1.CancelRunRequest{RunId: r, CancelledBy: "carol"})
	require.NoError(t, err)
	assert.Equal(t, "cancelled", done.GetRun().GetStatus())
	assert.Equal(t, "bob", done.GetRun().GetCancelledBy())
	assert.Equal(t, afterFirst, outbox())

	failed := startRun(t, schedule)
	finishRun(t, failed)
	_, err = runClient.CancelRun(ctx, &runv1.CancelRunRequest{RunId: failed})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	_, err = runClient.CancelRun(ctx, &runv1.CancelRunRequest{RunId: uuid.NewString()})
	assert.Equal(t, codes.NotFound, status.Code(err), "%v", err)
}

// PreviewRebase on RunService reports the rebase of a failed run and refuses
// an unknown source as NotFound.
func TestPreviewRebase_IsServedOnRunService(t *testing.T) {
	ctx := context.Background()
	const schedule = "rs-preview"
	seedActiveSchedule(t, schedule)
	live := promoteSchedules(t, stateDB, schedule)
	src := failedSource(t, schedule)

	got, err := runClient.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{SourceRunId: src})
	require.NoError(t, err)
	assert.Equal(t, live.ReleaseID, got.GetReleaseId())
	assert.Equal(t, int32(1), got.GetWillRun())
	_, err = runClient.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{SourceRunId: uuid.NewString()})
	assert.Equal(t, codes.NotFound, status.Code(err), "%v", err)
}
