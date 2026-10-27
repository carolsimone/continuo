package e2e

import (
	"context"
	"database/sql"
	"testing"
	"time"

	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rebaseReleaseMovedMessage is the text of state's refusal of a rebase whose
// caller previewed a release that is no longer live. The ui recognises the
// refusal by this exact text.
const rebaseReleaseMovedMessage = "the live release changed; preview again"

// TestRebase_ReRunsAChangedUpstreamNode fails the failure schedule on one
// release, then announces a release in which ftable_c, upstream of the failing
// ftable_e, has a new content hash and ftable_a has only a new image tag. A
// rebase of the failed run onto that release:
//   - is previewed with ftable_c as the one changed upstream node, the three
//     nodes below it and its last successful duration, whether or not the
//     changed upstream nodes are included; ftable_a is not listed, because an
//     image tag alone is not a change;
//   - is refused, and creates no run, when the caller previewed the source's
//     release;
//   - without the changed upstream nodes re-executes the failed part (ftable_e
//     to ftable_h) and inherits ftable_a to ftable_d; once that rebase is
//     cancelled, a preview of a rebase of it still lists ftable_c, because its
//     inherited ftable_c was built on the base release, not on the live one
//     the cancelled rebase runs on;
//   - with them also re-executes ftable_c and ftable_d below it, which succeed,
//     and inherits only ftable_a and ftable_b.
//
// The e2e dbt projects are baked into the service images, so the announcement
// changes ftable_c's content hash but not its SQL, and ftable_e fails again in
// every rebase. That a fixed upstream node makes the failure pass is covered by
// state's TestRebaseFromFailure.
func TestRebase_ReRunsAChangedUpstreamNode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)
	verifyK8sAvailable(t, ctx)

	defer cleanupTestData(t, ctx, clients, failureTestScheduleName)
	cleanupTestData(t, ctx, clients, failureTestScheduleName)

	// The source run is stamped with the release live when it starts.
	base := seedTopology(t, ctx, clients)
	waitForStateLiveRelease(t, ctx, clients, base)

	// ── The source run fails on the base release ────────────────────────────
	srcIDStr := createAndActivateFailureScheduler(t, ctx, clients)
	srcID, err := uuid.Parse(srcIDStr)
	require.NoError(t, err)

	t.Log("Waiting for ftable_e and ftable_g to fail and ftable_f and ftable_h to be skipped...")
	verifyNodeExhaustedRetries(t, ctx, clients, srcID, "ftable_e")
	verifyNodeExhaustedRetries(t, ctx, clients, srcID, "ftable_g")
	verifyTasksSkipped(t, ctx, clients, srcID, []string{"ftable_f", "ftable_h"})
	verifySchedulerFailed(t, ctx, clients, srcID)
	require.Equal(t, base.ReleaseID, readRunStamp(t, ctx, clients, srcID).ReleaseID,
		"the source run runs on the release live when it started")

	// ── A new release changes ftable_c's content and only ftable_a's image ──
	nodes, schedules := e2eTopology(t, ctx, clients)
	changedHash := "sha256:e2e-rebase-" + uuid.NewString()
	for i := range nodes {
		switch nodes[i].TableName {
		case "ftable_c":
			nodes[i].ContentHash = changedHash
		case "ftable_a":
			// Every rebase below inherits ftable_a, so no Job pulls this tag.
			nodes[i].ImageTag = "e2e-rebase-image-only-" + uuid.NewString()[:8]
		}
	}
	live := announceTopology(t, ctx, "e2e-rebase-"+uuid.NewString()[:8], nodes)
	defer restoreDefaultTopology(t, ctx, clients)
	waitForAnnouncedTopology(t, ctx, clients, live, schedules)
	waitForStateLiveRelease(t, ctx, clients, live)

	wantChanged := []rebaseChangedNode{{Service: "service-3", Schema: seedSchemaName, Table: "ftable_c", Downstream: 3}}

	// ── Preview with the changed upstream nodes ─────────────────────────────
	// ftable_c's duration is recorded from its attempt's final report, which
	// can land after the run is read, so the preview is read until it carries
	// one.
	t.Log("Previewing the rebase with the changed upstream nodes...")
	var withUpstream *statev1.PreviewRebaseResponse
	var lastSeen string
	pollUntil(t, ctx, 2*time.Minute, 2*time.Second, func() (bool, error) {
		resp, pErr := clients.stateClient.PreviewRebase(ctx, &statev1.PreviewRebaseRequest{
			SourceRunId:            srcIDStr,
			IncludeChangedUpstream: true,
		})
		if pErr != nil {
			return false, pErr
		}
		if s := resp.String(); s != lastSeen {
			t.Logf("PreviewRebase: %s", s)
			lastSeen = s
		}
		withUpstream = resp
		for _, n := range resp.GetChangedNodes() {
			if n.GetTableName() == "ftable_c" && n.GetLastSuccessSeconds() > 0 {
				return true, nil
			}
		}
		return false, nil
	}, "PreviewRebase never listed ftable_c with its last successful duration")

	assert.Equal(t, rebasePreviewCounts{
		ReleaseID: live.ReleaseID, PromotionSeq: live.PromotionSeq,
		Total: 8, WillRun: 6, FailedOrSkipped: 4, New: 0, ChangedUpstream: 1, Downstream: 1, Inherited: 2,
	}, previewCountsOf(withUpstream),
		"ftable_e..h re-execute as failed or skipped, ftable_c as changed upstream and ftable_d below it; ftable_a and ftable_b are inherited")
	assert.Equal(t, wantChanged, changedNodesOf(withUpstream),
		"ftable_c is the one changed upstream node; ftable_a's image tag alone is not a change")

	// ── Preview of the failed part only ─────────────────────────────────────
	failedPart, err := clients.stateClient.PreviewRebase(ctx, &statev1.PreviewRebaseRequest{SourceRunId: srcIDStr})
	require.NoError(t, err)
	assert.Equal(t, rebasePreviewCounts{
		ReleaseID: live.ReleaseID, PromotionSeq: live.PromotionSeq,
		Total: 8, WillRun: 4, FailedOrSkipped: 4, New: 0, ChangedUpstream: 0, Downstream: 0, Inherited: 4,
	}, previewCountsOf(failedPart), "without the changed upstream nodes only ftable_e..h re-execute")
	assert.Equal(t, wantChanged, changedNodesOf(failedPart),
		"the changed upstream nodes are listed whether or not the preview includes them")

	// ── A rebase previewed on a release no longer live is refused ───────────
	// The refusal is checked while no run is active on the schedule: an active
	// run refuses a rebase before the live release is compared.
	runsBefore := countScheduleRuns(t, ctx, clients, failureTestScheduleName)
	_, err = clients.stateClient.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{
		SourceRunId:            srcIDStr,
		IncludeChangedUpstream: true,
		ExpectedReleaseId:      base.ReleaseID,
	})
	require.Error(t, err, "a rebase previewed on a release that is no longer live must be refused")
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	assert.Equal(t, rebaseReleaseMovedMessage, status.Convert(err).Message())
	assert.Equal(t, runsBefore, countScheduleRuns(t, ctx, clients, failureTestScheduleName),
		"a refused rebase creates no run")

	// ── Rebase without the changed upstream nodes ───────────────────────────
	t.Log("Rebasing the failed part only...")
	failedOnly := triggerRebaseOnto(t, ctx, clients, srcID, false, live)
	failedOnlyTasks := readRebaseTasks(t, ctx, clients, failedOnly)
	assertRebasePartition(t, failedOnlyTasks,
		[]string{"ftable_a", "ftable_b", "ftable_c", "ftable_d"},
		[]string{"ftable_e", "ftable_f", "ftable_g", "ftable_h"})
	assert.Equal(t, int(failedPart.GetWillRun()), countExecuted(failedOnlyTasks), "the rebase re-executes what its preview counted")
	cancelActiveRebase(t, ctx, clients, failedOnly)

	// ── A rebase of the cancelled rebase compares ftable_c with the base ────
	ofRebase, err := clients.stateClient.PreviewRebase(ctx, &statev1.PreviewRebaseRequest{
		SourceRunId:            failedOnly.String(),
		IncludeChangedUpstream: true,
	})
	require.NoError(t, err)
	assert.Equal(t, previewCountsOf(withUpstream), previewCountsOf(ofRebase),
		"the cancelled rebase's ftable_e..h re-execute, and ftable_c, which it inherited from the source run's build on the base release, is changed")
	assert.Equal(t, wantChanged, changedNodesOf(ofRebase),
		"ftable_c is compared with the release that built it, not with the live release the cancelled rebase runs on")

	// ── Rebase with the changed upstream nodes ──────────────────────────────
	t.Log("Rebasing with the changed upstream nodes...")
	withChanged := triggerRebaseOnto(t, ctx, clients, srcID, true, live)
	withChangedTasks := readRebaseTasks(t, ctx, clients, withChanged)
	assertRebasePartition(t, withChangedTasks,
		[]string{"ftable_a", "ftable_b"},
		[]string{"ftable_c", "ftable_d", "ftable_e", "ftable_f", "ftable_g", "ftable_h"})
	assert.Equal(t, int(withUpstream.GetWillRun()), countExecuted(withChangedTasks), "the rebase re-executes what its preview counted")

	t.Log("Waiting for the changed ftable_c and ftable_d below it to succeed...")
	waitForAllTasksSucceeded(t, ctx, clients, withChanged, []string{"ftable_c", "ftable_d"})

	t.Log("Waiting for the rebase to finish failed: ftable_e and ftable_g fail again...")
	verifyNodeExhaustedRetries(t, ctx, clients, withChanged, "ftable_e")
	verifyNodeExhaustedRetries(t, ctx, clients, withChanged, "ftable_g")
	verifySchedulerFailed(t, ctx, clients, withChanged)
	final := readRebaseTasks(t, ctx, clients, withChanged)
	got := make(map[string]string, len(final))
	for table, task := range final {
		got[table] = task.Status
	}
	assert.Equal(t, map[string]string{
		"ftable_a": "succeeded", "ftable_b": "succeeded",
		"ftable_c": "succeeded", "ftable_d": "succeeded",
		"ftable_e": "failed", "ftable_g": "failed",
		"ftable_f": "skipped", "ftable_h": "skipped",
	}, got, "final task statuses of the rebase with the changed upstream nodes")
}

// rebasePreviewCounts is the part of a PreviewRebaseResponse compared whole.
type rebasePreviewCounts struct {
	ReleaseID       string
	PromotionSeq    int64
	Total           int32
	WillRun         int32
	FailedOrSkipped int32
	New             int32
	ChangedUpstream int32
	Downstream      int32
	Inherited       int32
}

func previewCountsOf(p *statev1.PreviewRebaseResponse) rebasePreviewCounts {
	return rebasePreviewCounts{
		ReleaseID:       p.GetReleaseId(),
		PromotionSeq:    p.GetPromotionSeq(),
		Total:           p.GetTotalNodes(),
		WillRun:         p.GetWillRun(),
		FailedOrSkipped: p.GetFailedOrSkipped(),
		New:             p.GetNewNodes(),
		ChangedUpstream: p.GetChangedUpstream(),
		Downstream:      p.GetDownstream(),
		Inherited:       p.GetInherited(),
	}
}

// rebaseChangedNode is a changed upstream node of a preview, without its last
// successful duration, which varies from run to run.
type rebaseChangedNode struct {
	Service    string
	Schema     string
	Table      string
	Downstream int32
}

func changedNodesOf(p *statev1.PreviewRebaseResponse) []rebaseChangedNode {
	out := make([]rebaseChangedNode, 0, len(p.GetChangedNodes()))
	for _, n := range p.GetChangedNodes() {
		out = append(out, rebaseChangedNode{
			Service:    n.GetServiceName(),
			Schema:     n.GetSchemaName(),
			Table:      n.GetTableName(),
			Downstream: n.GetDownstream(),
		})
	}
	return out
}

// runStamp is the release a run is stamped with.
type runStamp struct {
	ReleaseID    string
	PromotionSeq int64
}

func readRunStamp(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) runStamp {
	t.Helper()
	var releaseID sql.NullString
	var seq sql.NullInt64
	require.NoError(t, clients.stateDB.QueryRowContext(ctx,
		`SELECT release_id, promotion_seq FROM runs WHERE run_id = $1`, runID).Scan(&releaseID, &seq))
	return runStamp{ReleaseID: releaseID.String, PromotionSeq: seq.Int64}
}

// countScheduleRuns counts the runs of scheduleName.
func countScheduleRuns(t *testing.T, ctx context.Context, clients *testClients, scheduleName string) int {
	t.Helper()
	var n int
	require.NoError(t, clients.stateDB.GetContext(ctx, &n,
		`SELECT count(*) FROM runs WHERE schedule_name = $1`, scheduleName))
	return n
}

// triggerRebaseOnto rebases source onto live, the release the caller
// previewed, and checks the new run: a rebase of source on the failure
// schedule, stamped with live.
func triggerRebaseOnto(t *testing.T, ctx context.Context, clients *testClients, source uuid.UUID, includeChangedUpstream bool, live announceResult) uuid.UUID {
	t.Helper()
	resp, err := clients.stateClient.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{
		SourceRunId:            source.String(),
		IncludeChangedUpstream: includeChangedUpstream,
		ExpectedReleaseId:      live.ReleaseID,
	})
	require.NoError(t, err, "TriggerRebase(include_changed_upstream=%v) on the previewed live release", includeChangedUpstream)
	runID, err := uuid.Parse(resp.GetRunId())
	require.NoError(t, err, "rebase run_id must be a UUID")
	assert.Equal(t, failureTestScheduleName, resp.GetScheduleName())
	t.Logf("Rebase run created: run_id=%s include_changed_upstream=%v", runID, includeChangedUpstream)

	assert.Equal(t, "rebase", queryRunKind(t, clients.stateDB, runID))
	assert.Equal(t, source.String(), queryRunSourceRunID(t, clients, runID))
	assert.Equal(t, runStamp{ReleaseID: live.ReleaseID, PromotionSeq: live.PromotionSeq}, readRunStamp(t, ctx, clients, runID),
		"a rebase runs on the live release")
	return runID
}

// rebaseTask is one task of a rebase run.
type rebaseTask struct {
	Status    string
	Inherited bool // carried over from the source as succeeded, not executed
}

// readRebaseTasks reads the tasks of runID by table name. state stores a
// run's tasks in the transaction that creates it.
func readRebaseTasks(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) map[string]rebaseTask {
	t.Helper()
	rows, err := clients.stateDB.QueryContext(ctx, `
		SELECT table_name, status, inherited_from_task_id IS NOT NULL AS inherited
		FROM tasks
		WHERE run_id = $1`, runID)
	require.NoError(t, err)
	defer rows.Close()

	tasks := map[string]rebaseTask{}
	for rows.Next() {
		var table string
		var task rebaseTask
		require.NoError(t, rows.Scan(&table, &task.Status, &task.Inherited))
		tasks[table] = task
	}
	require.NoError(t, rows.Err())
	return tasks
}

// assertRebasePartition checks that a rebase holds exactly the given tables,
// inherited ones carried over as succeeded and executed ones run again.
func assertRebasePartition(t *testing.T, tasks map[string]rebaseTask, inherited, executed []string) {
	t.Helper()
	assert.Len(t, tasks, len(inherited)+len(executed), "rebase tasks: %v", tasks)
	for _, table := range inherited {
		task, ok := tasks[table]
		if assert.True(t, ok, "%s is missing from the rebase", table) {
			assert.True(t, task.Inherited, "%s should be inherited (inherited_from_task_id set)", table)
			assert.Equal(t, "succeeded", task.Status, "%s should be inherited as succeeded", table)
		}
	}
	for _, table := range executed {
		task, ok := tasks[table]
		if assert.True(t, ok, "%s is missing from the rebase", table) {
			assert.False(t, task.Inherited, "%s should be executed (inherited_from_task_id NULL)", table)
		}
	}
}

func countExecuted(tasks map[string]rebaseTask) int {
	n := 0
	for _, task := range tasks {
		if !task.Inherited {
			n++
		}
	}
	return n
}

// cancelActiveRebase cancels runID, the active run of the failure schedule,
// and waits until it is cancelled, which leaves the schedule free.
func cancelActiveRebase(t *testing.T, ctx context.Context, clients *testClients, runID uuid.UUID) {
	t.Helper()
	resp, err := clients.stateClient.CancelSchedule(ctx, &statev1.CancelScheduleRequest{
		ScheduleName:       failureTestScheduleName,
		CancelledBy:        "e2e",
		CancellationReason: "rebase checked",
	})
	require.NoError(t, err, "cancel the rebase run on %s", failureTestScheduleName)
	require.Equal(t, runID.String(), resp.GetScheduleId(), "the active run of the schedule is the rebase")
	verifySchedulerCancelled(t, ctx, clients, runID)
}
