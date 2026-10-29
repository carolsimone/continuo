package test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/run-controller/domain/topology"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
)

// rebaseTask is one task of a rebase as its creating transaction stored it.
type rebaseTask struct {
	NodeKey   string `db:"node_key"`
	Status    string `db:"status"`
	Inherited bool   `db:"inherited"`
}

// rebaseTasks reads runID's tasks in key order.
func rebaseTasks(t *testing.T, runID string) []rebaseTask {
	t.Helper()
	var out []rebaseTask
	require.NoError(t, stateDB.Select(&out, `
		SELECT node_key, status, inherited_from_task_id IS NOT NULL AS inherited
		FROM tasks WHERE run_id = $1 ORDER BY node_key COLLATE "C"`, runID))
	return out
}

// finishRun records runID failed, so its schedule has no active run, and
// releases its tasks' node leases.
func finishRun(t *testing.T, runID string) {
	t.Helper()
	_, err := stateDB.Exec(`UPDATE runs SET status = 'failed', completed_at = now() WHERE run_id = $1`, runID)
	require.NoError(t, err)
	releaseLeasesOf(t, stateDB, uuid.MustParse(runID))
}

// TestRebaseFromFailure previews and rebases a failed run through state's
// gRPC server: rebase-from-failure has two branches, mart.rff_x → mart.rff_y →
// mart.rff_z and mart.rff_p → mart.rff_q. The source run's rff_z failed; the
// live release fixes rff_y and changes rff_q. A rebase with the option
// rebuilds rff_y and then rff_z; without it rff_z alone; rff_q, on the other
// branch, is inherited either way.
func TestRebaseFromFailure(t *testing.T) {
	ctx := context.Background()
	const schedule = "rebase-from-failure"
	h := newBuiltHarness(stateDB)
	nodes := func(yHash, qHash string) []topology.NodeSpec {
		y := scheduledNode("mart", "rff_y", "dbt-model", schedule, "mart.rff_x")
		y.ContentHash = yHash
		q := scheduledNode("mart", "rff_q", "dbt-model", schedule, "mart.rff_p")
		q.ContentHash = qHash
		return []topology.NodeSpec{
			scheduledNode("mart", "rff_x", "dbt-model", schedule), y,
			scheduledNode("mart", "rff_z", "dbt-model", schedule, "mart.rff_y"),
			scheduledNode("mart", "rff_p", "dbt-model", schedule), q,
		}
	}
	source := promoteRelease(t, stateDB, nodes("sha256:rff_y", "sha256:rff_q")...)

	trig, err := runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: uuid.NewString(), ScheduleName: schedule})
	require.NoError(t, err)
	srcID := trig.RunId
	_, err = stateDB.Exec(`
		UPDATE tasks SET status = CASE node_key WHEN 'mart.rff_z' THEN 'failed' ELSE 'succeeded' END, attempt = 2
		WHERE run_id = $1`, srcID)
	require.NoError(t, err)
	finishRun(t, srcID)
	at := time.Now().UTC().Add(-time.Hour)
	_, err = stateDB.Exec(`
		INSERT INTO task_attempts (tenant_id, task_id, attempt, job_name, status, started_at, finished_at, execution_seconds)
		SELECT tenant_id, task_id, 0, job_name, 'succeeded', $2, $2, 42.5 FROM tasks WHERE run_id = $1 AND node_key = 'mart.rff_y'`, srcID, at)
	require.NoError(t, err)

	live := promoteRelease(t, stateDB, nodes("sha256:rff_y-fixed", "sha256:rff_q-changed")...)
	require.NotEqual(t, source, live)
	changedY := &runv1.ChangedUpstreamNode{ServiceName: "service-1", SchemaName: "mart", TableName: "rff_y", Downstream: 1, LastSuccessSeconds: 42.5}

	t.Run("the preview counts what each choice runs and names the changed upstream node", func(t *testing.T) {
		on, err := runClient.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{SourceRunId: srcID, IncludeChangedUpstream: true})
		require.NoError(t, err)
		assert.Equal(t, live.ReleaseID, on.ReleaseId)
		assert.Equal(t, live.PromotionSeq, on.PromotionSeq)
		assert.Equal(t, []int32{5, 2, 1, 0, 1, 0, 3},
			[]int32{on.TotalNodes, on.WillRun, on.FailedOrSkipped, on.NewNodes, on.ChangedUpstream, on.Downstream, on.Inherited})
		require.Len(t, on.ChangedNodes, 1)
		assert.Equal(t, changedY.String(), on.ChangedNodes[0].String())

		off, err := runClient.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{SourceRunId: srcID})
		require.NoError(t, err)
		assert.Equal(t, []int32{5, 1, 1, 0, 0, 0, 4},
			[]int32{off.TotalNodes, off.WillRun, off.FailedOrSkipped, off.NewNodes, off.ChangedUpstream, off.Downstream, off.Inherited})
		require.Len(t, off.ChangedNodes, 1, "the option's nodes are named whether or not it is set")
		assert.Equal(t, changedY.String(), off.ChangedNodes[0].String())
	})

	t.Run("a rebase with the option rebuilds the changed upstream node first", func(t *testing.T) {
		resp, err := runClient.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{
			RequestId: uuid.NewString(), SourceRunId: srcID, IncludeChangedUpstream: true, ExpectedReleaseId: live.ReleaseID,
		})
		require.NoError(t, err)
		assert.Equal(t, live, h.stampOf(t, uuid.MustParse(resp.RunId)))
		assert.Equal(t, []rebaseTask{
			{"mart.rff_p", "succeeded", true},
			{"mart.rff_q", "succeeded", true},
			{"mart.rff_x", "succeeded", true},
			{"mart.rff_y", "dispatched", false},
			{"mart.rff_z", "pending", false},
		}, rebaseTasks(t, resp.RunId))
		assert.Equal(t, []string{"mart.rff_y"}, h.created(t, uuid.MustParse(resp.RunId)).Dispatch)
		finishRun(t, resp.RunId)
	})

	t.Run("a rebase previewed on another live release is refused and creates no run", func(t *testing.T) {
		before := countOf(t, stateDB, `SELECT count(*) FROM runs`)

		_, err := runClient.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{
			RequestId: uuid.NewString(), SourceRunId: srcID, IncludeChangedUpstream: true, ExpectedReleaseId: source.ReleaseID,
		})

		require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		assert.Equal(t, "the live release changed; preview again", status.Convert(err).Message())
		assert.Equal(t, before, countOf(t, stateDB, `SELECT count(*) FROM runs`))
	})

	var activeID string
	t.Run("a rebase without the option restarts from the failed node alone", func(t *testing.T) {
		resp, err := runClient.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{RequestId: uuid.NewString(), SourceRunId: srcID, ExpectedReleaseId: live.ReleaseID})
		require.NoError(t, err)
		activeID = resp.RunId
		assert.Equal(t, []rebaseTask{
			{"mart.rff_p", "succeeded", true},
			{"mart.rff_q", "succeeded", true},
			{"mart.rff_x", "succeeded", true},
			{"mart.rff_y", "succeeded", true},
			{"mart.rff_z", "dispatched", false},
		}, rebaseTasks(t, resp.RunId))
		assert.Equal(t, []string{"mart.rff_z"}, h.created(t, uuid.MustParse(resp.RunId)).Dispatch)
	})

	t.Run("an active run refuses a rebase but not a preview", func(t *testing.T) {
		require.NotEmpty(t, activeID, "the previous subtest left its rebase running")

		_, err := runClient.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{RequestId: uuid.NewString(), SourceRunId: srcID, ExpectedReleaseId: live.ReleaseID})
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		assert.Contains(t, status.Convert(err).Message(), "active run")

		pv, err := runClient.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{SourceRunId: srcID, IncludeChangedUpstream: true})
		require.NoError(t, err)
		assert.Equal(t, int32(2), pv.WillRun)
		finishRun(t, activeID)
	})

	// The rebase without the option runs on the live release but inherits
	// rff_y from the source run, which built it on the source release. When
	// its rff_z fails again, a rebase of it compares rff_y as the source run
	// built it with the live rff_y, and so still offers the fix.
	t.Run("a rebase of a rebase compares an inherited success with the release that built it", func(t *testing.T) {
		require.NotEmpty(t, activeID, "the rebase without the option is the source")
		_, err := stateDB.Exec(`UPDATE tasks SET status = 'failed', attempt = 2 WHERE run_id = $1 AND node_key = 'mart.rff_z'`, activeID)
		require.NoError(t, err)

		pv, err := runClient.PreviewRebase(ctx, &runv1.PreviewRebaseRequest{SourceRunId: activeID, IncludeChangedUpstream: true})
		require.NoError(t, err)
		assert.Equal(t, live.ReleaseID, pv.ReleaseId)
		assert.Equal(t, []int32{5, 2, 1, 0, 1, 0, 3},
			[]int32{pv.TotalNodes, pv.WillRun, pv.FailedOrSkipped, pv.NewNodes, pv.ChangedUpstream, pv.Downstream, pv.Inherited})
		require.Len(t, pv.ChangedNodes, 1)
		assert.Equal(t, changedY.String(), pv.ChangedNodes[0].String())

		resp, err := runClient.TriggerRebase(ctx, &runv1.TriggerRebaseRequest{
			RequestId: uuid.NewString(), SourceRunId: activeID, IncludeChangedUpstream: true, ExpectedReleaseId: live.ReleaseID,
		})
		require.NoError(t, err)
		assert.Equal(t, []rebaseTask{
			{"mart.rff_p", "succeeded", true},
			{"mart.rff_q", "succeeded", true},
			{"mart.rff_x", "succeeded", true},
			{"mart.rff_y", "dispatched", false},
			{"mart.rff_z", "pending", false},
		}, rebaseTasks(t, resp.RunId))
		assert.Equal(t, []string{"mart.rff_y"}, h.created(t, uuid.MustParse(resp.RunId)).Dispatch)
		finishRun(t, resp.RunId)
	})
}
