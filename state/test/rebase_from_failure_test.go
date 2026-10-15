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

	"github.com/carolsimone/continuo/state/domain/topology"
	statev1 "github.com/carolsimone/continuo/state/proto/state/v1"
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
		FROM task_tracker WHERE schedule_id = $1 ORDER BY node_key COLLATE "C"`, runID))
	return out
}

// finishRun records runID failed, so its schedule has no active run.
func finishRun(t *testing.T, runID string) {
	t.Helper()
	_, err := stateDB.Exec(`UPDATE scheduler_tracker SET status = 'failed', completed_at = now() WHERE schedule_id = $1`, runID)
	require.NoError(t, err)
}

// TestRebaseFromFailure drives the design's example through state's gRPC
// server: rebase-from-failure has two branches, mart.rff_x → mart.rff_y →
// mart.rff_z and mart.rff_p → mart.rff_q. The source run's rff_z failed; the
// live release fixes rff_y and changes rff_q. A rebase with the option
// rebuilds rff_y and then rff_z; without it rff_z alone; rff_q, on the other
// branch, is inherited either way.
func TestRebaseFromFailure(t *testing.T) {
	ctx := context.Background()
	const schedule = "rebase-from-failure"
	h := newBuiltHarness(stateDB)
	seedActiveSchedule(t, schedule)
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

	trig, err := stateClient.TriggerSchedule(ctx, &statev1.TriggerScheduleRequest{ScheduleName: schedule})
	require.NoError(t, err)
	srcID := trig.ScheduleId
	_, err = stateDB.Exec(`
		UPDATE task_tracker SET status = CASE node_key WHEN 'mart.rff_z' THEN 'failed' ELSE 'succeeded' END, retry_count = 2
		WHERE schedule_id = $1`, srcID)
	require.NoError(t, err)
	finishRun(t, srcID)
	at := time.Now().UTC().Add(-time.Hour)
	_, err = stateDB.Exec(`
		INSERT INTO task_execution (task_id, created_at, started_at, completed_at, execution_time_seconds)
		SELECT task_id, $2, $2, $2, 42.5 FROM task_tracker WHERE schedule_id = $1 AND node_key = 'mart.rff_y'`, srcID, at)
	require.NoError(t, err)

	live := promoteRelease(t, stateDB, nodes("sha256:rff_y-fixed", "sha256:rff_q-changed")...)
	require.NotEqual(t, source, live)
	changedY := &statev1.ChangedUpstreamNode{ServiceName: "service-1", SchemaName: "mart", TableName: "rff_y", Downstream: 1, LastSuccessSeconds: 42.5}

	t.Run("the preview counts what each choice runs and names the changed upstream node", func(t *testing.T) {
		on, err := stateClient.PreviewRebase(ctx, &statev1.PreviewRebaseRequest{SourceRunId: srcID, IncludeChangedUpstream: true})
		require.NoError(t, err)
		assert.Equal(t, live.ReleaseID, on.ReleaseId)
		assert.Equal(t, live.PromotionSeq, on.PromotionSeq)
		assert.Equal(t, []int32{5, 2, 1, 0, 1, 0, 3},
			[]int32{on.TotalNodes, on.WillRun, on.FailedOrSkipped, on.NewNodes, on.ChangedUpstream, on.Downstream, on.Inherited})
		require.Len(t, on.ChangedNodes, 1)
		assert.Equal(t, changedY.String(), on.ChangedNodes[0].String())

		off, err := stateClient.PreviewRebase(ctx, &statev1.PreviewRebaseRequest{SourceRunId: srcID})
		require.NoError(t, err)
		assert.Equal(t, []int32{5, 1, 1, 0, 0, 0, 4},
			[]int32{off.TotalNodes, off.WillRun, off.FailedOrSkipped, off.NewNodes, off.ChangedUpstream, off.Downstream, off.Inherited})
		require.Len(t, off.ChangedNodes, 1, "the option's nodes are named whether or not it is set")
		assert.Equal(t, changedY.String(), off.ChangedNodes[0].String())
	})

	t.Run("a rebase with the option rebuilds the changed upstream node first", func(t *testing.T) {
		resp, err := stateClient.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{
			SourceRunId: srcID, IncludeChangedUpstream: true, ExpectedReleaseId: live.ReleaseID,
		})
		require.NoError(t, err)
		assert.Equal(t, live, h.stampOf(t, uuid.MustParse(resp.RunId)))
		assert.Equal(t, []rebaseTask{
			{"mart.rff_p", "succeeded", true},
			{"mart.rff_q", "succeeded", true},
			{"mart.rff_x", "succeeded", true},
			{"mart.rff_y", "pending", false},
			{"mart.rff_z", "pending", false},
		}, rebaseTasks(t, resp.RunId))
		assert.Equal(t, []string{"mart.rff_y"}, h.created(t, uuid.MustParse(resp.RunId)).Dispatch)
		finishRun(t, resp.RunId)
	})

	t.Run("a rebase previewed on another live release is refused and creates no run", func(t *testing.T) {
		before := countOf(t, stateDB, `SELECT count(*) FROM scheduler_tracker`)

		_, err := stateClient.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{
			SourceRunId: srcID, IncludeChangedUpstream: true, ExpectedReleaseId: source.ReleaseID,
		})

		require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		assert.Equal(t, "the live release changed; preview again", status.Convert(err).Message())
		assert.Equal(t, before, countOf(t, stateDB, `SELECT count(*) FROM scheduler_tracker`))
	})

	var activeID string
	t.Run("a rebase without the option restarts from the failed node alone", func(t *testing.T) {
		resp, err := stateClient.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{SourceRunId: srcID, ExpectedReleaseId: live.ReleaseID})
		require.NoError(t, err)
		activeID = resp.RunId
		assert.Equal(t, []rebaseTask{
			{"mart.rff_p", "succeeded", true},
			{"mart.rff_q", "succeeded", true},
			{"mart.rff_x", "succeeded", true},
			{"mart.rff_y", "succeeded", true},
			{"mart.rff_z", "pending", false},
		}, rebaseTasks(t, resp.RunId))
		assert.Equal(t, []string{"mart.rff_z"}, h.created(t, uuid.MustParse(resp.RunId)).Dispatch)
	})

	t.Run("an active run refuses a rebase but not a preview", func(t *testing.T) {
		require.NotEmpty(t, activeID, "the previous subtest left its rebase running")

		_, err := stateClient.TriggerRebase(ctx, &statev1.TriggerRebaseRequest{SourceRunId: srcID, ExpectedReleaseId: live.ReleaseID})
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		assert.Contains(t, status.Convert(err).Message(), "active run")

		pv, err := stateClient.PreviewRebase(ctx, &statev1.PreviewRebaseRequest{SourceRunId: srcID, IncludeChangedUpstream: true})
		require.NoError(t, err)
		assert.Equal(t, int32(2), pv.WillRun)
		finishRun(t, activeID)
	})
}
