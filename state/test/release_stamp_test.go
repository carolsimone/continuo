package test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/uow"
)

// goLive stores releaseID and makes it the default tenant's live release
// under seq.
func (h *builtHarness) goLive(t *testing.T, releaseID string, seq int64) {
	t.Helper()
	h.storeRelease(t, releaseID, scheduledNode("raw", "stamp_orders", "dbt-model", "stamp-rebase"),
		scheduledNode("mart", "stamp_trigger", "dbt-model", "stamp-trigger"))
	h.inTx(t, func(ctx context.Context, u uow.UnitOfWork) error {
		return u.Topology().SetLivePointer(ctx, pkgevents.DefaultTenantID,
			topology.Pointer{ReleaseID: releaseID, PromotionSeq: seq}, time.Now())
	})
}

// stampOf reads the release a run stored.
func (h *builtHarness) stampOf(t *testing.T, id uuid.UUID) run.ReleaseStamp {
	t.Helper()
	var row struct {
		ReleaseID    *string `db:"release_id"`
		PromotionSeq *int64  `db:"promotion_seq"`
	}
	require.NoError(t, h.db.Get(&row, `SELECT release_id, promotion_seq FROM scheduler_tracker WHERE schedule_id = $1`, id))
	var s run.ReleaseStamp
	if row.ReleaseID != nil {
		s.ReleaseID = *row.ReleaseID
	}
	if row.PromotionSeq != nil {
		s.PromotionSeq = *row.PromotionSeq
	}
	return s
}

// finished saves a failed run of schedule name on stamp, so it can be a rerun
// or rebase source.
func (h *builtHarness) finished(t *testing.T, name string, stamp run.ReleaseStamp) uuid.UUID {
	t.Helper()
	rn, _, err := run.NewPendingRun(name, run.KindCron, nil, identity.SystemUserID, nil, model.OperationRun, stamp, time.Now())
	require.NoError(t, err)
	id := h.saveRun(t, rn)
	_, err = h.db.Exec(`UPDATE scheduler_tracker SET status = 'failed', completed_at = now() WHERE schedule_id = $1`, id)
	require.NoError(t, err)
	_, err = h.db.Exec(`
		INSERT INTO task_tracker (task_id, schedule_id, service_name, schema_name, table_name, job_name, status, max_retries, node_key)
		VALUES ($1, $2, 'service-1', 'raw', 'stamp_orders', 'job', 'failed', 2, 'raw.stamp_orders')`, uuid.New(), id)
	require.NoError(t, err)
	return id
}

// TestReleaseStamp drives every creation path against a real Postgres: a run
// names the live release, or the source run's for a rerun and a
// snapshot_of_run single-node run.
func TestReleaseStamp(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	h := newBuiltHarness(db)
	ctx := context.Background()
	alice := identity.FromUserID("okta|alice")

	h.goLive(t, "rel-old", 4)
	source := h.finished(t, "stamp-daily", run.ReleaseStamp{ReleaseID: "rel-old", PromotionSeq: 4})
	h.goLive(t, "rel-live", 7)
	live := run.ReleaseStamp{ReleaseID: "rel-live", PromotionSeq: 7}
	old := run.ReleaseStamp{ReleaseID: "rel-old", PromotionSeq: 4}
	_, err := db.Exec(`INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at) VALUES ('stamp-trigger', now(), now())`)
	require.NoError(t, err)

	t.Run("a triggered run names the live release", func(t *testing.T) {
		id, _, err := svchandlers.NewActivateScheduleHandler(h.logger).Handle(ctx, h.factory(), "stamp-trigger", run.KindTrigger, nil, alice, model.OperationRun)
		require.NoError(t, err)
		assert.Equal(t, live, h.stampOf(t, id))
	})

	t.Run("a rerun names its source's release", func(t *testing.T) {
		id, _, err := svchandlers.NewTriggerRerunHandler(h.logger).Handle(ctx, h.factory(), source, alice)
		require.NoError(t, err)
		assert.Equal(t, old, h.stampOf(t, id))
	})

	t.Run("a rebase names the live release", func(t *testing.T) {
		src := h.finished(t, "stamp-rebase", old)
		id, _, err := svchandlers.NewTriggerRebaseHandler(h.logger).Handle(ctx, h.factory(), src, alice)
		require.NoError(t, err)
		assert.Equal(t, live, h.stampOf(t, id))
	})

	target := run.NodeID{ServiceName: "service-1", SchemaName: "raw", TableName: "stamp_orders"}

	t.Run("a latest single-node run names the live release", func(t *testing.T) {
		id, _, err := svchandlers.NewTriggerSingleNodeRunHandler(h.logger).Handle(ctx, h.factory(), svchandlers.TriggerSingleNodeRunInput{
			Target: target, MetadataSource: run.MetadataSourceLatest, Operation: model.OperationRun, Initiator: alice,
		})
		require.NoError(t, err)
		assert.Equal(t, live, h.stampOf(t, id))
	})

	t.Run("a snapshot_of_run single-node run names its source's release", func(t *testing.T) {
		src := h.finished(t, "stamp-snapshot", old)
		id, _, err := svchandlers.NewTriggerSingleNodeRunHandler(h.logger).Handle(ctx, h.factory(), svchandlers.TriggerSingleNodeRunInput{
			Target: target, MetadataSource: run.MetadataSourceSnapshotOfRun, SourceRunID: &src, Operation: model.OperationRun, Initiator: alice,
		})
		require.NoError(t, err)
		assert.Equal(t, old, h.stampOf(t, id))
	})

	t.Run("a stamped run whose tasks orchestrator chose records no built version", func(t *testing.T) {
		rn, _, err := run.NewPendingRun("stamp-cron", run.KindCron, nil, identity.SystemUserID, nil, model.OperationRun, live, time.Now())
		require.NoError(t, err)
		runID := h.saveRun(t, rn)
		taskID := h.dispatch(t, runID, "stamp_orders", "service-1:v1")
		h.finish(t, runID, taskID, run.TaskStatusSucceeded)

		_, ok := h.loaded(t, "raw.stamp_orders")
		assert.False(t, ok)
	})
}
