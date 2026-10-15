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
	"github.com/carolsimone/continuo/state/service/topologyretention"
	"github.com/carolsimone/continuo/state/service/uow"
)

// settles is how long a goroutine that must be blocked on the topology lock is
// given to prove it is not progressing.
const settles = 500 * time.Millisecond

// waitFor returns the value ch delivers, failing the test if none arrives.
func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not finish", what)
		panic("unreachable")
	}
}

// requireBlocked fails if ch delivers within settles.
func requireBlocked[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("%s must wait on the topology lock, but finished with %v", what, v)
	case <-time.After(settles):
	}
}

// A run is created, a promotion moves the live release and the pruner runs, in
// that order, against a real Postgres. Creation reads the live release under
// the topology lock, so it serialises with both: a creation that starts while a
// promotion is in flight names the promoted release, and a promotion and a
// prune that start while a creation is in flight wait for its commit, after
// which the release the run names is referenced and survives.
func TestReleaseStamp_CreationSerialisesWithPromotionAndPruning(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	db, cleanup := setupPostgres(t)
	defer cleanup()
	h := newBuiltHarness(db)
	ctx := context.Background()
	alice := identity.FromUserID("okta|alice")

	h.goLive(t, "rel-old", 4)
	h.storeRelease(t, "rel-new", promotedNode("raw", "stamp_orders", "dbt-model"), scheduledNode("mart", "stamp_lock", "dbt-model", "stamp-lock"))
	_, err := db.Exec(`UPDATE topology_releases SET loaded_at = now() - interval '30 days' WHERE release_id = 'rel-old'`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at) VALUES ('stamp-lock', now(), now())`)
	require.NoError(t, err)

	promote := func(releaseID string, seq int64) func(context.Context, uow.UnitOfWork) error {
		return func(ctx context.Context, u uow.UnitOfWork) error {
			if err := u.Topology().LockTopology(ctx); err != nil {
				return err
			}
			return u.Topology().SetLivePointer(ctx, pkgevents.DefaultTenantID,
				topology.Pointer{ReleaseID: releaseID, PromotionSeq: seq}, time.Now())
		}
	}

	t.Run("a creation behind an uncommitted promotion names the promoted release", func(t *testing.T) {
		promotion := h.factory()
		require.NoError(t, promotion.Begin(ctx))
		defer promotion.Rollback() //nolint:errcheck
		require.NoError(t, promote("rel-new", 5)(ctx, promotion))

		created := make(chan uuid.UUID, 1)
		failed := make(chan error, 1)
		go func() {
			id, _, err := svchandlers.NewActivateScheduleHandler(defaultPlanner, h.logger).Handle(ctx, h.factory(), "stamp-lock", run.KindTrigger, nil, alice, model.OperationRun)
			if err != nil {
				failed <- err
				return
			}
			created <- id
		}()
		requireBlocked(t, created, "the creation")
		select {
		case err := <-failed:
			t.Fatalf("the creation failed: %v", err)
		default:
		}

		require.NoError(t, promotion.Commit())
		id := waitFor(t, created, "the creation")
		assert.Equal(t, run.ReleaseStamp{ReleaseID: "rel-new", PromotionSeq: 5}, h.stampOf(t, id))
	})

	t.Run("a promotion and a prune behind an uncommitted creation leave its release in place", func(t *testing.T) {
		// Put the live release back to rel-old, which is past retention and named
		// by no run yet.
		h.inTx(t, promote("rel-old", 4))
		_, err := db.Exec(`DELETE FROM scheduler_tracker WHERE release_id = 'rel-new'`)
		require.NoError(t, err)

		creation := h.factory()
		require.NoError(t, creation.Begin(ctx))
		defer creation.Rollback() //nolint:errcheck
		require.NoError(t, creation.Topology().LockTopology(ctx))
		p, err := creation.Topology().LivePointer(ctx, pkgevents.DefaultTenantID)
		require.NoError(t, err)
		require.Equal(t, "rel-old", p.ReleaseID)
		stamp := run.ReleaseStamp{ReleaseID: p.ReleaseID, PromotionSeq: p.PromotionSeq}
		rn, err := run.NewPendingRun("stamp-lock", run.KindTrigger, nil, alice.UserID, nil, model.OperationRun, stamp, time.Now())
		require.NoError(t, err)
		require.NoError(t, creation.Run().SaveRun(ctx, rn))

		promoted := make(chan error, 1)
		go func() {
			u := h.factory()
			if err := u.Begin(ctx); err != nil {
				promoted <- err
				return
			}
			defer u.Rollback() //nolint:errcheck
			if err := promote("rel-new", 5)(ctx, u); err != nil {
				promoted <- err
				return
			}
			promoted <- u.Commit()
		}()
		pruned := make(chan error, 1)
		go func() {
			_, err := topologyretention.NewPruner(h.factory, h.logger).Prune(ctx, 7*24*time.Hour, 1000)
			pruned <- err
		}()
		requireBlocked(t, promoted, "the promotion")
		requireBlocked(t, pruned, "the prune")

		require.NoError(t, creation.Commit())
		require.NoError(t, waitFor(t, promoted, "the promotion"))
		require.NoError(t, waitFor(t, pruned, "the prune"))

		assert.Equal(t, stamp, h.stampOf(t, rn.ScheduleID()))
		assert.Positive(t, countOf(t, db, `SELECT count(*) FROM topology_releases WHERE release_id = 'rel-old'`),
			"the release the run names survives the prune")
		assert.Positive(t, countOf(t, db, `SELECT count(*) FROM topology_nodes WHERE release_id = 'rel-old'`),
			"the nodes the run's graph reads survive the prune")
	})
}
