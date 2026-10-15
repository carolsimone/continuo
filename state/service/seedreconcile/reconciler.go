// Package seedreconcile loads into the warehouse the dbt seeds of the live
// release whose content it does not hold. A pass compares each live seed's
// content hash with the version state last recorded as loaded and, for the
// seeds that differ, creates one promote-seed run, which dispatches every one
// of them as it is created, and records a build request per seed. A pass runs
// inside every release.promoted:v2 transaction that moves the live release,
// and on a fixed interval; both hold the topology lock, so two passes never
// plan the same seed twice.
package seedreconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/seeds"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/handlers"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/runstart"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// maxRunIDProbes bounds the promote-seed run ids one pass tries for a release.
// A pass takes the number after the count of runs that name the release; a run
// that already holds that id without naming the release moves the pass on to
// the next number.
const maxRunIDProbes = 8

// Reconciler is the seed reconcile.
type Reconciler struct {
	uowFactory  func() uow.UnitOfWork
	maintenance bool
	tenantID    string
	logger      *slog.Logger
}

// Compile-time assertion that the reconciler is the seed step of the
// release.promoted:v2 handler.
var _ handlers.SeedReconciler = (*Reconciler)(nil)

// New builds the reconciler. RunOnce and Loop reconcile tenantID;
// AfterPromotion reconciles the promotion's tenant. While maintenance is true
// no pass runs, so no run is created; the first promotion's adoption is still
// recorded, since it starts no work.
func New(uowFactory func() uow.UnitOfWork, maintenance bool, tenantID string, logger *slog.Logger) *Reconciler {
	return &Reconciler{uowFactory: uowFactory, maintenance: maintenance, tenantID: tenantID, logger: logger}
}

// AfterPromotion runs in the release.promoted:v2 transaction, after the
// live-release pointer step and under the topology lock the handler holds.
//
// It acts only on a promotion that moved the live release. A redelivered,
// older or conflicting promotion left the live release as it was, so it reads
// nothing and creates nothing: the pass that followed the live release's own
// move committed with it, and the periodic pass covers whatever changed since.
//
// The first promotion state applies records the seeds it adopts as already
// loaded, and a pass over the new live release follows every move.
func (r *Reconciler) AfterPromotion(ctx context.Context, u uow.UnitOfWork, t ports.PromotionTrigger) error {
	if t.Outcome != topology.PointerMoved {
		return nil
	}
	if t.FirstPromotion {
		if err := r.adopt(ctx, u, t); err != nil {
			return err
		}
	}
	if r.maintenance {
		r.logger.Info("seed reconcile skipped: maintenance mode is on",
			"tenant_id", t.TenantID, "release_id", t.ReleaseID, "promotion_seq", t.PromotionSeq)
		return nil
	}
	return r.pass(ctx, u, t.TenantID, t.MessageProcessingID)
}

// RunOnce runs one pass for the reconciler's tenant in a transaction of its
// own, under the topology lock. During maintenance it does nothing.
func (r *Reconciler) RunOnce(ctx context.Context) error {
	if r.maintenance {
		return nil
	}
	u := r.uowFactory()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin the seed reconcile: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()
	if err := u.Topology().LockTopology(ctx); err != nil {
		return fmt.Errorf("lock the topology: %w", err)
	}
	if err := r.pass(ctx, u, r.tenantID, uuid.Nil); err != nil {
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit the seed reconcile: %w", err)
	}
	committed = true
	return nil
}

// Loop runs RunOnce every interval until ctx ends. A failed pass is logged and
// the next one runs on schedule. During maintenance the loop does not start.
func (r *Reconciler) Loop(ctx context.Context, interval time.Duration) {
	if r.maintenance {
		r.logger.Info("Seed reconcile loop not started: maintenance mode is on")
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("Seed reconcile pass failed", "tenant_id", r.tenantID, "error", err)
			}
		}
	}
}

// adopt records, on the first promotion state applies, the seeds treated as
// already loaded at the release's content: none for an install's first release
// (bootstrap); otherwise every seed the release did not change, and every seed
// when the release's first promote-seed run already exists.
func (r *Reconciler) adopt(ctx context.Context, u uow.UnitOfWork, t ports.PromotionTrigger) error {
	if t.Bootstrap {
		r.logger.Info("seed reconcile: bootstrap release, no seed adopted",
			"tenant_id", t.TenantID, "release_id", t.ReleaseID)
		return nil
	}
	live, err := u.Topology().ReleaseNodes(ctx, t.TenantID, t.ReleaseID, string(model.NodeTypeDbtSeed))
	if err != nil {
		return fmt.Errorf("read the seeds of release %s: %w", t.ReleaseID, err)
	}
	firstRunExists, err := u.Run().RunExists(ctx, seeds.RunID(t.ReleaseID, 1))
	if err != nil {
		return fmt.Errorf("look up the first promote-seed run of release %s: %w", t.ReleaseID, err)
	}
	now := u.Clock().Now()
	adopted := seeds.Adoptions(live, t.ChangedNodeIDs, false, firstRunExists)
	for _, d := range adopted {
		if err := u.Materializations().RecordBuilt(ctx, t.TenantID, seeds.Adopted(d, t.ReleaseID, now)); err != nil {
			return fmt.Errorf("adopt seed %s: %w", d.Key, err)
		}
	}
	r.logger.Info("seed reconcile: seeds adopted on the first promotion",
		"tenant_id", t.TenantID, "release_id", t.ReleaseID,
		"adopted", len(adopted), "first_run_exists", firstRunExists)
	return nil
}

// pass builds what the tenant's live release needs, in u's transaction, which
// holds the topology lock. The seeds the decision rules pick form one
// promote-seed run on the live release, each at its live image; each seed gets
// a build request naming that run, and the run's dispatches are written to the
// outbox under msgProcID.
func (r *Reconciler) pass(ctx context.Context, u uow.UnitOfWork, tenantID string, msgProcID uuid.UUID) error {
	live, err := u.Topology().LivePointer(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("read the live release: %w", err)
	}
	if live.IsZero() {
		return nil
	}
	nodes, err := u.Topology().ReleaseNodes(ctx, tenantID, live.ReleaseID, string(model.NodeTypeDbtSeed))
	if err != nil {
		return fmt.Errorf("read the seeds of release %s: %w", live.ReleaseID, err)
	}
	desired := seeds.DesiredFrom(nodes)
	if len(desired) == 0 {
		return nil
	}
	built, err := u.Materializations().List(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("read the loaded versions: %w", err)
	}
	toBuild := seeds.Plan(desired, built)
	if len(toBuild) == 0 {
		return nil
	}

	keys := make([]topology.NodeKey, 0, len(toBuild))
	for _, d := range toBuild {
		keys = append(keys, d.Key)
	}
	planner := runstart.Planner{TenantID: tenantID}
	sel, err := planner.Seeds(ctx, u, live, keys)
	if err != nil {
		return fmt.Errorf("select the seeds of release %s: %w", live.ReleaseID, err)
	}
	now := u.Clock().Now()
	seedRun, err := createRun(ctx, u, live, toBuild, now)
	if err != nil {
		return err
	}
	runID := seedRun.ScheduleID()
	for _, d := range toBuild {
		if err := u.Materializations().RecordAttempt(ctx, tenantID, d.Key, d.ContentHash, runID, now); err != nil {
			return fmt.Errorf("record the build request of %s: %w", d.Key, err)
		}
	}
	if err := planner.Start(ctx, u, seedRun, sel, msgProcID); err != nil {
		return err
	}
	r.logger.Info("seed reconcile: promote-seed run created",
		"tenant_id", tenantID, "release_id", live.ReleaseID, "promotion_seq", live.PromotionSeq,
		"run_id", runID, "seeds", keys)
	return nil
}

// createRun saves the promote-seed run that loads toBuild on the live release
// and returns it. The id is the release's run id numbered one past the runs
// that already name the release; an id another run holds moves it to the next
// number.
func createRun(ctx context.Context, u uow.UnitOfWork, live topology.Pointer, toBuild []seeds.Desired, now time.Time) (*run.Run, error) {
	existing, err := u.Run().CountReleaseRuns(ctx, live.ReleaseID, run.KindPromoteSeed)
	if err != nil {
		return nil, fmt.Errorf("count the promote-seed runs of release %s: %w", live.ReleaseID, err)
	}
	nodes := seedNodes(toBuild)
	for n := existing + 1; n <= existing+maxRunIDProbes; n++ {
		id := seeds.RunID(live.ReleaseID, n)
		newRun, _, err := run.NewPromotedSeedsRun(id, scheduleName(id), run.ReleaseStamp{ReleaseID: live.ReleaseID, PromotionSeq: live.PromotionSeq}, nodes, now)
		if err != nil {
			return nil, fmt.Errorf("new promote-seed run: %w", err)
		}
		err = u.Run().SaveRun(ctx, newRun)
		if errors.Is(err, run.ErrRunAlreadyExists) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("save promote-seed run %s: %w", id, err)
		}
		return newRun, nil
	}
	return nil, fmt.Errorf("no free promote-seed run id for release %s after %d tries", live.ReleaseID, maxRunIDProbes)
}

// seedNodes maps desired seeds onto the run's nodes, pinning each one's node
// type and live image.
func seedNodes(ds []seeds.Desired) []run.SeedNode {
	out := make([]run.SeedNode, 0, len(ds))
	for _, d := range ds {
		out = append(out, run.SeedNode{
			NodeID:   run.NodeID{ServiceName: d.ServiceName, SchemaName: d.SchemaName, TableName: d.TableName},
			NodeType: d.NodeType,
			ImageTag: d.ImageTag,
		})
	}
	return out
}

// scheduleName names a promote-seed run in the UI: a fixed prefix and the
// first eight hex digits of its id.
func scheduleName(id uuid.UUID) string {
	return "promote-seed-" + strings.ReplaceAll(id.String(), "-", "")[:8]
}
