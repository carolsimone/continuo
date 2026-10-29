package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/run-controller/domain/events"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/carolsimone/continuo/run-controller/service/uow"
	"github.com/google/uuid"
)

// SeedReconciler runs the seed reconcile inside a release.promoted:v2
// transaction, after the live-release pointer step and under the same topology
// lock. It receives the handler's unit of work, so what it writes commits with
// the promotion.
type SeedReconciler interface {
	AfterPromotion(ctx context.Context, u uow.UnitOfWork, t ports.PromotionTrigger) error
}

// ReleasePromotedInput is one release.promoted:v2 entry with the topology its
// artifact holds. The binding parses Promotion from the entry; Load fills Nodes
// before the transaction opens.
type ReleasePromotedInput struct {
	Promotion events.ReleasePromoted
	Nodes     []topology.NodeSpec
}

// ReleasePromotedHandler stores each promoted release's topology and moves
// run-controller's live-release pointer.
//
// Load reads the release's topology artifact before the transaction opens.
// Handle then, in the message's transaction, stores the release once and moves
// the pointer only to a higher promotion seq: a redelivery changes nothing, an
// older promotion is kept as history, and another release under the live seq
// is logged and leaves the pointer. A move deletes the materialization rows of
// nodes the new live release does not have. When a SeedReconciler is set, it
// runs last.
type ReleasePromotedHandler struct {
	artifacts ports.TopologyArtifactReader
	seeds     SeedReconciler
	logger    *slog.Logger
}

// NewReleasePromotedHandler builds the handler. seeds may be nil: the handler
// then stores topologies and moves the pointer without reconciling seeds.
func NewReleasePromotedHandler(artifacts ports.TopologyArtifactReader, seeds SeedReconciler, logger *slog.Logger) *ReleasePromotedHandler {
	return &ReleasePromotedHandler{artifacts: artifacts, seeds: seeds, logger: logger}
}

// Load reads the promotion's topology artifact and returns in with its nodes.
// It runs before the transaction opens. A corrupt artifact, or one of another
// release, is permanent; a missing artifact is retried; an outage keeps its
// cause so the consumer pauses.
func (h *ReleasePromotedHandler) Load(ctx context.Context, in ReleasePromotedInput) (ReleasePromotedInput, error) {
	p := in.Promotion
	art, err := h.artifacts.Load(ctx, p.TopologyURI, p.TopologySHA256)
	if err != nil {
		if errors.Is(err, ports.ErrTopologyArtifactCorrupt) {
			return in, fmt.Errorf("%w: release %s: %w", pkgevents.ErrPermanent, p.ReleaseID, err)
		}
		return in, fmt.Errorf("load the topology artifact of release %s: %w", p.ReleaseID, err)
	}
	if art.ReleaseID != p.ReleaseID {
		return in, fmt.Errorf("%w: topology artifact %s belongs to release %q, the promotion is of %q",
			pkgevents.ErrPermanent, p.TopologyURI, art.ReleaseID, p.ReleaseID)
	}
	in.Nodes = art.Nodes
	return in, nil
}

// Handle applies one promotion.
//
// Caller contract: u.Begin(ctx) has been called; the handler MUST NOT commit.
// Returning nil tells the binding to commit; returning an error triggers
// rollback. A release whose nodes share a key, and a stored release announced
// with another checksum, are permanent.
func (h *ReleasePromotedHandler) Handle(ctx context.Context, u uow.UnitOfWork, in ReleasePromotedInput, msgProcID uuid.UUID) error {
	p := in.Promotion
	rel, err := topology.NewRelease(p.TenantID, p.ReleaseID, p.PromotionSeq, p.TopologyURI, p.TopologySHA256, in.Nodes)
	if err != nil {
		return fmt.Errorf("%w: release %s: %w", pkgevents.ErrPermanent, p.ReleaseID, err)
	}

	topo := u.Topology()
	if err := topo.LockTopology(ctx); err != nil {
		return fmt.Errorf("lock the topology: %w", err)
	}
	loaded, err := topo.SaveRelease(ctx, rel)
	if err != nil {
		if errors.Is(err, repository.ErrReleaseChecksumConflict) {
			return fmt.Errorf("%w: %w", pkgevents.ErrPermanent, err)
		}
		return fmt.Errorf("store release %s: %w", p.ReleaseID, err)
	}
	live, err := topo.LivePointer(ctx, p.TenantID)
	if err != nil {
		return fmt.Errorf("read the live release: %w", err)
	}

	outcome := topology.DecidePointer(live, p.ReleaseID, p.PromotionSeq)
	switch outcome {
	case topology.PointerMoved:
		next := topology.Pointer{ReleaseID: p.ReleaseID, PromotionSeq: p.PromotionSeq}
		if err := topo.SetLivePointer(ctx, p.TenantID, next, u.Clock().Now()); err != nil {
			return fmt.Errorf("move the live release: %w", err)
		}
		removed, err := u.Materializations().DeleteAbsentFrom(ctx, p.TenantID, p.ReleaseID)
		if err != nil {
			return fmt.Errorf("delete the materializations of removed nodes: %w", err)
		}
		h.logger.Info("release.promoted: live release moved",
			"tenant_id", p.TenantID, "release_id", p.ReleaseID, "promotion_seq", p.PromotionSeq,
			"previous_release_id", live.ReleaseID, "previous_promotion_seq", live.PromotionSeq,
			"loaded", loaded, "materializations_removed", removed)
	case topology.PointerConflict:
		h.logger.Error("release.promoted: another release is live under the same promotion seq; live release unchanged",
			"tenant_id", p.TenantID, "release_id", p.ReleaseID, "promotion_seq", p.PromotionSeq,
			"live_release_id", live.ReleaseID, "loaded", loaded)
	default:
		h.logger.Info("release.promoted: live release unchanged",
			"tenant_id", p.TenantID, "release_id", p.ReleaseID, "promotion_seq", p.PromotionSeq,
			"outcome", outcome.String(), "live_release_id", live.ReleaseID,
			"live_promotion_seq", live.PromotionSeq, "loaded", loaded)
	}

	if h.seeds == nil {
		return nil
	}
	if err := h.seeds.AfterPromotion(ctx, u, ports.PromotionTrigger{
		TenantID:            p.TenantID,
		ReleaseID:           p.ReleaseID,
		PromotionSeq:        p.PromotionSeq,
		Outcome:             outcome,
		FirstPromotion:      live.IsZero(),
		Bootstrap:           p.Bootstrap,
		ChangedNodeIDs:      p.ChangedNodeIDs,
		MessageProcessingID: msgProcID,
	}); err != nil {
		return fmt.Errorf("reconcile seeds after release %s: %w", p.ReleaseID, err)
	}
	return nil
}
