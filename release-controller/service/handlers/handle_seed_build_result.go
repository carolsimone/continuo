package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// HandleSeedBuildResultInput carries the aggregated candidate seed-build outcome
// from executor-controller (seed.build.completed:v1).
type HandleSeedBuildResultInput struct {
	ReleaseID   string `json:"release_id"`
	Status      string `json:"status"` // "ok" | "failed"
	ErrorClass  string `json:"error_class,omitempty"`
	ErrorDetail string `json:"error_detail,omitempty"`
}

// HandleSeedBuildResult advances a SeedBuilding release. On success it emits
// validation.requested:v1 (the full closure minus the just-built seeds, which now
// live in the candidate schema). On failure it rejects the release.
// If excluding the built seeds leaves an empty validation set, the release is
// promoted directly (mirror of the nothing-to-validate path in handleParseOK).
func HandleSeedBuildResult(ctx context.Context, d *Deps, in HandleSeedBuildResultInput) error {
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	r, err := u.ReleaseRepo().Get(ctx, in.ReleaseID)
	if err != nil {
		return fmt.Errorf("get release: %w", err)
	}
	if r == nil {
		d.Logger.Warn("seed build result for unknown release; dropping", "release_id", in.ReleaseID)
		return nil
	}
	now := d.Clock.Now()
	if in.Status != "ok" {
		return handleSeedBuildFailed(ctx, d, u, r, in, now)
	}
	return handleSeedBuildOK(ctx, d, u, r, in, now)
}

func handleSeedBuildFailed(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, in HandleSeedBuildResultInput, now time.Time) error {
	if err := r.TransitionToRejected("seed_build_failed", nil, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	payload, err := json.Marshal(map[string]string{
		"release_id":   in.ReleaseID,
		"reason":       "seed_build_failed",
		"error_class":  in.ErrorClass,
		"error_detail": in.ErrorDetail,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(in.ReleaseID),
		EventType:     "release_rejected",
		Payload:       payload,
		StreamName:    streams.ReleaseRejectedV1,
		Status:        "pending",
		MaxRetries:    3,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseSeedBuildCompleted(ctx, in.ReleaseID, false, 0)
	d.Telemetry.ReleaseRejected(ctx, in.ReleaseID, "seed_build_failed", nil)
	return nil
}

func handleSeedBuildOK(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, in HandleSeedBuildResultInput, now time.Time) error {
	if err := r.TransitionFromSeedBuilding(now); err != nil {
		return fmt.Errorf("transition from seed building: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	topo := r.CandidateTopology()
	allIDs := r.ValidationNodeIDs()

	// Recompute the changed-closure to identify the just-built seeds and exclude
	// them from the validation leg (they are already in the candidate schema).
	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get current prod: %w", err)
	}
	changed := release.DerivedChangedNodeIDs(topo, cp.TopologySnapshot())
	changedClosure := release.DescendantsClosure(topo, changed)
	changedClosureSet := make(map[string]bool, len(changedClosure))
	for _, id := range changedClosure {
		changedClosureSet[id] = true
	}
	builtSeeds := make(map[string]bool)
	for _, id := range newChangedSeedIDs(topo, allIDs, changedClosureSet) {
		builtSeeds[id] = true
	}

	// Filter the recorded validation IDs to exclude the just-built seeds.
	validationIDs := make([]string, 0, len(allIDs))
	for _, id := range allIDs {
		if !builtSeeds[id] {
			validationIDs = append(validationIDs, id)
		}
	}

	// Edge case: excluding the built seeds leaves nothing to validate (the release
	// was a seed-only change with no downstream models). Promote directly, mirroring
	// the nothing-to-validate short-circuit in handleParseOK.
	if len(validationIDs) == 0 {
		if err := promoteToProduction(ctx, d, u, r, in.ReleaseID, now); err != nil {
			return err
		}
		if err := u.Commit(); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
		d.Telemetry.ReleaseSeedBuildCompleted(ctx, in.ReleaseID, true, 0)
		d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, true, 0, 0, 0)
		d.Telemetry.ReleasePromoted(ctx, in.ReleaseID, len(topo))
		return nil
	}

	inSet := make(map[string]bool, len(validationIDs))
	for _, id := range validationIDs {
		inSet[id] = true
	}
	candidateSchema := "_candidate_" + sanitizeSchemaSuffix(in.ReleaseID)
	payload, err := json.Marshal(map[string]any{
		"release_id":        in.ReleaseID,
		"mode":              "validation",
		"nodes":             validationNodesInOrder(topo, validationIDs, inSet, changedClosureSet),
		"node_ids_in_order": validationIDs,
		"image_tags":        r.ImageTags(),
		"candidate_schema":  candidateSchema,
		"dbt_flags":         []string{"--empty"},
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(in.ReleaseID),
		EventType:     "validation_requested",
		Payload:       payload,
		StreamName:    streams.ValidationRequestedV1,
		Status:        "pending",
		MaxRetries:    3,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseSeedBuildCompleted(ctx, in.ReleaseID, true, 0)
	d.Telemetry.ReleaseValidationRequested(ctx, in.ReleaseID, len(validationIDs))
	return nil
}
