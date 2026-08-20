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
	ReleaseID   string       `json:"release_id"`
	Status      string       `json:"status"` // "ok" | "failed"
	PerNode     []NodeResult `json:"per_node"`
	ErrorClass  string       `json:"error_class,omitempty"`
	ErrorDetail string       `json:"error_detail,omitempty"`
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
	// Build per-node results and derive the failing set.
	results, failing := stageResults(in.PerNode)

	if len(in.PerNode) == 0 {
		// A failed seed build with no per-node detail still rejects the release,
		// but carries nothing for the remediation classifier to act on.
		d.Logger.Warn("seed build failed with no per-node results; release rejected without a remediation trigger",
			"release_id", in.ReleaseID)
	}

	r.RecordStageResults("seed_build", results)
	if err := r.TransitionToRejected("seed_build_failed", in.ErrorDetail, failing, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	// perNodeEntry is the outbox wire shape for a single seed-build-leg result.
	// FilePath and Service carry the source location from the candidate topology
	// so the remediation agent can locate the seed source file without querying
	// GetNodeLocation, which only holds promoted topology and cannot find
	// newly-added seeds.
	type perNodeEntry struct {
		NodeID        string `json:"node_id"`
		Status        string `json:"status"`
		DBTLogURI     string `json:"dbt_log_uri,omitempty"`
		RunResultsURI string `json:"run_results_uri,omitempty"`
		FilePath      string `json:"file_path,omitempty"`
		Service       string `json:"service,omitempty"`
	}

	// Build a single lookup from the candidate topology so per-node rejection
	// entries carry the source location needed by the remediation agent.
	type sourceLoc struct{ filePath, service string }
	locByNodeID := make(map[string]sourceLoc, len(r.CandidateTopology()))
	for _, n := range r.CandidateTopology() {
		locByNodeID[n.UniqueID] = sourceLoc{filePath: n.OriginalFilePath, service: n.ServiceName}
	}

	perNode := make([]perNodeEntry, len(in.PerNode))
	for i, n := range in.PerNode {
		loc := locByNodeID[n.NodeID]
		perNode[i] = perNodeEntry{
			NodeID:        n.NodeID,
			Status:        n.Status,
			DBTLogURI:     n.DBTLogURI,
			RunResultsURI: n.RunResultsURI,
			FilePath:      loc.filePath,
			Service:       loc.service,
		}
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":       in.ReleaseID,
		"stage":            "seed_build",
		"reason":           "seed_build_failed",
		"error_class":      in.ErrorClass,
		"error_detail":     in.ErrorDetail,
		"failing_nodes":    failing,
		"per_node":         perNode,
		"repo":             r.Repo(),
		"commit_sha":       r.CommitSHA(),
		"code_bundle_uri":  r.CodeBundleURI(),
		"candidate_schema": CandidateSchemaFor(in.ReleaseID),
		"shadow":           r.IsShadow(),
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
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseSeedBuildCompleted(ctx, in.ReleaseID, false, 0)
	d.Telemetry.ReleaseRejected(ctx, in.ReleaseID, "seed_build_failed", failing)
	return nil
}

func handleSeedBuildOK(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, in HandleSeedBuildResultInput, now time.Time) error {
	// Record per-node seed-build results on the ok path so the UI can surface
	// per-seed build status even when the overall build succeeds.
	results, _ := stageResults(in.PerNode)
	r.RecordStageResults("seed_build", results)

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

	// Filter the recorded validation IDs to exclude the just-built seeds. This
	// same filtered set is both persisted onto the release (below) and sent to
	// the validation leg, keeping the release's stored validation node set equal
	// to exactly the nodes the executor validates and emits per-node results for.
	validationIDs := make([]string, 0, len(allIDs))
	for _, id := range allIDs {
		if !builtSeeds[id] {
			validationIDs = append(validationIDs, id)
		}
	}

	// Advance to Validating and narrow the persisted validation set to the
	// filtered IDs. Done before the empty-set promote short-circuit, which needs
	// the status already Validating.
	if err := r.TransitionFromSeedBuilding(validationIDs, now); err != nil {
		return fmt.Errorf("transition from seed building: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
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
		if !r.IsShadow() {
			d.Telemetry.ReleasePromoted(ctx, in.ReleaseID, len(topo))
		}
		return nil
	}

	inSet := make(map[string]bool, len(validationIDs))
	for _, id := range validationIDs {
		inSet[id] = true
	}
	candidateSchema := CandidateSchemaFor(in.ReleaseID)
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
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
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
