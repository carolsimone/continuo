package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// HandleSeedBuildResultInput carries the aggregated candidate seed-build outcome
// from execution-controller (seed.build.completed:v1).
type HandleSeedBuildResultInput struct {
	ReleaseID   string
	Status      string // "ok" | "failed"
	PerNode     []NodeResult
	ErrorDetail string
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

	// Get, not Load: the seed-build leg has a single writer of this row, so no
	// run-row FOR UPDATE is needed. Holding no run-row lock also means the
	// nothing-to-validate short-circuit below can take the release-queue advisory
	// lock inside promoteToProduction without inverting any advisory↔run-row
	// order — only one of the two locks is ever held here.
	r, err := u.RunRepo().Get(ctx, in.ReleaseID)
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

// applyWarehouseFindings records what each seed's preflight found in the
// warehouse on the release's notice for that seed. A result without findings
// changes nothing, and neither does one for a seed the release has no notice for.
func applyWarehouseFindings(d *Deps, r *pipeline.Run, perNode []NodeResult) {
	for _, n := range perNode {
		if n.SeedPreflight == nil {
			continue
		}
		if !r.ApplyWarehouseFindings(n.NodeID, *n.SeedPreflight) {
			d.Logger.Info("seed preflight findings for a seed with no notice", "release_id", r.ID(), "node_id", n.NodeID)
		}
	}
}

func handleSeedBuildFailed(ctx context.Context, d *Deps, u uow.UnitOfWork, r *pipeline.Run, in HandleSeedBuildResultInput, now time.Time) error {
	// Build per-node results and derive the failing set.
	results, failing := stageResults(in.PerNode)

	if len(in.PerNode) == 0 {
		// A failed seed build with no per-node detail still rejects the release,
		// but carries nothing for the remediation classifier to act on.
		d.Logger.Warn("seed build failed with no per-node results; release rejected without a remediation trigger",
			"release_id", in.ReleaseID)
	}

	r.RecordStageResults("seed_build", results)
	applyWarehouseFindings(d, r, in.PerNode)

	// Build a single lookup from the candidate topology so per-node rejection
	// entries carry the source location the remediation agent needs: the
	// promoted topology GetNodeLocation serves cannot find newly-added seeds.
	type sourceLoc struct{ filePath, service string }
	topo, err := candidateTopology(ctx, d, r)
	if err != nil {
		return err
	}
	locByNodeID := make(map[string]sourceLoc, len(topo))
	for _, n := range topo {
		locByNodeID[n.UniqueID] = sourceLoc{filePath: n.OriginalFilePath, service: n.ServiceName}
	}

	// Text from the producer is the better explanation; when it sends none, say
	// which seeds did not build, the one thing the topology knows about the failure.
	detail := in.ErrorDetail
	if detail == "" {
		detail = release.SummarizeSeedFailure(
			failedNodes(failing, func(id string) string { return locByNodeID[id].service }))
	}
	if err := r.Fail(string(pkg_model.RejectReasonSeedBuildFailed), detail, failing, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}

	perNode := make([]ports.RejectedNode, len(in.PerNode))
	for i, n := range in.PerNode {
		loc := locByNodeID[n.NodeID]
		perNode[i] = ports.RejectedNode{
			NodeID:        n.NodeID,
			Status:        n.Status,
			DBTLogURI:     n.DBTLogURI,
			RunResultsURI: n.RunResultsURI,
			FilePath:      loc.filePath,
			Service:       loc.service,
		}
	}

	payload, err := d.Rejections.Encode(ports.ReleaseRejection{
		Shape:           ports.RejectionShapeSeedBuild,
		ReleaseID:       in.ReleaseID,
		Reason:          pkg_model.RejectReasonSeedBuildFailed,
		ErrorDetail:     detail,
		FailingNodes:    failing,
		PerNode:         perNode,
		Repo:            r.Repo(),
		CommitSHA:       r.CommitSHA(),
		CodeBundleURI:   r.CodeBundleURI(),
		CandidateSchema: CandidateSchemaFor(in.ReleaseID),
	})
	if err != nil {
		return fmt.Errorf("encode rejection: %w", err)
	}

	if err := emitReleaseRejected(ctx, u, r, payload); err != nil {
		return err
	}
	if err := u.RunRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	if err := enqueueRunFinished(ctx, u, r, now); err != nil {
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseSeedBuildCompleted(ctx, in.ReleaseID, false, r.StageDuration(pipeline.StatusSeedBuilding))
	recordTerminalTelemetry(ctx, d, r, 0)
	return nil
}

func handleSeedBuildOK(ctx context.Context, d *Deps, u uow.UnitOfWork, r *pipeline.Run, in HandleSeedBuildResultInput, now time.Time) error {
	// Record per-node seed-build results on the ok path so the UI can surface
	// per-seed build status even when the overall build succeeds.
	results, _ := stageResults(in.PerNode)
	r.RecordStageResults("seed_build", results)
	applyWarehouseFindings(d, r, in.PerNode)

	topo, err := candidateTopology(ctx, d, r)
	if err != nil {
		return err
	}
	allIDs := r.ValidationNodeIDs()

	// Recompute the rebuilt-from-candidate set to identify the just-built seeds
	// and exclude them from the validation leg (they are already in the candidate
	// schema). rebuiltFromCandidateFor derives it as the parse leg did, so a
	// verification run identifies the same built seeds and assigns the same
	// per-node build strategy here as it did when it requested the seed build.
	prod, err := currentProdTopology(ctx, d, u)
	if err != nil {
		return err
	}
	rebuiltFromCandidate, err := rebuiltFromCandidateFor(ctx, d, u, r, topo, prod, allIDs)
	if err != nil {
		return err
	}
	builtSeeds := make(map[string]bool)
	for _, id := range newChangedSeedIDs(topo, allIDs, rebuiltFromCandidate) {
		builtSeeds[id] = true
	}

	// Filter the recorded validation IDs to exclude the just-built seeds. This
	// same filtered set is both persisted onto the release (below) and sent to
	// the validation leg, keeping the release's stored validation node set equal
	// to exactly the nodes execution-controller validates and emits per-node results for.
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
	if err := u.RunRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	// Edge case: excluding the built seeds leaves nothing to validate (the release
	// was a seed-only change with no downstream models). Promote directly, mirroring
	// the nothing-to-validate short-circuit in handleParseOK.
	if len(validationIDs) == 0 {
		if err := concludeValidated(ctx, d, u, r, now); err != nil {
			return err
		}
		if err := u.Commit(); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
		d.Telemetry.ReleaseSeedBuildCompleted(ctx, in.ReleaseID, true, r.StageDuration(pipeline.StatusSeedBuilding))
		d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, true, 0, 0, r.StageDuration(pipeline.StatusValidating))
		recordTerminalTelemetry(ctx, d, r, len(topo))
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
		"nodes":             validationNodesInOrder(d.Bucket, in.ReleaseID, topo, validationIDs, inSet, rebuiltFromCandidate),
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
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseSeedBuildCompleted(ctx, in.ReleaseID, true, r.StageDuration(pipeline.StatusSeedBuilding))
	d.Telemetry.ReleaseValidationRequested(ctx, in.ReleaseID, len(validationIDs))
	return nil
}
