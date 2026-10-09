package handlers

import (
	"context"
	"fmt"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// HandleValidationResultInput carries the terminal validation decision (the
// kind=complete message on validation.result:v1). Per-node content is not
// carried here: each node's outcome arrives earlier on the same stream as a
// kind=node message and is projected into the release read model, then read back
// from the store when this terminal message decides promote-or-reject.
type HandleValidationResultInput struct {
	ReleaseID       string `json:"release_id"`
	AggregateStatus string `json:"aggregate_status"`
}

// HandleValidationResult decides the terminal outcome of a release's validation
// leg from the per-node results already projected into the release read model.
//
// The kind=complete message is emitted last, after every per-node message on
// validation.result:v1, so a single in-order consumer has normally stored every
// node's outcome by the time this runs. The decision itself reads only
// aggregate_status (which executor computed from every node's terminal outcome),
// so it is order-independent and needs no completeness barrier: if a node is
// absent from the store — a projection write not yet delivered, or permanently
// dropped — the decision still stands on aggregate_status and logs the missing
// nodes rather than blocking.
//
// If every present validation node passed and the aggregate status is ok: a
// candidate promotes to production (updates CurrentProd, upserts the changed
// service's service_prod pointer, emits release.promoted:v2) and a
// verification passes without touching either. Otherwise a candidate rejects
// and emits release.rejected:v1, while a verification fails and emits no
// release event. A run of either kind, on either outcome, emits
// pipeline.run.finished:v1.
func HandleValidationResult(ctx context.Context, d *Deps, in HandleValidationResultInput) error {
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	r, err := u.RunRepo().Load(ctx, in.ReleaseID) // FOR UPDATE: serialize against per-node upserts
	if err != nil {
		return fmt.Errorf("load release: %w", err)
	}
	if r == nil {
		// The release referenced by this result no longer exists (e.g. it was
		// pruned, or this is a stale/duplicate message reclaimed from a previous
		// consumer for a release that was deleted). There is nothing to promote
		// or reject; ack and drop rather than dereference a nil aggregate.
		d.Logger.Warn("validation result for unknown release; dropping",
			"release_id", in.ReleaseID)
		return nil
	}

	now := d.Clock.Now()

	// Per-node content lives in the read model, projected from the earlier
	// kind=node messages on validation.result:v1. Read the validation-stage
	// results the stream already stored rather than re-carrying them here.
	stored := map[string]pipeline.NodeValidationResult{}
	for _, n := range r.PerNodeResults() {
		if n.Stage == "validation" {
			stored[n.NodeID] = n
		}
	}

	// With a single in-order consumer the store is already complete when the
	// terminal message arrives, so no completeness barrier is needed. A node can
	// only be absent if its projection write was permanently dropped. Do not block
	// or treat it as failing: log the gap and decide from the authoritative
	// aggregate_status, which reflects that node's real outcome.
	var missing []string
	for _, id := range r.ValidationNodeIDs() {
		if _, ok := stored[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		d.Logger.Warn("deciding from aggregate_status; per-node audit missing nodes",
			"release_id", in.ReleaseID, "missing", missing)
	}

	// Only flag nodes that ARE present and non-ok. A missing node (whose
	// projection row was lost) has an absent/zero status; treating that as non-ok
	// would wrongly reject a good release when aggregate_status == "ok". With this
	// guard, missing nodes neither block nor fail, and the decision rests on the
	// aggregate. In the normal complete case every ValidationNodeID is present.
	var failing []string
	for _, id := range r.ValidationNodeIDs() {
		if n, ok := stored[id]; ok && n.Status != "ok" {
			failing = append(failing, id)
		}
	}

	aggregateOK := in.AggregateStatus == "ok"
	if len(failing) > 0 || !aggregateOK {
		return handleValidationFailed(ctx, d, u, r, in, failing, now)
	}
	return handleValidationOK(ctx, d, u, r, in, now)
}

// finishVerification ends a verification that survived the pipeline: it
// passes, saves, and announces the terminal decision. It never reads or
// writes current_prod or service_prod and emits no release event.
func finishVerification(ctx context.Context, u uow.UnitOfWork, r *pipeline.Run, now time.Time) error {
	if err := r.Pass(now); err != nil {
		return fmt.Errorf("pass verification %s: %w", r.ID(), err)
	}
	if err := u.RunRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save verification %s: %w", r.ID(), err)
	}
	return enqueueRunFinished(ctx, u, r, now)
}

// concludeValidated ends a run whose validation set passed (or was empty): a
// candidate is promoted to production, a verification passes.
func concludeValidated(ctx context.Context, d *Deps, u uow.UnitOfWork, r *pipeline.Run, now time.Time) error {
	if r.Kind() == pipeline.KindVerification {
		return finishVerification(ctx, u, r, now)
	}
	return promoteToProduction(ctx, d, u, r, now)
}

// promoteToProduction applies the promotion effects shared by the
// validation-passed path and the nothing-to-validate short-circuit: it takes
// the next promotion seq, points current_prod at this release's candidate
// topology artifact under that seq, upserts the changed service's
// service_prod pointer, transitions the release to Promoted, persists it, and
// writes the release.promoted:v2 and pipeline.run.finished:v1 outbox rows. The
// caller owns Begin/Commit and any telemetry. The release must already hold
// its candidate topology (i.e. be in Validating) and be a candidate — call
// concludeValidated instead when the run's kind is unknown to the caller.
func promoteToProduction(ctx context.Context, d *Deps, u uow.UnitOfWork, r *pipeline.Run, now time.Time) error {
	releaseID := r.ID()
	ref := r.CandidateTopologyRef()

	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get current prod: %w", err)
	}
	// Computed against the production topology BEFORE cp.Update replaces it.
	changed, err := changedSincePromotion(ctx, d, r, cp)
	if err != nil {
		return err
	}

	seq, err := u.PromotionSequenceRepo().Next(ctx)
	if err != nil {
		return fmt.Errorf("take promotion seq: %w", err)
	}
	if err := cp.Update(releaseID, ref, seq, now); err != nil {
		return fmt.Errorf("move current prod: %w", err)
	}
	if err := u.CurrentProdRepo().Upsert(ctx, cp); err != nil {
		return fmt.Errorf("upsert current prod: %w", err)
	}

	// Upsert the changed service's production pointer so future releases can
	// assemble this service's manifest key at their AdvanceQueue step.
	changedService := r.ChangedService()
	sp := release.NewServiceProd(
		changedService,
		releaseID,
		CanonicalManifestKey(d.Bucket, changedService, releaseID, r.ManifestKind()),
		r.ImageTags()[changedService],
		r.ManifestKind(),
		now,
	)
	if err := u.ServiceProdRepo().Upsert(ctx, sp); err != nil {
		return fmt.Errorf("upsert service_prod: %w", err)
	}

	if err := r.Promote(now); err != nil {
		return fmt.Errorf("transition to promoted: %w", err)
	}
	if err := u.RunRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	if err := enqueueReleasePromoted(ctx, u, events.ReleasePromoted{
		ReleaseID:       releaseID,
		PromotedAt:      now.UTC(),
		PromotionSeq:    seq,
		TopologyURI:     ref.URI,
		TopologySHA256:  ref.SHA256,
		ChangedNodeIDs:  changed,
		CandidateSchema: CandidateSchemaFor(releaseID),
		CodeBundleURI:   r.CodeBundleURI(),
		Repo:            r.Repo(),
		CommitSHA:       r.CommitSHA(),
		Bootstrap:       r.IsBootstrap(),
	}); err != nil {
		return err
	}
	return enqueueRunFinished(ctx, u, r, now)
}

func handleValidationOK(ctx context.Context, d *Deps, u uow.UnitOfWork, r *pipeline.Run, in HandleValidationResultInput, now time.Time) error {
	if err := concludeValidated(ctx, d, u, r, now); err != nil {
		return err
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, true, len(r.ValidationNodeIDs()), 0, 0)
	recordTerminalTelemetry(ctx, d, r, r.CandidateTopologyRef().NodeCount)
	return nil
}

// handleValidationFailed records a validation rejection. failing carries the
// validation nodes whose stored per-node outcome was not "ok"; it may be empty
// when only a non-ok aggregate_status triggered the rejection. The failing set
// is persisted on the Release aggregate and surfaced in the outbox payload
// alongside the raw aggregate_status so operators can distinguish why the release
// was rejected. The per-node audit rows are sourced from the read model that the
// kind=node messages projected, not from this terminal message. A node whose
// projection write was permanently dropped may be absent from the read model, in
// which case only present, non-ok nodes appear in failing.
func handleValidationFailed(ctx context.Context, d *Deps, u uow.UnitOfWork, r *pipeline.Run, in HandleValidationResultInput, failing []string, now time.Time) error {
	if err := r.Fail(string(pkg_model.RejectReasonValidationFailed), "", failing, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	topo, err := candidateTopology(ctx, d, r)
	if err != nil {
		return err
	}

	// Build a per-node lookup over the candidate topology so each entry in the
	// rejected payload carries, alongside its outcome:
	//   - candidate_artifact_uri: the S3 pointer (derived per run) to the artifact that was
	//     checked — mirroring the dbt_log_uri pattern as a pointer, not inline
	//     content;
	//   - node_type, file_path, service: the node's kind and source location as
	//     THIS candidate declares them.
	// The location comes from the candidate topology rather than being resolved
	// downstream because a rejected release is never promoted: the promoted
	// topology holds nothing for a newly-added node, and the previous release's
	// path for a node whose candidate moved it. node_type additionally lets the
	// remediation agent recognise a python node — whose candidate artifact is a
	// JSON validation spec rather than SQL — without a topology lookup of its own.
	type candidateFacts struct{ artifactURI, nodeType, filePath, service string }
	factsByNodeID := make(map[string]candidateFacts, len(topo))
	for _, n := range topo {
		factsByNodeID[n.UniqueID] = candidateFacts{
			artifactURI: candidateArtifactURI(d.Bucket, r.ID(), n),
			nodeType:    n.NodeType,
			filePath:    n.OriginalFilePath,
			service:     n.ServiceName,
		}
	}

	// Which candidate nodes changed against production, so each failing node
	// can name the changed ancestors that may be the root cause of its failure.
	prod, err := currentProdTopology(ctx, d, u)
	if err != nil {
		return err
	}
	changedSet := make(map[string]bool)
	for _, id := range release.DerivedChangedNodeIDs(topo, prod) {
		changedSet[id] = true
	}

	// Source the per-node audit rows from the projected read model, enriched with
	// each node's candidate artifact pointer, kind, and source location from the
	// candidate topology. A failing node additionally names its changed
	// ancestors, which may be the root cause of its failure.
	var perNode []ports.RejectedNode
	for _, nr := range r.PerNodeResults() {
		if nr.Stage != "validation" {
			continue
		}
		f := factsByNodeID[nr.NodeID]
		entry := ports.RejectedNode{
			NodeID:               nr.NodeID,
			Status:               nr.Status,
			DBTLogURI:            nr.DBTLogURI,
			RunResultsURI:        nr.RunResultsURI,
			CandidateArtifactURI: f.artifactURI,
			NodeType:             f.nodeType,
			FilePath:             f.filePath,
			Service:              f.service,
		}
		if nr.Status != "ok" {
			for _, a := range release.ChangedAncestors(topo, nr.NodeID, changedSet) {
				entry.ChangedAncestors = append(entry.ChangedAncestors, ports.ChangedAncestor{
					NodeID: a.NodeID, FilePath: a.FilePath, Service: a.Service, Depth: a.Depth,
				})
			}
		}
		perNode = append(perNode, entry)
	}

	payload, err := d.Rejections.Encode(ports.ReleaseRejection{
		Shape:           ports.RejectionShapeValidation,
		ReleaseID:       in.ReleaseID,
		Reason:          pkg_model.RejectReasonValidationFailed,
		FailingNodes:    failing,
		AggregateStatus: in.AggregateStatus,
		PerNode:         perNode,
		Repo:            r.Repo(),
		CommitSHA:       r.CommitSHA(),
		CodeBundleURI:   r.CodeBundleURI(),
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

	okCount := len(r.ValidationNodeIDs()) - len(failing)
	d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, false, okCount, len(failing), 0)
	recordTerminalTelemetry(ctx, d, r, 0)
	return nil
}
