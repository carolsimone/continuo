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
// If every present validation node passed and the aggregate status is ok:
// promotes the release to production, updates CurrentProd, upserts the changed
// service's service_prod pointer, and emits release.promoted:v1. Otherwise
// rejects the release and emits release.rejected:v1.
func HandleValidationResult(ctx context.Context, d *Deps, in HandleValidationResultInput) error {
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	r, err := u.ReleaseRepo().Load(ctx, in.ReleaseID) // FOR UPDATE: serialize against per-node upserts
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
	stored := map[string]release.NodeValidationResult{}
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

// promoteToProduction applies the promotion effects shared by the
// validation-passed path and the nothing-to-validate short-circuit: it points
// current_prod at this release's candidate topology, upserts the changed
// service's service_prod pointer, transitions the release to Promoted, persists
// it, and writes the release.promoted:v1 outbox row. The caller owns
// Begin/Commit and any telemetry. The release must already hold its candidate
// topology (i.e. be in Validating).
func promoteToProduction(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, releaseID string, now time.Time) error {
	// A shadow release runs the same parse+validation pipeline as a normal
	// release but must never reach production: it exists only to verify a
	// proposed fix. Stop at Validated instead of touching current_prod,
	// service_prod, or emitting release.promoted:v1.
	if r.IsShadow() {
		if err := r.TransitionToValidated(now); err != nil {
			return fmt.Errorf("transition shadow release %s to validated: %w", releaseID, err)
		}
		if err := u.ReleaseRepo().Save(ctx, r); err != nil {
			return fmt.Errorf("save validated shadow release %s: %w", releaseID, err)
		}
		return nil
	}

	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get current prod: %w", err)
	}
	// candidate_artifact_uri is transient, release-specific validation data; the promoted
	// topology (current_prod and the release.promoted event) must not carry it.
	promotedTopo := r.CandidateTopology().WithoutCandidateArtifactURI()

	// Determine which nodes actually changed versus the prod being replaced, so
	// the release.promoted event can tag them. Computed against cp's snapshot
	// BEFORE cp.Update overwrites it below. Bootstrap (empty prod) flags all.
	changedSet := make(map[string]bool)
	for _, id := range release.DerivedChangedNodeIDs(promotedTopo, cp.TopologySnapshot()) {
		changedSet[id] = true
	}

	cp.Update(releaseID, promotedTopo, now)
	if err := u.CurrentProdRepo().Upsert(ctx, cp); err != nil {
		return fmt.Errorf("upsert current prod: %w", err)
	}

	// Upsert the changed service's production pointer so future releases can
	// assemble this service's manifest key at their AdvanceQueue step.
	changed := r.ChangedService()
	sp := release.NewServiceProd(
		changed,
		releaseID,
		CanonicalManifestKey(d.Bucket, changed, releaseID, r.ManifestKind()),
		r.ImageTags()[changed],
		r.ManifestKind(),
		now,
	)
	if err := u.ServiceProdRepo().Upsert(ctx, sp); err != nil {
		return fmt.Errorf("upsert service_prod: %w", err)
	}

	if err := r.TransitionToPromoted(now); err != nil {
		return fmt.Errorf("transition to promoted: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	type promotedNodeWire struct {
		UniqueID          string   `json:"unique_id"`
		SchemaName        string   `json:"schema_name"`
		TableName         string   `json:"table_name"`
		ServiceName       string   `json:"service_name"`
		NodeType          string   `json:"node_type"`
		ContentHash       string   `json:"content_hash"`
		TestCount         int      `json:"test_count"`
		ImageTag          string   `json:"image_tag"`
		UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
		Schedule          string   `json:"schedule"`
		Changed           bool     `json:"changed"`
		OriginalFilePath  string   `json:"original_file_path"`
	}
	wireTopo := make([]promotedNodeWire, len(promotedTopo))
	for i, n := range promotedTopo {
		wireTopo[i] = promotedNodeWire{
			UniqueID:          n.UniqueID,
			SchemaName:        n.SchemaName,
			TableName:         n.TableName,
			ServiceName:       n.ServiceName,
			NodeType:          n.NodeType,
			ContentHash:       n.ContentHash,
			TestCount:         n.TestCount,
			ImageTag:          n.ImageTag,
			UpstreamUniqueIDs: n.UpstreamUniqueIDs,
			Schedule:          n.Schedule,
			Changed:           changedSet[n.UniqueID],
			OriginalFilePath:  n.OriginalFilePath,
		}
	}
	payload, err := json.Marshal(map[string]any{
		"release_id":       releaseID,
		"topology":         wireTopo,
		"image_tags":       r.ImageTags(),
		"repo":             r.Repo(),
		"commit_sha":       r.CommitSHA(),
		"promoted_at":      now.UTC(),
		"candidate_schema": CandidateSchemaFor(releaseID),
		"code_bundle_uri":  r.CodeBundleURI(),
		"bootstrap":        r.IsBootstrap(),
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	return u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(releaseID),
		EventType:     "release_promoted",
		Payload:       payload,
		StreamName:    streams.ReleasePromotedV1,
		Status:        "pending",
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
		CreatedAt:     now,
	})
}

func handleValidationOK(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, in HandleValidationResultInput, now time.Time) error {
	if err := promoteToProduction(ctx, d, u, r, in.ReleaseID, now); err != nil {
		return err
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, true, len(r.ValidationNodeIDs()), 0, 0)
	if !r.IsShadow() {
		d.Telemetry.ReleasePromoted(ctx, in.ReleaseID, len(r.CandidateTopology()))
	}
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
func handleValidationFailed(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, in HandleValidationResultInput, failing []string, now time.Time) error {
	if err := r.TransitionToRejected("validation_failed", "", failing, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	// Build a per-node lookup over the candidate topology so each entry in the
	// rejected payload carries, alongside its outcome:
	//   - candidate_artifact_uri: the S3 pointer to the artifact that was
	//     validated — mirroring the dbt_log_uri pattern as a pointer, not inline
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
	factsByNodeID := make(map[string]candidateFacts, len(r.CandidateTopology()))
	for _, n := range r.CandidateTopology() {
		factsByNodeID[n.UniqueID] = candidateFacts{
			artifactURI: n.CandidateArtifactURI,
			nodeType:    n.NodeType,
			filePath:    n.OriginalFilePath,
			service:     n.ServiceName,
		}
	}

	type perNodeEntry struct {
		NodeID               string `json:"node_id"`
		Status               string `json:"status"`
		DBTLogURI            string `json:"dbt_log_uri,omitempty"`
		RunResultsURI        string `json:"run_results_uri,omitempty"`
		CandidateArtifactURI string `json:"candidate_artifact_uri,omitempty"`
		NodeType             string `json:"node_type,omitempty"`
		FilePath             string `json:"file_path,omitempty"`
		Service              string `json:"service,omitempty"`
	}
	// Source the per-node audit rows from the projected read model, enriched with
	// each node's candidate artifact pointer, kind, and source location from the
	// candidate topology.
	var perNode []perNodeEntry
	for _, nr := range r.PerNodeResults() {
		if nr.Stage != "validation" {
			continue
		}
		f := factsByNodeID[nr.NodeID]
		perNode = append(perNode, perNodeEntry{
			NodeID:               nr.NodeID,
			Status:               nr.Status,
			DBTLogURI:            nr.DBTLogURI,
			RunResultsURI:        nr.RunResultsURI,
			CandidateArtifactURI: f.artifactURI,
			NodeType:             f.nodeType,
			FilePath:             f.filePath,
			Service:              f.service,
		})
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":       in.ReleaseID,
		"stage":            "validation",
		"reason":           "validation_failed",
		"failing_nodes":    failing,
		"missing_nodes":    []string{}, // dropped-projection nodes are logged, not carried here; kept for payload shape stability
		"aggregate_status": in.AggregateStatus,
		"per_node":         perNode,
		"repo":             r.Repo(),
		"commit_sha":       r.CommitSHA(),
		"code_bundle_uri":  r.CodeBundleURI(),
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

	okCount := len(r.ValidationNodeIDs()) - len(failing)
	d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, false, okCount, len(failing), 0)
	d.Telemetry.ReleaseRejected(ctx, in.ReleaseID, "validation_failed", failing)
	return nil
}
