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

// HandleValidationResultInput carries the terminal validation decision from
// executor-controller's validation.completed:v1 event. Per-node content is not
// carried here: it is projected into the release read model incrementally by the
// validation.node.result:v1 stream and read back from the store when this
// terminal event decides promote-or-reject.
type HandleValidationResultInput struct {
	ReleaseID       string `json:"release_id"`
	AggregateStatus string `json:"aggregate_status"`
}

// HandleValidationResult decides the terminal outcome of a release's validation
// leg from the per-node results the validation.node.result:v1 stream already
// projected into the release read model.
//
// If every validation node passed and the aggregate status is ok: promotes the
// release to production, updates CurrentProd, upserts the changed service's
// service_prod pointer, and emits release.promoted:v1. Otherwise rejects the
// release and emits release.rejected:v1.
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

	// Per-node content lives in the read model, projected incrementally from
	// validation.node.result:v1. Read the validation-stage results the stream
	// already stored rather than re-carrying them on this event.
	stored := map[string]release.NodeValidationResult{}
	for _, n := range r.PerNodeResults() {
		if n.Stage == "validation" {
			stored[n.NodeID] = n
		}
	}

	// Completeness barrier: the decision must see every expected node. If a
	// per-node projection has not landed yet (the terminal event overtook it on a
	// separate stream), return an error so the message redelivers and retries once
	// the stream catches up. Guaranteed to resolve: every node emitted its result
	// before this aggregate was emitted.
	var missing []string
	for _, id := range r.ValidationNodeIDs() {
		if _, ok := stored[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("per-node projection incomplete for %s: awaiting %d node(s) %v",
			in.ReleaseID, len(missing), missing)
	}

	var failing []string
	for _, id := range r.ValidationNodeIDs() {
		if stored[id].Status != "ok" {
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
	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get current prod: %w", err)
	}
	// candidate_sql_uri is transient, release-specific validation data; the promoted
	// topology (current_prod and the release.promoted event) must not carry it.
	promotedTopo := r.CandidateTopology().WithoutCandidateSQLURI()

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
		CanonicalManifestKey(d.Bucket, changed, releaseID),
		r.ImageTags()[changed],
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
		"candidate_schema": "_candidate_" + sanitizeSchemaSuffix(releaseID),
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
		MaxRetries:    3,
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
	d.Telemetry.ReleasePromoted(ctx, in.ReleaseID, len(r.CandidateTopology()))
	return nil
}

// handleValidationFailed records a validation rejection. failing carries the
// validation nodes whose stored per-node outcome was not "ok"; it may be empty
// when only a non-ok aggregate_status triggered the rejection. The failing set
// is persisted on the Release aggregate and surfaced in the outbox payload
// alongside the raw aggregate_status so operators can distinguish why the release
// was rejected. The per-node audit rows are sourced from the read model that
// validation.node.result:v1 projected, not from this terminal event. The
// completeness barrier in HandleValidationResult guarantees every expected node
// is present before this runs, so there are never missing nodes here.
func handleValidationFailed(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, in HandleValidationResultInput, failing []string, now time.Time) error {
	if err := r.TransitionToRejected("validation_failed", failing, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	// Build a map from node UniqueID to CandidateSQLURI so each per-node entry
	// in the rejected payload can carry the S3 pointer to the SQL that was
	// validated — mirroring the dbt_log_uri pattern as a pointer, not inline SQL.
	uriByNodeID := make(map[string]string, len(r.CandidateTopology()))
	for _, n := range r.CandidateTopology() {
		uriByNodeID[n.UniqueID] = n.CandidateSQLURI
	}

	type perNodeEntry struct {
		NodeID          string `json:"node_id"`
		Status          string `json:"status"`
		DBTLogURI       string `json:"dbt_log_uri,omitempty"`
		RunResultsURI   string `json:"run_results_uri,omitempty"`
		CandidateSQLURI string `json:"candidate_sql_uri,omitempty"`
	}
	// Source the per-node audit rows from the projected read model, enriched with
	// each node's candidate SQL pointer from the candidate topology.
	var perNode []perNodeEntry
	for _, nr := range r.PerNodeResults() {
		if nr.Stage != "validation" {
			continue
		}
		perNode = append(perNode, perNodeEntry{
			NodeID:          nr.NodeID,
			Status:          nr.Status,
			DBTLogURI:       nr.DBTLogURI,
			RunResultsURI:   nr.RunResultsURI,
			CandidateSQLURI: uriByNodeID[nr.NodeID],
		})
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":       in.ReleaseID,
		"stage":            "validation",
		"reason":           "validation_failed",
		"failing_nodes":    failing,
		"missing_nodes":    []string{}, // barrier guarantees completeness; kept for payload shape stability
		"aggregate_status": in.AggregateStatus,
		"per_node":         perNode,
		"repo":             r.Repo(),
		"commit_sha":       r.CommitSHA(),
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

	okCount := len(r.ValidationNodeIDs()) - len(failing)
	d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, false, okCount, len(failing), 0)
	d.Telemetry.ReleaseRejected(ctx, in.ReleaseID, "validation_failed", failing)
	return nil
}
