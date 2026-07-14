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

// NodeResult is the wire-input counterpart of the domain value object
// release.NodeValidationResult. It is intentionally kept separate (rather than
// reusing the domain type) so the inbound transport shape stays decoupled from
// the domain: the handler maps NodeResult → release.NodeValidationResult before
// recording it on the aggregate. The outbox payload (perNodeEntry) is likewise a
// distinct boundary DTO that deliberately omits duration_ms.
type NodeResult struct {
	NodeID        string `json:"node_id"`
	Status        string `json:"status"` // "ok" or "failed"
	DBTLogURI     string `json:"dbt_log_uri,omitempty"`
	RunResultsURI string `json:"run_results_uri,omitempty"`
	DurationMS    int64  `json:"duration_ms,omitempty"`
}

// HandleValidationResultInput carries the aggregated validation outcome from
// executor-controller.
type HandleValidationResultInput struct {
	ReleaseID       string       `json:"release_id"`
	PerNodeResults  []NodeResult `json:"per_node_results"`
	AggregateStatus string       `json:"aggregate_status"`
}

// HandleValidationResult processes the per-node validation results received from
// executor-controller.
//
// If every node passed: promotes the release to production, updates CurrentProd,
// upserts the changed service's service_prod pointer, and emits release.promoted:v1.
// If any node failed: rejects the release and emits release.rejected:v1.
func HandleValidationResult(ctx context.Context, d *Deps, in HandleValidationResultInput) error {
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
		// The release referenced by this result no longer exists (e.g. it was
		// pruned, or this is a stale/duplicate message reclaimed from a previous
		// consumer for a release that was deleted). There is nothing to promote
		// or reject; ack and drop rather than dereference a nil aggregate.
		d.Logger.Warn("validation result for unknown release; dropping",
			"release_id", in.ReleaseID)
		return nil
	}

	now := d.Clock.Now()

	results := make([]release.NodeValidationResult, len(in.PerNodeResults))
	for i, n := range in.PerNodeResults {
		results[i] = release.NodeValidationResult{
			NodeID:        n.NodeID,
			Status:        n.Status,
			DBTLogURI:     n.DBTLogURI,
			RunResultsURI: n.RunResultsURI,
			DurationMS:    n.DurationMS,
		}
	}
	r.RecordValidationResults(results)

	seen := map[string]string{}
	for _, n := range in.PerNodeResults {
		seen[n.NodeID] = n.Status
	}

	var failing []string
	for _, n := range in.PerNodeResults {
		if n.Status != "ok" {
			failing = append(failing, n.NodeID)
		}
	}

	var missing []string
	for _, id := range r.ValidationNodeIDs() {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}

	aggregateOK := in.AggregateStatus == "ok"

	if len(failing) > 0 || len(missing) > 0 || !aggregateOK {
		return handleValidationFailed(ctx, d, u, r, in, failing, missing, now)
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
	d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, true, len(in.PerNodeResults), 0, 0)
	d.Telemetry.ReleasePromoted(ctx, in.ReleaseID, len(r.CandidateTopology()))
	return nil
}

// handleValidationFailed records a validation rejection. failing carries
// explicit per-node failures from PerNodeResults; missing carries nodes the
// release expected but executor-controller never reported. Both are persisted
// on the Release aggregate (as the combined failing_nodes audit set) and
// surfaced separately in the outbox payload alongside the raw aggregate_status
// so operators can distinguish why the release was rejected — including the
// case where neither list has entries and only a non-ok aggregate_status
// triggered the rejection.
func handleValidationFailed(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, in HandleValidationResultInput, failing, missing []string, now time.Time) error {
	combined := append([]string{}, failing...)
	combined = append(combined, missing...)

	if err := r.TransitionToRejected("validation_failed", combined, now); err != nil {
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
	perNode := make([]perNodeEntry, len(in.PerNodeResults))
	for i, nr := range in.PerNodeResults {
		perNode[i] = perNodeEntry{
			NodeID:          nr.NodeID,
			Status:          nr.Status,
			DBTLogURI:       nr.DBTLogURI,
			RunResultsURI:   nr.RunResultsURI,
			CandidateSQLURI: uriByNodeID[nr.NodeID],
		}
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":       in.ReleaseID,
		"stage":            "validation",
		"reason":           "validation_failed",
		"failing_nodes":    failing,
		"missing_nodes":    missing,
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

	okCount := len(in.PerNodeResults) - len(failing)
	d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, false, okCount, len(combined), 0)
	d.Telemetry.ReleaseRejected(ctx, in.ReleaseID, "validation_failed", combined)
	return nil
}
