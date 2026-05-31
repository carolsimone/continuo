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

// NodeResult carries the per-node outcome of a dbt validation run.
type NodeResult struct {
	NodeID     string `json:"node_id"`
	Status     string `json:"status"` // "ok" or "failed"
	DBTLogURI  string `json:"dbt_log_uri,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
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
// and emits release.promoted:v1.
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

	now := d.Clock.Now()

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
// current_prod at this release's candidate topology, transitions the release to
// Promoted, persists it, and writes the release.promoted:v1 outbox row. The
// caller owns Begin/Commit and any telemetry. The release must already hold its
// candidate topology (i.e. be in Validating).
func promoteToProduction(ctx context.Context, u uow.UnitOfWork, r *release.Release, releaseID string, now time.Time) error {
	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get current prod: %w", err)
	}
	cp.Update(releaseID, r.ManifestsURI(), r.CandidateTopology(), now)
	if err := u.CurrentProdRepo().Upsert(ctx, cp); err != nil {
		return fmt.Errorf("upsert current prod: %w", err)
	}

	if err := r.TransitionToPromoted(now); err != nil {
		return fmt.Errorf("transition to promoted: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	payload, err := json.Marshal(map[string]any{
		"release_id": releaseID,
		"topology":   r.CandidateTopology(),
		"image_tags": r.ImageTags(),
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
	if err := promoteToProduction(ctx, u, r, in.ReleaseID, now); err != nil {
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

	if err := r.TransitionToRejected("validation_failed", combined, "", now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	type perNodeEntry struct {
		NodeID    string `json:"node_id"`
		Status    string `json:"status"`
		DBTLogURI string `json:"dbt_log_uri,omitempty"`
	}
	perNode := make([]perNodeEntry, len(in.PerNodeResults))
	for i, nr := range in.PerNodeResults {
		perNode[i] = perNodeEntry{
			NodeID:    nr.NodeID,
			Status:    nr.Status,
			DBTLogURI: nr.DBTLogURI,
		}
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":       in.ReleaseID,
		"reason":           "validation_failed",
		"failing_nodes":    failing,
		"missing_nodes":    missing,
		"aggregate_status": in.AggregateStatus,
		"per_node":         perNode,
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
