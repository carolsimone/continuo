package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
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
	if err := d.UoW.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer d.UoW.Rollback() //nolint:errcheck

	r, err := d.UoW.ReleaseRepo().Get(ctx, in.ReleaseID)
	if err != nil {
		return fmt.Errorf("get release: %w", err)
	}

	now := d.Clock.Now()

	var failing []string
	for _, nr := range in.PerNodeResults {
		if nr.Status != "ok" {
			failing = append(failing, nr.NodeID)
		}
	}

	if len(failing) == 0 {
		return handleValidationOK(ctx, d, r, in, now)
	}
	return handleValidationFailed(ctx, d, r, in, failing, now)
}

func handleValidationOK(ctx context.Context, d *Deps, r *release.Release, in HandleValidationResultInput, now time.Time) error {
	cp, err := d.UoW.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get current prod: %w", err)
	}
	cp.Update(in.ReleaseID, r.CandidateTopology(), now)
	if err := d.UoW.CurrentProdRepo().Upsert(ctx, cp); err != nil {
		return fmt.Errorf("upsert current prod: %w", err)
	}

	if err := r.TransitionToPromoted(now); err != nil {
		return fmt.Errorf("transition to promoted: %w", err)
	}
	if err := d.UoW.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	payload, err := json.Marshal(map[string]any{
		"release_id": in.ReleaseID,
		"topology":   r.CandidateTopology(),
		"image_tags": r.ImageTags(),
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := d.UoW.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(in.ReleaseID),
		EventType:     "release_promoted",
		Payload:       payload,
		StreamName:    streams.ReleasePromotedV1,
		Status:        "pending",
		MaxRetries:    3,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}

	if err := d.UoW.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, true, len(in.PerNodeResults), 0, 0)
	d.Telemetry.ReleasePromoted(ctx, in.ReleaseID, len(r.CandidateTopology()))
	return nil
}

func handleValidationFailed(ctx context.Context, d *Deps, r *release.Release, in HandleValidationResultInput, failing []string, now time.Time) error {
	if err := r.TransitionToRejected("validation_failed", failing, "", now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := d.UoW.ReleaseRepo().Save(ctx, r); err != nil {
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
		"release_id":    in.ReleaseID,
		"reason":        "validation_failed",
		"failing_nodes": failing,
		"per_node":      perNode,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := d.UoW.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
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

	if err := d.UoW.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	okCount := len(in.PerNodeResults) - len(failing)
	d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, false, okCount, len(failing), 0)
	d.Telemetry.ReleaseRejected(ctx, in.ReleaseID, "validation_failed", failing)
	return nil
}
