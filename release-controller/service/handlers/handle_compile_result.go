package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/google/uuid"
)

// HandleCompileResultInput carries the aggregated compile outcome from
// executor-controller (compile.completed:v1).
type HandleCompileResultInput struct {
	ReleaseID   string       `json:"release_id"`
	Status      string       `json:"status"` // "ok" | "failed"
	PerNode     []NodeResult `json:"per_node"`
	ErrorClass  string       `json:"error_class,omitempty"`
	ErrorDetail string       `json:"error_detail,omitempty"`
}

// manifestKeyDTO is the wire shape for one service's manifest entry in the
// release.requested:v1 payload. Kept here (single definition) and used by
// both HandleCompileResult and any caller that needs to assemble the payload.
type manifestKeyDTO struct {
	Service string `json:"service"`
	S3URI   string `json:"s3_uri"`
}

// releaseRequestedPayload is the exact wire shape of release.requested:v1 as
// consumed by manifest-controller. The shape must not change.
type releaseRequestedPayload struct {
	ReleaseID    string           `json:"release_id"`
	ManifestKeys []manifestKeyDTO `json:"manifest_keys"`
}

// HandleCompileResult advances a Compiling release once the dbt compile job
// finishes.
//
// ok path: TransitionFromCompiling (Compiling→Parsing), re-assembles the
// manifest-key set from live service_prod, emits release.requested:v1 with
// manifest_keys — payload shape identical to the pre-compile-leg behaviour so
// manifest-controller requires no change.
//
// failed path: TransitionToRejected("compile_failed"), emits release.rejected:v1.
//
// unknown release: drops the message (ack).
func HandleCompileResult(ctx context.Context, d *Deps, in HandleCompileResultInput) error {
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
		d.Logger.Warn("compile result for unknown release; dropping", "release_id", in.ReleaseID)
		return nil
	}

	now := d.Clock.Now()

	// Build per-node results and derive the failing set for both branches.
	results := make([]release.NodeValidationResult, len(in.PerNode))
	var failing []string
	for i, n := range in.PerNode {
		results[i] = release.NodeValidationResult{
			NodeID:        n.NodeID,
			Status:        n.Status,
			DBTLogURI:     n.DBTLogURI,
			RunResultsURI: n.RunResultsURI,
		}
		if n.Status != "ok" {
			failing = append(failing, n.NodeID)
		}
	}

	if in.Status != "ok" {
		r.RecordStageResults("compile", results)
		if err := r.TransitionToRejected("compile_failed", failing, now); err != nil {
			return fmt.Errorf("transition to rejected: %w", err)
		}
		if err := u.ReleaseRepo().Save(ctx, r); err != nil {
			return fmt.Errorf("save release: %w", err)
		}

		// perNodeEntry is the outbox wire shape for a single compile-leg result.
		// Intentionally omits duration_ms (irrelevant for compile) and file_path
		// (populated by the remediation service when it reads S3 logs).
		type perNodeEntry struct {
			NodeID        string `json:"node_id"`
			Status        string `json:"status"`
			DBTLogURI     string `json:"dbt_log_uri,omitempty"`
			RunResultsURI string `json:"run_results_uri,omitempty"`
		}
		perNode := make([]perNodeEntry, len(in.PerNode))
		for i, n := range in.PerNode {
			perNode[i] = perNodeEntry{
				NodeID:        n.NodeID,
				Status:        n.Status,
				DBTLogURI:     n.DBTLogURI,
				RunResultsURI: n.RunResultsURI,
			}
		}

		payload, err := json.Marshal(map[string]any{
			"release_id":   in.ReleaseID,
			"stage":        "compile",
			"reason":       "compile_failed",
			"error_class":  in.ErrorClass,
			"error_detail": in.ErrorDetail,
			"failing_nodes": failing,
			"per_node":     perNode,
			"repo":         r.Repo(),
			"commit_sha":   r.CommitSHA(),
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
		d.Telemetry.ReleaseRejected(ctx, in.ReleaseID, "compile_failed", failing)
		return nil
	}

	// ok path: Compiling → Parsing, re-read live service_prod, emit release.requested.
	// Record compile results on the ok path too so the UI can surface
	// "Compilation: success" for each service unit.
	r.RecordStageResults("compile", results)
	if err := r.TransitionFromCompiling(now); err != nil {
		return fmt.Errorf("transition from compiling: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	// Re-read live service_prod so the manifest-key set reflects any promotions
	// that happened while this release was compiling — same "read live state when
	// proceeding" rationale as AdvanceQueue.
	pointers, err := u.ServiceProdRepo().List(ctx)
	if err != nil {
		return fmt.Errorf("list service prod: %w", err)
	}
	imageTag := r.ImageTags()[r.ChangedService()]
	set := AssembleManifestSet(pointers, d.Bucket, r.ChangedService(), in.ReleaseID, imageTag)

	keys := make([]manifestKeyDTO, len(set.ManifestKeys))
	for i, k := range set.ManifestKeys {
		keys[i] = manifestKeyDTO{Service: k.Service, S3URI: k.S3URI}
	}
	releasePayload, err := json.Marshal(releaseRequestedPayload{
		ReleaseID:    in.ReleaseID,
		ManifestKeys: keys,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(in.ReleaseID),
		EventType:     "release_requested",
		Payload:       releasePayload,
		StreamName:    streams.ReleaseRequestedV1,
		Status:        "pending",
		MaxRetries:    3,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseParseRequested(ctx, in.ReleaseID)
	return nil
}
