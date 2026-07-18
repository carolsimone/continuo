package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
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

// compileRejection maps the compile Job's failed container to the reject
// reason and the operator/remediation-facing detail. The parse and upload
// containers are continuo's parse-export leg — their failures must never be
// presented as dbt SQL errors, or the remediation agent is misled into
// proposing a model fix for a problem no model change can solve.
//
// The compile leg enqueues exactly one compile Deployment per release (one
// node, named for the service), and that pod's initContainers run
// sequentially — the first failure terminates the pod, so the "upload" main
// container never runs. A compile pod therefore reports at most one failed
// container, so perNode carries at most one entry with FailedContainer set;
// this loop returning on the first match is not order-dependent in practice.
// The iteration remains defensive for malformed or future multi-entry
// payloads.
func compileRejection(perNode []NodeResult) (reason, errorClass, errorDetail string) {
	for _, n := range perNode {
		switch n.FailedContainer {
		case "parse-prod", "parse-candidate":
			return "parse_rehearsal_failed", "parse_rehearsal_failed",
				"the project re-parses under run-pod conditions — typically an env_var() read at parse time whose value differs between compile and run pods, or partial parse disabled in the project (flags: partial_parse: false / --no-partial-parse); this is not a SQL error"
		case "upload":
			return "artifact_upload_failed", "artifact_upload_failed",
				"internal artifact publication failed; no change to the dbt project will fix this"
		}
	}
	return "compile_failed", "", ""
}

// HandleCompileResult advances a Compiling release once the dbt compile job
// finishes.
//
// ok path: TransitionFromCompiling (Compiling→Parsing), re-assembles the
// manifest-key set from live service_prod, emits release.requested:v1 with
// manifest_keys — payload shape identical to the pre-compile-leg behaviour so
// manifest-controller requires no change.
//
// failed path: TransitionToRejected with a reason derived from the per-node
// failed_container attribution (compile_failed, parse_rehearsal_failed, or
// artifact_upload_failed — see compileRejection), emits release.rejected:v1.
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
	results, failing := stageResults(in.PerNode)

	if in.Status != "ok" {
		if len(in.PerNode) == 0 {
			// A failed compile with no per-node detail (e.g. a producer that
			// predates per-node compile results) still rejects the release, but
			// carries nothing for the remediation classifier to act on.
			d.Logger.Warn("compile failed with no per-node results; release rejected without a remediation trigger",
				"release_id", in.ReleaseID)
		}
		reason, errorClass, errorDetail := compileRejection(in.PerNode)
		if errorClass == "" {
			errorClass, errorDetail = in.ErrorClass, in.ErrorDetail
		}

		r.RecordStageResults("compile", results)
		if err := r.TransitionToRejected(reason, failing, now); err != nil {
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
			"release_id":    in.ReleaseID,
			"stage":         "compile",
			"reason":        reason,
			"error_class":   errorClass,
			"error_detail":  errorDetail,
			"failing_nodes": failing,
			"per_node":      perNode,
			"repo":          r.Repo(),
			"commit_sha":    r.CommitSHA(),
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
		d.Telemetry.ReleaseRejected(ctx, in.ReleaseID, reason, failing)
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
