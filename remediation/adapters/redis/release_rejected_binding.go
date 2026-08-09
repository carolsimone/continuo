package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/domain/failure"
	"github.com/carolsimone/continuo/remediation/service/handlers"
)

// rejectedPayload mirrors the release.rejected:v1 wire shape produced by
// release-controller. Only the fields the classifier needs are decoded.
type rejectedPayload struct {
	ReleaseID string `json:"release_id"`
	Stage     string `json:"stage"`  // "compile" | "seed_build" | "validation"; absent in older payloads
	Reason    string `json:"reason"` // "compile_failed" | "seed_build_failed" | "validation_failed" | "parse_rehearsal_failed" | "artifact_upload_failed"
	Repo      string `json:"repo"`
	CommitSHA string `json:"commit_sha"`
	PerNode   []struct {
		NodeID               string `json:"node_id"`
		Status               string `json:"status"`
		DBTLogURI            string `json:"dbt_log_uri"`
		RunResultsURI        string `json:"run_results_uri"`
		CandidateArtifactURI string `json:"candidate_artifact_uri"`
		// FilePath and Service carry the seed source location from the candidate
		// topology (set by release-controller on seed_build rejections). When
		// present, the remediation agent can locate the source file without
		// querying Ancestry, which only holds promoted topology.
		FilePath string `json:"file_path"`
		Service  string `json:"service"`
	} `json:"per_node"`
}

// sourceFromPayload resolves the remediation Source from a release.rejected
// payload. It prefers the explicit stage field and falls back to the reason
// field for older payloads that carry no stage. The bool is false when the
// rejection is not one of the three remediable pipeline legs — e.g. a
// parse-phase rejection (parse_failed, unbuildable_cross_service_upstream) or
// an unknown future stage; the caller then produces no evidence rather than
// misrouting it onto a leg's classification path.
func sourceFromPayload(stage, reason string) (failure.Source, bool) {
	switch stage {
	case "compile":
		return failure.SourceCompile, true
	case "seed_build":
		return failure.SourceSeed, true
	case "validation":
		return failure.SourceValidation, true
	case "":
		switch reason {
		case "compile_failed":
			return failure.SourceCompile, true
		case "seed_build_failed":
			return failure.SourceSeed, true
		case "validation_failed":
			return failure.SourceValidation, true
		}
	}
	return "", false
}

// evidenceFromRejected translates a release.rejected:v1 payload into one
// FailureEvidence per failed node. Nodes with status != "failed" are skipped.
// The Source field is derived from the payload's stage field; when stage is
// absent (older payloads) the reason field is used as a fallback.
// FilePath is left empty here — it is populated by the ClassifyFailure handler
// once the dbt log has been fetched (task 5.3).
func evidenceFromRejected(raw []byte) ([]failure.FailureEvidence, error) {
	var p rejectedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("unmarshal release.rejected payload: %w", err)
	}

	// Parse-export-leg rejections are not model failures: a rehearsal miss is
	// a project *property* (env_var() at parse time / partial parse disabled)
	// and an upload failure is continuo-internal. Neither is fixable by a
	// model change, so no heal evidence is produced for them.
	if p.Reason == "parse_rehearsal_failed" || p.Reason == "artifact_upload_failed" {
		return nil, nil
	}

	src, ok := sourceFromPayload(p.Stage, p.Reason)
	if !ok {
		// Not a remediable pipeline-leg rejection (e.g. a parse-phase failure):
		// produce no evidence rather than misrouting it onto a leg's path.
		return nil, nil
	}

	out := make([]failure.FailureEvidence, 0, len(p.PerNode))
	for _, n := range p.PerNode {
		if n.Status != "failed" {
			continue
		}
		out = append(out, failure.FailureEvidence{
			Source:               src,
			ReleaseID:            p.ReleaseID,
			NodeID:               n.NodeID,
			DBTLogURI:            n.DBTLogURI,
			RunResultsURI:        n.RunResultsURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Service:              n.Service,
			Repo:                 p.Repo,
			CommitSHA:            p.CommitSHA,
		})
	}
	return out, nil
}

// NewReleaseRejectedConsumer constructs a StreamConsumer that reads
// release.rejected:v1 and classifies each failed node via handlers.ClassifyFailure.
// The consumer group is created idempotently by StreamConsumer.Start; call
// Start(ctx) in a goroutine to begin consuming.
func NewReleaseRejectedConsumer(rc *goredis.Client, deps handlers.Deps, logger *slog.Logger) *pkgredis.StreamConsumer {
	handler := func(ctx context.Context, msg goredis.XMessage) error {
		raw, ok := msg.Values["payload"].(string)
		if !ok {
			logger.Error("release.rejected:v1 missing payload — discarding", "message_id", msg.ID)
			return nil // permanent: ACK by returning nil so the message is not left in the PEL
		}
		evs, err := evidenceFromRejected([]byte(raw))
		if err != nil {
			logger.Error("release.rejected:v1 decode failure — discarding", "message_id", msg.ID, "error", err)
			return nil // permanent: malformed payload cannot be retried
		}
		for _, ev := range evs {
			if err := handlers.ClassifyFailure(ctx, deps, ev); err != nil {
				return err // transient: do not ACK; allow redelivery via PEL sweep
			}
		}
		return nil
	}
	return pkgredis.NewStreamConsumer(
		rc,
		streams.ReleaseRejectedV1,
		streams.RemediationReleaseRejected,
		handler,
		logger,
	)
}
