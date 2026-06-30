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
	Reason    string `json:"reason"` // "compile_failed" | "seed_build_failed" | "validation_failed"
	Repo      string `json:"repo"`
	CommitSHA string `json:"commit_sha"`
	PerNode   []struct {
		NodeID          string `json:"node_id"`
		Status          string `json:"status"`
		DBTLogURI       string `json:"dbt_log_uri"`
		RunResultsURI   string `json:"run_results_uri"`
		CandidateSQLURI string `json:"candidate_sql_uri"`
	} `json:"per_node"`
}

// stageToSource maps the pipeline stage string from the payload to the
// domain Source constant. Unknown values default to SourceValidation so that
// the classifier degrades gracefully rather than dropping a failure.
func stageToSource(stage string) failure.Source {
	switch stage {
	case "compile":
		return failure.SourceCompile
	case "seed_build":
		return failure.SourceSeed
	default:
		return failure.SourceValidation
	}
}

// reasonToStage maps a legacy reason field to the equivalent stage string so
// that older payloads without an explicit stage field are handled identically
// to newer ones.
func reasonToStage(reason string) string {
	switch reason {
	case "compile_failed":
		return "compile"
	case "seed_build_failed":
		return "seed_build"
	default:
		return "validation"
	}
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

	stage := p.Stage
	if stage == "" {
		stage = reasonToStage(p.Reason)
	}
	src := stageToSource(stage)

	out := make([]failure.FailureEvidence, 0, len(p.PerNode))
	for _, n := range p.PerNode {
		if n.Status != "failed" {
			continue
		}
		out = append(out, failure.FailureEvidence{
			Source:          src,
			ReleaseID:       p.ReleaseID,
			NodeID:          n.NodeID,
			DBTLogURI:       n.DBTLogURI,
			RunResultsURI:   n.RunResultsURI,
			CandidateSQLURI: n.CandidateSQLURI,
			Repo:            p.Repo,
			CommitSHA:       p.CommitSHA,
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
