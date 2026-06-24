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

// evidenceFromRejected translates a release.rejected:v1 payload into one
// FailureEvidence per failed node. Nodes with status != "failed" are skipped.
func evidenceFromRejected(raw []byte) ([]failure.FailureEvidence, error) {
	var p rejectedPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("unmarshal release.rejected payload: %w", err)
	}
	out := make([]failure.FailureEvidence, 0, len(p.PerNode))
	for _, n := range p.PerNode {
		if n.Status != "failed" {
			continue
		}
		out = append(out, failure.FailureEvidence{
			Source:          failure.SourceValidation,
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
