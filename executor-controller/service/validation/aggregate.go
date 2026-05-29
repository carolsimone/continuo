// Package validation holds the shared, infrastructure-free gate that emits the
// per-release validation.completed:v1 aggregate once every mode=validation node
// for a release has reached a terminal outcome. It is used by two call sites:
// the deploy dispatcher (when a node fails AT dispatch) and the
// validation.node.completed:v1 handler (when a node terminates after dispatch).
// Both must run identical logic and share one immutable dedup namespace, so the
// gate lives here, depending only on domain ports and pkg primitives — never on
// any adapters/* package.
package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// DedupNamespace seeds the deterministic aggregate_id for a release's
// validation.completed:v1 outbox row so a re-emission (e.g. a retried batch that
// re-wins the sentinel after a crash between INSERT and commit) deduplicates to
// one published event in the consumer. IMMUTABLE: changing it would let an
// already-published aggregate re-publish under a new id. This is the single
// source of the namespace for every emit call site.
var DedupNamespace = uuid.MustParse("e2a9c7d4-3f1b-4a8e-9c5d-1b6f0a2e8d7c")

// EmitValidationAggregateIfComplete emits validation.completed:v1 for releaseID
// once every mode=validation row for that release has reached a terminal
// outcome. It is a no-op while any node is still pending. The sentinel
// ClaimEmission ensures exactly one of the racing callers writes the outbox row;
// losers return without emitting. The aggregate_status is "ok" iff every per-node
// outcome is "ok".
//
// namespace is passed in (rather than read from the package var directly) so the
// caller threads the single immutable source explicitly; pass DedupNamespace.
func EmitValidationAggregateIfComplete(
	ctx context.Context,
	depRepo repository.DeploymentRepository,
	outboxRepo outbox.Repository,
	aggRepo repository.ValidationAggregateRepository,
	namespace uuid.UUID,
	releaseID string,
	now time.Time,
) error {
	pending, err := depRepo.PendingValidationCount(ctx, releaseID)
	if err != nil {
		return fmt.Errorf("pending validation count: %w", err)
	}
	if pending > 0 {
		return nil
	}

	won, err := aggRepo.ClaimEmission(ctx, releaseID, now)
	if err != nil {
		return fmt.Errorf("claim validation aggregate emission: %w", err)
	}
	if !won {
		return nil
	}

	results, err := depRepo.ListValidationResults(ctx, releaseID)
	if err != nil {
		return fmt.Errorf("list validation results: %w", err)
	}

	perNode := make([]map[string]any, 0, len(results))
	aggregate := "ok"
	for _, r := range results {
		node := map[string]any{
			"node_id": r.NodeID(),
			"status":  r.Outcome(),
		}
		if uri := r.DBTLogURI(); uri != "" { // omitempty: absent when no log was produced
			node["dbt_log_uri"] = uri
		}
		perNode = append(perNode, node)
		if r.Outcome() != "ok" {
			aggregate = "failed"
		}
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":       releaseID,
		"per_node_results": perNode,
		"aggregate_status": aggregate,
	})
	if err != nil {
		return fmt.Errorf("marshal aggregate payload: %w", err)
	}

	aggID := uuid.NewSHA1(namespace, []byte("release:"+releaseID))
	return outboxRepo.Create(ctx, &outbox.Entry{
		AggregateType: "release",
		AggregateID:   aggID,
		EventType:     "validation_completed",
		Payload:       payload,
		StreamName:    streams.ValidationCompletedV1,
		MaxRetries:    3,
	})
}
