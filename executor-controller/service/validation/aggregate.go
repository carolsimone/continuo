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

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// EventTypeValidationCompleted is the canonical outbox event_type string for
// the validation.completed:v1 aggregate event. Defined here (service layer) so
// both the emit site (aggregate.go) and the publisher adapter (adapters/publisher)
// share one source of truth without requiring the service to import an adapter.
const EventTypeValidationCompleted = "validation_completed"

// EventTypeSeedBuildCompleted is the canonical outbox event_type string for
// the seed.build.completed:v1 aggregate event.
const EventTypeSeedBuildCompleted = "seed_build_completed"

// EventTypeCompileCompleted is the canonical outbox event_type string for
// the compile.completed:v1 aggregate event.
const EventTypeCompileCompleted = "compile_completed"

// DedupNamespace seeds the deterministic aggregate_id for a release's
// validation.completed:v1 outbox row so a re-emission (e.g. a retried batch that
// re-wins the sentinel after a crash between INSERT and commit) deduplicates to
// one published event in the consumer. IMMUTABLE: changing it would let an
// already-published aggregate re-publish under a new id. This is the single
// source of the namespace for every validation emit call site.
var DedupNamespace = uuid.MustParse("e2a9c7d4-3f1b-4a8e-9c5d-1b6f0a2e8d7c")

// SeedBuildDedupNamespace is the seed-build leg's equivalent of DedupNamespace.
// It MUST be distinct from DedupNamespace so the seed-build and validation legs
// of one release derive different outbox aggregate_ids (they share release_id);
// the consumer-side dedup then treats the two legs' emissions independently.
// IMMUTABLE for the same reason as DedupNamespace.
var SeedBuildDedupNamespace = uuid.MustParse("b7f3d1a8-6c20-4e9d-8a14-2f5e0c9b4a13")

// CompileDedupNamespace is the compile leg's equivalent of DedupNamespace.
// It MUST be distinct from both DedupNamespace (validation) and
// SeedBuildDedupNamespace (seed-build) so the three legs of one release derive
// different outbox aggregate_ids (they share release_id); the consumer-side
// dedup then treats the three legs' emissions independently.
// IMMUTABLE for the same reason as DedupNamespace.
var CompileDedupNamespace = uuid.MustParse("c4e2a1f7-8b3d-4c9e-a0f1-5d8b2c7e4a91")

// emitConfig parametrizes the shared aggregate-emit gate per leg. The validation
// and seed-build legs share one machinery and one sentinel table, differing only
// in the stream/event they publish, the dedup namespace, the payload shape, and
// the mode the repo scopes its counts/lists to.
type emitConfig struct {
	streamName string
	eventType  string
	namespace  uuid.UUID
	mode       model.Mode
}

var validationEmit = emitConfig{
	streamName: streams.ValidationCompletedV1,
	eventType:  EventTypeValidationCompleted,
	namespace:  DedupNamespace,
	mode:       model.ModeValidation,
}

var seedBuildEmit = emitConfig{
	streamName: streams.SeedBuildCompletedV1,
	eventType:  EventTypeSeedBuildCompleted,
	namespace:  SeedBuildDedupNamespace,
	mode:       model.ModeSeedBuild,
}

// compileEmit is the compile leg's emitConfig. It emits compile.completed:v1
// once the per-release compile node settles, scoped to ModeCompile so it never
// interferes with the co-existing validation/seed-build legs of the same release.
var compileEmit = emitConfig{
	streamName: streams.CompileCompletedV1,
	eventType:  EventTypeCompileCompleted,
	namespace:  CompileDedupNamespace,
	mode:       model.ModeCompile,
}

// EmitValidationAggregateIfComplete emits validation.completed:v1 for releaseID
// once every mode=validation row for that release has reached a terminal
// outcome. It is a no-op while any node is still pending. It first takes a
// per-release transaction advisory lock so the count -> claim -> emit sequence
// is serialized across concurrent transactions, then the sentinel ClaimEmission
// ensures exactly one of the racing callers writes the outbox row; losers return
// without emitting. Together they make emission exactly-once — never zero, never
// double. The aggregate_status is "ok" iff every per-node outcome is "ok".
//
// It runs inside the caller's transaction (the dispatch tx or the
// node-completed UoW); the advisory lock auto-releases at that transaction's
// commit/rollback.
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
	cfg := validationEmit
	cfg.namespace = namespace // caller threads the single immutable source explicitly
	// Serialize the whole count -> claim -> emit sequence per (release, leg).
	// Without this lock, two overlapping last-node terminals (e.g. the
	// dispatcher's FailValidation-at-dispatch path racing the
	// validation.node.completed handler, or two replicas) can each read the other
	// node as still pending under READ COMMITTED and both no-op, leaving the
	// release hung in "validating" with no aggregate emitted. The advisory lock
	// is transaction-scoped: the second caller blocks here until the first
	// commits, then sees pending==0 and either wins the sentinel or loses cleanly.
	if err := aggRepo.LockRelease(ctx, releaseID, cfg.mode); err != nil {
		return fmt.Errorf("lock release for aggregate gate: %w", err)
	}
	return emitAggregateIfComplete(ctx, depRepo, outboxRepo, aggRepo, cfg, releaseID, now)
}

// EmitSeedBuildAggregateIfComplete is the seed-build leg's equivalent of
// EmitValidationAggregateIfComplete: it emits seed.build.completed:v1 for
// releaseID once every mode=seed_build row for that release has reached a
// terminal outcome, scoped entirely to ModeSeedBuild so a co-existing validation
// leg for the same release never affects it. Seeds are flat roots (no in-leg
// upstreams), so there is no gating to propagate — the gate is just
// count -> claim -> emit under the per-(release, leg) advisory lock. Unlike the
// validation aggregate, the emission does NOT trigger any candidate-schema
// teardown: the candidate schema must survive into the validation leg.
func EmitSeedBuildAggregateIfComplete(
	ctx context.Context,
	depRepo repository.DeploymentRepository,
	outboxRepo outbox.Repository,
	aggRepo repository.ValidationAggregateRepository,
	releaseID string,
	now time.Time,
) error {
	if err := aggRepo.LockRelease(ctx, releaseID, seedBuildEmit.mode); err != nil {
		return fmt.Errorf("lock release for seed-build aggregate gate: %w", err)
	}
	return emitAggregateIfComplete(ctx, depRepo, outboxRepo, aggRepo, seedBuildEmit, releaseID, now)
}

// emitAggregateIfComplete is the post-lock body of the aggregate-emit gate. It
// assumes the per-release advisory lock is ALREADY held (taken by the public
// wrapper or by SettleNodeTerminal) and must never take it itself, so a settle
// that already locked once does not double-lock. It counts pending nodes, claims
// the emission sentinel, and writes the validation.completed:v1 outbox row.
func emitAggregateIfComplete(
	ctx context.Context,
	depRepo repository.DeploymentRepository,
	outboxRepo outbox.Repository,
	aggRepo repository.ValidationAggregateRepository,
	cfg emitConfig,
	releaseID string,
	now time.Time,
) error {
	pending, err := depRepo.PendingValidationCount(ctx, releaseID, cfg.mode)
	if err != nil {
		return fmt.Errorf("pending %s count: %w", cfg.mode, err)
	}
	if pending > 0 {
		return nil
	}

	won, err := aggRepo.ClaimEmission(ctx, releaseID, cfg.mode, now)
	if err != nil {
		return fmt.Errorf("claim %s aggregate emission: %w", cfg.mode, err)
	}
	if !won {
		return nil
	}

	results, err := depRepo.ListValidationResults(ctx, releaseID, cfg.mode)
	if err != nil {
		return fmt.Errorf("list %s results: %w", cfg.mode, err)
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
		if uri := r.DBTRunResultsURI(); uri != "" { // omitempty: absent when no structured block
			node["run_results_uri"] = uri
		}
		perNode = append(perNode, node)
		if r.Outcome() != "ok" {
			aggregate = "failed"
		}
	}

	candidateSchema := ""
	if len(results) > 0 {
		candidateSchema = results[0].ValidationCommand().CandidateSchema
	}

	payload, err := json.Marshal(aggregatePayload(cfg.mode, releaseID, perNode, aggregate, candidateSchema))
	if err != nil {
		return fmt.Errorf("marshal aggregate payload: %w", err)
	}

	aggID := uuid.NewSHA1(cfg.namespace, []byte("release:"+releaseID))
	return outboxRepo.Create(ctx, &outbox.Entry{
		AggregateType: "release",
		AggregateID:   aggID,
		EventType:     cfg.eventType,
		Payload:       payload,
		StreamName:    cfg.streamName,
		MaxRetries:    3,
	})
}

// aggregatePayload builds the leg-specific completion payload. The validation
// leg's shape is preserved byte-for-byte ({release_id, per_node_results,
// aggregate_status, candidate_schema}); the seed-build leg uses release-
// controller's HandleSeedBuildResult contract ({release_id, status, per_node,
// candidate_schema} — it reads only release_id + status, the rest is symmetry).
// The compile leg shares the seed-build shape: release-controller's
// HandleCompileResultInput reads release_id + status under the "status" key, so
// compile MUST emit "status" (not "aggregate_status") — otherwise the consumer
// decodes Status as "" and treats every compile (even a successful one) as a
// failure, rejecting the release.
func aggregatePayload(mode model.Mode, releaseID string, perNode []map[string]any, aggregate, candidateSchema string) map[string]any {
	if mode == model.ModeSeedBuild || mode == model.ModeCompile {
		return map[string]any{
			"release_id":       releaseID,
			"status":           aggregate,
			"per_node":         perNode,
			"candidate_schema": candidateSchema,
		}
	}
	return map[string]any{
		"release_id":       releaseID,
		"per_node_results": perNode,
		"aggregate_status": aggregate,
		"candidate_schema": candidateSchema,
	}
}
