package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// HandleParsedManifestInput carries the result of the manifest-controller
// parsing a candidate release. Status must be "ok" or "failed".
type HandleParsedManifestInput struct {
	ReleaseID   string           `json:"release_id"`
	Status      string           `json:"status"` // "ok" or "failed"
	Topology    release.Topology `json:"topology,omitempty"`
	ErrorClass  string           `json:"error_class,omitempty"`
	ErrorDetail string           `json:"error_detail,omitempty"`
}

// HandleParsedManifest handles the manifest parse result from manifest-controller.
//
// On failure: transitions the release to Rejected and emits release.rejected:v1.
// On success: joins image tags into the topology, computes the validation closure,
// transitions to Validating, and emits validation.requested:v1.
func HandleParsedManifest(ctx context.Context, d *Deps, in HandleParsedManifestInput) error {
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
		// The release referenced by this parse result no longer exists (pruned,
		// or a stale/duplicate message reclaimed for a deleted release). Ack and
		// drop rather than dereference a nil aggregate.
		d.Logger.Warn("parsed manifest for unknown release; dropping",
			"release_id", in.ReleaseID)
		return nil
	}

	now := d.Clock.Now()

	if in.Status == "failed" {
		return handleParseFailed(ctx, d, u, r, in, now)
	}
	return handleParseOK(ctx, d, u, r, in, now)
}

func handleParseFailed(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, in HandleParsedManifestInput, now time.Time) error {
	if err := r.TransitionToRejected("parse_failed", nil, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	payload, err := json.Marshal(map[string]string{
		"release_id":   in.ReleaseID,
		"reason":       "parse_failed",
		"error_class":  in.ErrorClass,
		"error_detail": in.ErrorDetail,
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
	d.Telemetry.ReleaseParseCompleted(ctx, in.ReleaseID, false, 0)
	d.Telemetry.ReleaseRejected(ctx, in.ReleaseID, "parse_failed", nil)
	return nil
}

func handleParseOK(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, in HandleParsedManifestInput, now time.Time) error {
	topo := joinImageTags(in.Topology, r.ImageTags())

	// A bootstrap release skips validation entirely: it seeds current_prod and
	// swaps topology directly. This is the one-time cutover (or a trusted
	// re-baseline) where current_prod is empty/mismatched and normal validation
	// would reject every cross-service upstream as new.
	if r.IsBootstrap() {
		return promoteBootstrap(ctx, d, u, r, in.ReleaseID, topo, now)
	}

	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get current prod: %w", err)
	}

	// Derive the validation seed set from the content_hash diff against the
	// current prod topology: candidate nodes that are new or whose hash changed.
	// Bootstrap (no prod row) yields an empty prod snapshot, so every candidate
	// node is treated as new and the whole topology is validated.
	changed := release.DerivedChangedNodeIDs(topo, cp.TopologySnapshot())

	// Validate the changed-and-downstream closure plus the FULL transitive
	// upstream closure (across service boundaries) so every node's refs — intra-
	// service ref()s and cross-service {{ xschema() }} refs alike — resolve inside
	// the candidate schema. Upstreams build --empty first; the executor gates on them.
	changedClosure := release.DescendantsClosure(topo, changed)
	validationIDs := unionSorted(changedClosure, release.FullAncestorsClosure(topo, changedClosure))

	// Every node in the validation build set must have all its upstreams present
	// in the candidate topology; an upstream absent from it cannot be built into
	// the candidate schema and would fail the dbt --empty run with a missing
	// relation. Reject early with a clear, actionable message instead.
	if edges := release.UnbuildableCrossServiceUpstreams(topo, validationIDs); len(edges) > 0 {
		return rejectUnbuildableCrossServiceUpstream(ctx, d, u, r, in.ReleaseID, edges, now)
	}

	if err := r.TransitionToValidating(topo, validationIDs, now); err != nil {
		return fmt.Errorf("transition to validating: %w", err)
	}

	// Nothing to validate: no candidate node is new or content-changed vs prod
	// (e.g. a release that only bumps image tags, or removes a node). Emitting an
	// empty validation.requested would be rejected by executor-controller as a
	// permanent parse error, so no validation.completed would ever arrive and the
	// release would block the queue indefinitely. Promote directly instead — an
	// empty candidate diff trivially passes the gate.
	if len(validationIDs) == 0 {
		if err := promoteToProduction(ctx, u, r, in.ReleaseID, now); err != nil {
			return err
		}
		if err := u.Commit(); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
		// Emit the full lifecycle span sequence (parsed → validated-0-nodes →
		// promoted) so this no-validation-needed promotion is observable the same
		// way a normal pass is, just with a zero-node validation.
		d.Telemetry.ReleaseParseCompleted(ctx, in.ReleaseID, true, 0)
		d.Telemetry.ReleaseValidationCompleted(ctx, in.ReleaseID, true, 0, 0, 0)
		d.Telemetry.ReleasePromoted(ctx, in.ReleaseID, len(topo))
		return nil
	}

	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	candidateSchema := "_candidate_" + sanitizeSchemaSuffix(in.ReleaseID)

	inSet := make(map[string]bool, len(validationIDs))
	for _, id := range validationIDs {
		inSet[id] = true
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":        in.ReleaseID,
		"mode":              "validation",
		"nodes":             validationNodesInOrder(topo, validationIDs, inSet),
		"node_ids_in_order": validationIDs,
		"image_tags":        r.ImageTags(),
		"candidate_schema":  candidateSchema,
		"dbt_flags":         []string{"--empty"},
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(in.ReleaseID),
		EventType:     "validation_requested",
		Payload:       payload,
		StreamName:    streams.ValidationRequestedV1,
		Status:        "pending",
		MaxRetries:    3,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseParseCompleted(ctx, in.ReleaseID, true, 0)
	d.Telemetry.ReleaseValidationRequested(ctx, in.ReleaseID, len(validationIDs))
	return nil
}

// promoteBootstrap promotes a bootstrap release without validation: it records
// the candidate topology (TransitionToValidating with no validation nodes) and
// runs the shared promoteToProduction path, which seeds current_prod and emits
// release.promoted:v1. The full parse/validated/promoted telemetry span is
// emitted (with a zero-node validation) so a bootstrap is observable like any
// other promotion.
func promoteBootstrap(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, releaseID string, topo release.Topology, now time.Time) error {
	// TransitionToValidating records the candidate topology and satisfies
	// promoteToProduction's Validating precondition; no nodes are submitted to
	// validation.
	if err := r.TransitionToValidating(topo, nil, now); err != nil {
		return fmt.Errorf("transition to validating (bootstrap): %w", err)
	}
	if err := promoteToProduction(ctx, u, r, releaseID, now); err != nil {
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	// okCount is 0: a bootstrap validates no nodes (consistent with the
	// no-validation promote path's zero-node validation span).
	d.Telemetry.ReleaseParseCompleted(ctx, releaseID, true, 0)
	d.Telemetry.ReleaseValidationCompleted(ctx, releaseID, true, 0, 0, 0)
	d.Telemetry.ReleasePromoted(ctx, releaseID, len(topo))
	return nil
}

// joinImageTags returns a new Topology with ImageTag populated on each node
// whose ServiceName has a matching entry in imageTags.
func joinImageTags(topo release.Topology, imageTags map[string]string) release.Topology {
	result := make(release.Topology, len(topo))
	for i, n := range topo {
		if tag, ok := imageTags[n.ServiceName]; ok {
			n.ImageTag = tag
		}
		result[i] = n
	}
	return result
}

// validationNodesInOrder returns one map per validation node in lexical
// (sorted) order, carrying the per-node fields executor-controller needs to
// build a candidate dbt job. upstream_node_ids lists the in-set upstreams (intra-
// AND cross-service) that must succeed before this node can run its candidate
// schema build. Dispatch ordering is deterministic but NOT topological; per-node
// execution sequencing is enforced at runtime by the executor's gating on
// upstream_node_ids, not by position in this list.
func validationNodesInOrder(topo release.Topology, validationIDs []string, inSet map[string]bool) []map[string]any {
	byID := make(map[string]release.Node, len(topo))
	for _, n := range topo {
		byID[n.UniqueID] = n
	}
	out := make([]map[string]any, 0, len(validationIDs))
	for _, id := range validationIDs {
		n, ok := byID[id]
		if !ok {
			continue
		}
		out = append(out, map[string]any{
			"unique_id":         n.UniqueID,
			"service_name":      n.ServiceName,
			"node_type":         n.NodeType,
			"schema_name":       n.SchemaName,
			"table_name":        n.TableName,
			"image_tag":         n.ImageTag,
			"upstream_node_ids": release.InSetUpstreams(topo, id, inSet),
		})
	}
	return out
}

var nonAlphanumUnderscore = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// sanitizeSchemaSuffix replaces any character that is not [a-zA-Z0-9_] with _.
// Used to derive a safe Postgres schema name suffix from a release_id.
func sanitizeSchemaSuffix(s string) string {
	return nonAlphanumUnderscore.ReplaceAllString(s, "_")
}

// unionSorted merges two ID slices into a deduplicated, lexically-sorted slice.
func unionSorted(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	for _, s := range a {
		seen[s] = true
	}
	for _, s := range b {
		seen[s] = true
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// rejectUnbuildableCrossServiceUpstream transitions the release to Rejected and
// emits release.rejected:v1 naming the offending edge(s) — upstreams that are
// referenced by a changed node but absent from the candidate topology and
// therefore cannot be built into the candidate schema.
func rejectUnbuildableCrossServiceUpstream(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, releaseID string, edges []release.CrossServiceEdge, now time.Time) error {
	detail := formatCrossServiceEdges(edges)
	if err := r.TransitionToRejected("unbuildable_cross_service_upstream", nil, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	payload, err := json.Marshal(map[string]string{
		"release_id":   releaseID,
		"reason":       "unbuildable_cross_service_upstream",
		"error_class":  "validation_unsupported",
		"error_detail": detail,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(releaseID),
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
	// Parse phase succeeded; the release is rejected at validation-policy level, not parse level.
	d.Telemetry.ReleaseParseCompleted(ctx, releaseID, true, 0)
	d.Telemetry.ReleaseRejected(ctx, releaseID, "unbuildable_cross_service_upstream", nil)
	return nil
}

// formatCrossServiceEdges renders offending edges as a human-readable string
// listing each node->upstream pair separated by commas, followed by an
// actionable hint naming the missing producing model.
func formatCrossServiceEdges(edges []release.CrossServiceEdge) string {
	parts := make([]string, 0, len(edges))
	for _, e := range edges {
		parts = append(parts, e.Node+"->"+e.Upstream)
	}
	return strings.Join(parts, ", ") +
		"; upstream is not produced by any service in this release — add the producing model"
}
