package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// HandleParsedManifestInput carries the result of the topology-controller
// parsing a candidate release. Status must be "ok" or "failed".
type HandleParsedManifestInput struct {
	ReleaseID     string           `json:"release_id"`
	Status        string           `json:"status"` // "ok" or "failed"
	Topology      release.Topology `json:"topology,omitempty"`
	CodeBundleURI string           `json:"code_bundle_uri,omitempty"`
	ErrorClass    string           `json:"error_class,omitempty"`
	ErrorDetail   string           `json:"error_detail,omitempty"`
}

// HandleParsedManifest handles the manifest parse result from topology-controller.
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
	if err := r.TransitionToRejected("parse_failed", in.ErrorDetail, nil, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":   in.ReleaseID,
		"reason":       "parse_failed",
		"error_class":  in.ErrorClass,
		"error_detail": in.ErrorDetail,
		"shadow":       r.IsShadow(),
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
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
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
	r.SetCodeBundleURI(in.CodeBundleURI)

	// A relation must be produced by exactly one node, and a unique_id must
	// identify exactly one node — two independent checks. Two nodes claiming
	// the same relation write the same warehouse table, the second silently
	// replacing the first in the promoted topology, current_prod, and the
	// code bundle. Two nodes sharing a unique_id are indistinguishable to
	// every downstream lookup keyed on it — the code bundle, the candidate
	// object key, release-controller's own topology walks, the
	// orchestrator's :Table MERGE — so one is silently erased regardless of
	// whether their resolved relations also collide. Checked here, above the
	// bootstrap branch, because the candidate topology recorded at this
	// point feeds every later route to promoteToProduction: the bootstrap
	// branch below, the nothing-to-validate short-circuit later in this
	// function, the seed-build leg's own nothing-to-validate-after-built-seeds
	// short-circuit (handle_seed_build_result.go), and the normal
	// post-validation promotion (handle_validation_result.go). One check
	// here covers all four.
	if claims := release.DuplicateClaims(topo); len(claims) > 0 {
		return rejectDuplicateTable(ctx, d, u, r, in.ReleaseID, claims, now)
	}

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
	// upstream closure (across service boundaries) so every upstream is built as an
	// empty table in the candidate schema before its dependents. The executor
	// builds each node from compiled SQL whose schema-qualified refs are rewritten
	// to the candidate schema, gated in dependency order.
	changedClosure := release.DescendantsClosure(topo, changed)
	validationIDs := unionSorted(changedClosure, release.FullAncestorsClosure(topo, changedClosure))

	// Every node in the validation build set must have all its upstreams present
	// in the candidate topology; an upstream absent from it cannot be built into
	// the candidate schema and would fail the dbt --empty run with a missing
	// relation. Reject early with a clear, actionable message instead.
	if edges := release.UnbuildableCrossServiceUpstreams(topo, validationIDs); len(edges) > 0 {
		return rejectUnbuildableCrossServiceUpstream(ctx, d, u, r, in.ReleaseID, edges, now)
	}

	// Build changedClosureSet once (also used by validationNodesInOrder, Task A1).
	changedClosureSet := make(map[string]bool, len(changedClosure))
	for _, id := range changedClosure {
		changedClosureSet[id] = true
	}

	// Nothing to validate: no candidate node is new or content-changed vs prod
	// (e.g. a release that only bumps image tags, or removes a node). Emitting an
	// empty validation.requested would be rejected by executor-controller as a
	// permanent parse error, so no validation.completed would ever arrive and the
	// release would block the queue indefinitely. Promote directly instead — an
	// empty candidate diff trivially passes the gate.
	//
	// A shadow release is the exception and is rejected here instead: it exists
	// only to measure a proposed fix, and an empty candidate diff measures
	// nothing.
	if len(validationIDs) == 0 {
		if r.IsShadow() {
			return rejectShadowWithNothingToValidate(ctx, d, u, r, in.ReleaseID, now)
		}
		if err := r.TransitionToValidating(topo, validationIDs, now); err != nil {
			return fmt.Errorf("transition to validating: %w", err)
		}
		if err := promoteToProduction(ctx, d, u, r, in.ReleaseID, now); err != nil {
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
		if !r.IsShadow() {
			d.Telemetry.ReleasePromoted(ctx, in.ReleaseID, len(topo))
		}
		return nil
	}

	// New/changed seeds in the validation set must be built into the candidate
	// schema (real data, team image) BEFORE validation, so dependent candidate
	// models validate against the correct seed structure. Route to the seed-build
	// leg; validation.requested is emitted later, on seed.build.completed.
	seedIDs := newChangedSeedIDs(topo, validationIDs, changedClosureSet)
	if len(seedIDs) > 0 {
		return emitSeedBuildRequested(ctx, d, u, r, in.ReleaseID, topo, validationIDs, seedIDs, now)
	}

	// No new/changed seeds: Part A path (validate directly).
	if err := r.TransitionToValidating(topo, validationIDs, now); err != nil {
		return fmt.Errorf("transition to validating: %w", err)
	}

	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	candidateSchema := CandidateSchemaFor(in.ReleaseID)

	inSet := make(map[string]bool, len(validationIDs))
	for _, id := range validationIDs {
		inSet[id] = true
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":        in.ReleaseID,
		"mode":              "validation",
		"nodes":             validationNodesInOrder(topo, validationIDs, inSet, changedClosureSet),
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
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
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

// newChangedSeedIDs returns the validation-set node IDs that are dbt-seeds in the
// changed-closure (new or content-changed). These cannot be adapter-validated
// (no compiled SQL) and cannot be cloned (structure may have changed) — they are
// built into the candidate schema by the seed-build leg. Sorted for determinism.
func newChangedSeedIDs(topo release.Topology, validationIDs []string, changedClosureSet map[string]bool) []string {
	inSet := make(map[string]bool, len(validationIDs))
	for _, id := range validationIDs {
		inSet[id] = true
	}
	var out []string
	for _, n := range topo {
		if inSet[n.UniqueID] && changedClosureSet[n.UniqueID] && n.NodeType == "dbt-seed" {
			out = append(out, n.UniqueID)
		}
	}
	sort.Strings(out)
	return out
}

// seedBuildNodesInOrder returns one map per seed node (sorted by seedIDs order)
// carrying the fields executor-controller needs to build the seed into the
// candidate schema with the team image. No candidate_artifact_uri / validation_op:
// seeds are built, not adapter-validated; no upstreams: seeds are roots.
func seedBuildNodesInOrder(topo release.Topology, seedIDs []string) []map[string]any {
	byID := make(map[string]release.Node, len(topo))
	for _, n := range topo {
		byID[n.UniqueID] = n
	}
	out := make([]map[string]any, 0, len(seedIDs))
	for _, id := range seedIDs {
		n, ok := byID[id]
		if !ok {
			continue
		}
		out = append(out, map[string]any{
			"unique_id":    n.UniqueID,
			"service_name": n.ServiceName,
			"node_type":    n.NodeType,
			"schema_name":  n.SchemaName,
			"table_name":   n.TableName,
			"image_tag":    n.ImageTag,
		})
	}
	return out
}

// emitSeedBuildRequested transitions the release to SeedBuilding (recording the
// full candidate topology + validation IDs for the later validation.requested),
// emits seed.build.requested:v1 with ONLY the seed nodes, and commits.
func emitSeedBuildRequested(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release,
	releaseID string, topo release.Topology, validationIDs, seedIDs []string, now time.Time) error {

	if err := r.TransitionToSeedBuilding(topo, validationIDs, now); err != nil {
		return fmt.Errorf("transition to seed building: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	candidateSchema := CandidateSchemaFor(releaseID)
	payload, err := json.Marshal(map[string]any{
		"release_id":        releaseID,
		"mode":              "seed_build",
		"candidate_schema":  candidateSchema,
		"image_tags":        r.ImageTags(),
		"seeds":             seedBuildNodesInOrder(topo, seedIDs),
		"seed_ids_in_order": seedIDs,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(releaseID),
		EventType:     "seed_build_requested",
		Payload:       payload,
		StreamName:    streams.SeedBuildRequestedV1,
		Status:        "pending",
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseParseCompleted(ctx, releaseID, true, 0)
	d.Telemetry.ReleaseSeedBuildRequested(ctx, releaseID, len(seedIDs))
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
	if err := promoteToProduction(ctx, d, u, r, releaseID, now); err != nil {
		return err
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	// okCount is 0: a bootstrap validates no nodes (consistent with the
	// no-validation promote path's zero-node validation span).
	d.Telemetry.ReleaseParseCompleted(ctx, releaseID, true, 0)
	d.Telemetry.ReleaseValidationCompleted(ctx, releaseID, true, 0, 0, 0)
	if !r.IsShadow() {
		d.Telemetry.ReleasePromoted(ctx, releaseID, len(topo))
	}
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
func validationNodesInOrder(topo release.Topology, validationIDs []string, inSet, changedClosureSet map[string]bool) []map[string]any {
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
		op, prodSchema := validationOpFor(n, changedClosureSet)
		out = append(out, map[string]any{
			"unique_id":              n.UniqueID,
			"service_name":           n.ServiceName,
			"node_type":              n.NodeType,
			"schema_name":            n.SchemaName,
			"table_name":             n.TableName,
			"image_tag":              n.ImageTag,
			"upstream_node_ids":      release.InSetUpstreams(topo, id, inSet),
			"candidate_artifact_uri": n.CandidateArtifactURI,
			"validation_op":          op,
			"prod_schema":            prodSchema,
		})
	}
	return out
}

// validationOpFor returns the runner op and the prod source schema for a
// validation node. A changed-closure dbt node has compiled SQL rewritten to
// the candidate schema (build_from_sql); a changed-closure python node has a
// JSON validation spec of declared reads + output columns
// (build_from_columns). Every other node in the validation set is an
// unchanged upstream — there is no candidate artifact for it — so it is
// cloned empty from its production schema regardless of kind.
func validationOpFor(n release.Node, changedClosureSet map[string]bool) (op, prodSchema string) {
	if changedClosureSet[n.UniqueID] {
		if pkg_model.NodeType(n.NodeType).IsPython() {
			return "build_from_columns", ""
		}
		return "build_from_sql", ""
	}
	return "clone_from_prod", n.SchemaName
}

var nonAlphanumUnderscore = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// SanitizeSchemaSuffix converts a release ID into a dbt schema-name suffix by
// replacing all non-alphanumeric characters (except underscore) with underscores.
// It is used by CandidateSchemaFor to construct the candidate schema name.
func SanitizeSchemaSuffix(s string) string {
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

// rejectShadowWithNothingToValidate ends a shadow release whose candidate
// topology is identical to production, so its validation set is empty.
//
// A shadow release runs the real pipeline for one purpose: to find out whether
// a proposed fix survives validation. Its terminal "validated" status is the
// only evidence anyone has that the fix works, and it is what a human is shown
// before being asked to merge that fix. An empty validation set means no node
// would have been built or checked at all, so taking the trivial-pass
// promotion path here would report an unmeasured fix as verified. It is
// rejected instead, which routes it into the handling a fix that failed
// verification already gets: the attempt is recorded as failed with this
// reason as its evidence, and either the next attempt starts or the operator
// is left with the failure.
//
// The case arises when the proposed fix changed nothing the candidate topology
// can see — a node's content_hash folds its source, shared code, and resolved
// config, so an edit touching none of them leaves it identical to production —
// or when the packaged candidate declares no node at all.
//
// The rejection carries no stage and no per_node entries, so remediation
// derives no failure evidence from it and opens no heal trigger for it;
// shadow:true is the same signal every other shadow rejection carries.
func rejectShadowWithNothingToValidate(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, releaseID string, now time.Time) error {
	const detail = "a shadow release verifies a fix by validating it, and no candidate node is new or " +
		"content-changed against production, so nothing would have been validated and the fix is unproven"

	if err := r.TransitionToRejected("nothing_to_validate", detail, nil, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	payload, err := json.Marshal(map[string]any{
		"release_id":   releaseID,
		"reason":       "nothing_to_validate",
		"error_detail": detail,
		"shadow":       true,
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
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	// The parse itself succeeded; the release is rejected because what it
	// proposes cannot be measured, not on a malformed artifact.
	d.Telemetry.ReleaseParseCompleted(ctx, releaseID, true, 0)
	d.Telemetry.ReleaseRejected(ctx, releaseID, "nothing_to_validate", nil)
	return nil
}

// rejectUnbuildableCrossServiceUpstream transitions the release to Rejected and
// emits release.rejected:v1 naming the offending edge(s) — upstreams that are
// referenced by a changed node but absent from the candidate topology and
// therefore cannot be built into the candidate schema.
func rejectUnbuildableCrossServiceUpstream(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, releaseID string, edges []release.CrossServiceEdge, now time.Time) error {
	detail := formatCrossServiceEdges(edges)
	if err := r.TransitionToRejected("unbuildable_cross_service_upstream", detail, nil, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	payload, err := json.Marshal(map[string]any{
		"release_id":   releaseID,
		"reason":       "unbuildable_cross_service_upstream",
		"error_class":  "validation_unsupported",
		"error_detail": detail,
		"shadow":       r.IsShadow(),
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
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
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

// rejectDuplicateTable transitions the release to Rejected and emits
// release.rejected:v1 naming every node in every collision — both relation
// collisions (two nodes writing the same warehouse table) and identity
// collisions (two nodes sharing a unique_id). Each per_node entry carries the
// claimant a rename should target — the changed service's, when it has one —
// plus the competing claimant's service and file path, so the remediation
// classifier can build evidence without resolving the source location
// itself. That resolution is not available downstream: a rejected release is
// never promoted, so GetNodeLocation (which serves the promoted topology)
// holds nothing for these nodes. relation_id carries the contested relation itself
// (DuplicateClaim.RelationID), separately from node_id (the target
// claimant's own identity): remediation's classification signature and the
// remediation agent's rename prompt need the relation, not the target's
// declared name, or they name the wrong thing whenever the two differ (a
// node with an alias override).
//
// node_type carries the target claimant's kind (dbt-model, dbt-seed,
// dbt-snapshot, python-model, or python-csv) so remediation can tell, without a topology
// lookup of its own, whether the target's source is a single file this
// system can read. A python node's relation is declared in the service's
// contract.yaml, whose repository path this system does not carry — only
// file_path for the contract's script entry — so the fixer must skip a
// python target rather than editing a file that cannot produce the fix.
//
// A per_node entry is emitted only for a two-claimant RELATION collision.
// An identity collision never gets one, regardless of claimant count: the
// only rename a fixer can express is an alias edit, which changes a node's
// resolved relation but never its unique_id, so no proposal it could make
// would clear the collision — the same reasoning that already rules out a
// three-or-more-way relation collision, where fixing one claimant still
// leaves the relation claimed by the rest. A rename proposal always targets
// one claimant against one competitor; with three or more relation
// claimants, fixing the target still leaves the relation claimed by the N-2
// competitors that were never named as "the other producer", so the release
// would fail again immediately on a proposal that looked sufficient — and
// clearing an N-way collision would require N-1 independent PRs, proposed
// separately, to all merge together. Emitting no per_node entry means
// remediation builds no evidence and opens no trigger for that claim; it
// still appears in error_detail and failing_nodes, so an operator resolves
// it by hand. If every claim in the release is either an identity collision
// or a three-or-more-way relation collision, per_node is empty and nothing
// downstream fires.
//
// failing_nodes lists every claimant's own unique_id across every claim
// (deduplicated), not one entry per claim — a relation claim's claimants can
// carry different unique_ids (see DuplicateClaims), so a single shared id no
// longer identifies "the collision".
//
// repo/commit_sha describe only the service this release changed — each team
// ships its dbt or python jobs from its own repository, so there is no single
// checkout that contains every service's models. Target prefers the changed
// service's claimant, so in the common case per_node.file_path names a file in
// that same repo/commit_sha and the remediation agent can read it directly.
// When no claimant belongs to the changed service (a bootstrap release, or two
// other services colliding while this release changed a third), the target
// claimant's source lives in a repository this event does not name, and the
// agent's read of repo@commit_sha for that path fails. That is expected: the
// agent proposes nothing rather than guessing at a file it cannot see, and
// error_detail already names every claimant with its path so an operator can
// resolve the collision by hand.
func rejectDuplicateTable(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release,
	releaseID string, claims []release.DuplicateClaim, now time.Time) error {

	detail := release.FormatDuplicateClaims(claims)

	seen := make(map[string]bool, len(claims))
	failing := make([]string, 0, len(claims))
	perNode := make([]map[string]any, 0, len(claims))
	for _, c := range claims {
		for _, cl := range c.Claimants {
			if !seen[cl.UniqueID] {
				seen[cl.UniqueID] = true
				failing = append(failing, cl.UniqueID)
			}
		}
		if c.Kind == release.CollisionIdentity {
			// No rename a fixer can express changes a unique_id, so no
			// proposal could ever clear this — emit no per_node entry and
			// therefore no heal trigger.
			continue
		}
		if len(c.Claimants) != 2 {
			continue
		}
		target, other := c.Target(r.ChangedService())
		perNode = append(perNode, map[string]any{
			"node_id":         target.UniqueID,
			"status":          "failed",
			"service":         target.ServiceName,
			"file_path":       target.OriginalFilePath,
			"node_type":       target.NodeType,
			"relation_id":     c.RelationID,
			"other_service":   other.ServiceName,
			"other_file_path": other.OriginalFilePath,
		})
	}

	if err := r.TransitionToRejected("duplicate_table", detail, failing, now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":      releaseID,
		"reason":          "duplicate_table",
		"error_class":     "DuplicatedTable",
		"error_detail":    detail,
		"failing_nodes":   failing,
		"per_node":        perNode,
		"repo":            r.Repo(),
		"commit_sha":      r.CommitSHA(),
		"code_bundle_uri": r.CodeBundleURI(),
		"shadow":          r.IsShadow(),
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	r.SetRejectionPayload(payload)
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(releaseID),
		EventType:     "release_rejected",
		Payload:       payload,
		StreamName:    streams.ReleaseRejectedV1,
		Status:        "pending",
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	// The parse itself succeeded; the release is rejected on a topology
	// invariant, not on a malformed artifact.
	d.Telemetry.ReleaseParseCompleted(ctx, releaseID, true, 0)
	d.Telemetry.ReleaseRejected(ctx, releaseID, "duplicate_table", failing)
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
