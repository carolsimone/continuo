package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
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

	now := d.Clock.Now()

	if in.Status == "failed" {
		return handleParseFailed(ctx, d, u, r, in, now)
	}
	return handleParseOK(ctx, d, u, r, in, now)
}

func handleParseFailed(ctx context.Context, d *Deps, u uow.UnitOfWork, r *release.Release, in HandleParsedManifestInput, now time.Time) error {
	if err := r.TransitionToRejected("parse_failed", nil, "", now); err != nil {
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

	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get current prod: %w", err)
	}

	// Derive the validation seed set from the content_hash diff against the
	// current prod topology: candidate nodes that are new or whose hash changed.
	// Bootstrap (no prod row) yields an empty prod snapshot, so every candidate
	// node is treated as new and the whole topology is validated.
	changed := release.DerivedChangedNodeIDs(topo, cp.TopologySnapshot())
	validationIDs := release.DescendantsClosure(topo, changed)

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
		d.Telemetry.ReleaseParseCompleted(ctx, in.ReleaseID, true, 0)
		d.Telemetry.ReleasePromoted(ctx, in.ReleaseID, len(topo))
		return nil
	}

	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	candidateSchema := "_candidate_" + sanitizeSchemaSuffix(in.ReleaseID)

	deferStateURI := ""
	if cp.ReleaseID() != "" {
		deferStateURI = fmt.Sprintf("s3://continuo/releases/%s/manifests/", cp.ReleaseID())
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":        in.ReleaseID,
		"mode":              "validation",
		"nodes":             validationNodesInOrder(topo, validationIDs),
		"node_ids_in_order": validationIDs,
		"image_tags":        r.ImageTags(),
		"candidate_schema":  candidateSchema,
		"defer_state_uri":   deferStateURI,
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

// validationNodesInOrder returns one map per validation node in
// topological order, carrying the per-node fields executor-controller needs
// to build a candidate dbt job: unique_id, service_name, node_type,
// schema_name, table_name, image_tag. The map shape is intentionally flat (no nested
// upstreams) — executor-controller only needs what to run, not how the
// candidate DAG is structured. The order follows validationIDs which is the
// topo-sorted output of DescendantsClosure.
func validationNodesInOrder(topo release.Topology, validationIDs []string) []map[string]any {
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

var nonAlphanumUnderscore = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// sanitizeSchemaSuffix replaces any character that is not [a-zA-Z0-9_] with _.
// Used to derive a safe Postgres schema name suffix from a release_id.
func sanitizeSchemaSuffix(s string) string {
	return nonAlphanumUnderscore.ReplaceAllString(s, "_")
}
