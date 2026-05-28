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
	if err := d.UoW.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer d.UoW.Rollback() //nolint:errcheck

	r, err := d.UoW.ReleaseRepo().Get(ctx, in.ReleaseID)
	if err != nil {
		return fmt.Errorf("get release: %w", err)
	}

	now := d.Clock.Now()

	if in.Status == "failed" {
		return handleParseFailed(ctx, d, r, in, now)
	}
	return handleParseOK(ctx, d, r, in, now)
}

func handleParseFailed(ctx context.Context, d *Deps, r *release.Release, in HandleParsedManifestInput, now time.Time) error {
	if err := r.TransitionToRejected("parse_failed", nil, "", now); err != nil {
		return fmt.Errorf("transition to rejected: %w", err)
	}
	if err := d.UoW.ReleaseRepo().Save(ctx, r); err != nil {
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
	if err := d.UoW.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
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

	if err := d.UoW.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseParseCompleted(ctx, in.ReleaseID, false, 0)
	d.Telemetry.ReleaseRejected(ctx, in.ReleaseID, "parse_failed", nil)
	return nil
}

func handleParseOK(ctx context.Context, d *Deps, r *release.Release, in HandleParsedManifestInput, now time.Time) error {
	topo := joinImageTags(in.Topology, r.ImageTags())
	validationIDs := release.DescendantsClosure(topo, r.ChangedNodeIDs())

	if err := r.TransitionToValidating(topo, validationIDs, now); err != nil {
		return fmt.Errorf("transition to validating: %w", err)
	}
	if err := d.UoW.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	candidateSchema := "_candidate_" + sanitizeSchemaSuffix(in.ReleaseID)

	cp, err := d.UoW.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get current prod: %w", err)
	}
	deferStateURI := ""
	if cp.ReleaseID() != "" {
		deferStateURI = fmt.Sprintf("s3://continuo/releases/%s/manifests/", cp.ReleaseID())
	}

	payload, err := json.Marshal(map[string]any{
		"release_id":         in.ReleaseID,
		"mode":               "validation",
		"node_ids_in_order":  validationIDs,
		"image_tags":         r.ImageTags(),
		"candidate_schema":   candidateSchema,
		"defer_state_uri":    deferStateURI,
		"dbt_flags":          []string{"--empty"},
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := d.UoW.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
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

	if err := d.UoW.Commit(); err != nil {
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

var nonAlphanumUnderscore = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// sanitizeSchemaSuffix replaces any character that is not [a-zA-Z0-9_] with _.
// Used to derive a safe Postgres schema name suffix from a release_id.
func sanitizeSchemaSuffix(s string) string {
	return nonAlphanumUnderscore.ReplaceAllString(s, "_")
}
