package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ReceiveCandidateInput carries the fields required to register a new
// release candidate. All four fields are mandatory.
type ReceiveCandidateInput struct {
	ReleaseID      string            `json:"release_id"`
	ChangedNodeIDs []string          `json:"changed_node_ids"`
	ImageTags      map[string]string `json:"image_tags"`
	ManifestsURI   string            `json:"manifests_uri"`
}

func (i ReceiveCandidateInput) validate() error {
	if i.ReleaseID == "" {
		return errors.New("release_id is required")
	}
	if len(i.ChangedNodeIDs) == 0 {
		return errors.New("changed_node_ids must be non-empty")
	}
	if len(i.ImageTags) == 0 {
		return errors.New("image_tags must be non-empty")
	}
	if i.ManifestsURI == "" {
		return errors.New("manifests_uri is required")
	}
	return nil
}

// ReceiveCandidate persists a new Release row in StatusReceived, idempotent on
// the release_id PK. The caller (HTTP handler) is responsible for returning
// 202 Accepted to CI.
func ReceiveCandidate(ctx context.Context, d *Deps, in ReceiveCandidateInput) error {
	if err := in.validate(); err != nil {
		return err
	}
	if err := d.UoW.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer d.UoW.Rollback() //nolint:errcheck

	existing, _ := d.UoW.ReleaseRepo().Get(ctx, in.ReleaseID)
	if existing != nil {
		return d.UoW.Commit()
	}

	r := release.New(in.ReleaseID, in.ChangedNodeIDs, in.ImageTags, in.ManifestsURI, d.Clock.Now())
	if err := d.UoW.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	if err := d.UoW.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseReceived(ctx, in.ReleaseID, len(in.ChangedNodeIDs))
	return nil
}
