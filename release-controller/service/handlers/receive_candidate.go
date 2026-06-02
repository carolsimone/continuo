package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ReceiveCandidateInput carries the fields required to register a new release
// candidate. ReleaseID, ImageTags, and ManifestsURI are mandatory; Bootstrap is
// optional (defaults false) and, when true, promotes the release without
// validation. The changed-node set is not supplied by the caller —
// release-controller derives it later from the content_hash diff against the
// current prod topology in HandleParsedManifest.
type ReceiveCandidateInput struct {
	ReleaseID    string            `json:"release_id"`
	ImageTags    map[string]string `json:"image_tags"`
	ManifestsURI string            `json:"manifests_uri"`
	Bootstrap    bool              `json:"bootstrap"`
}

func (i ReceiveCandidateInput) validate() error {
	if i.ReleaseID == "" {
		return errors.New("release_id is required")
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
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	existing, _ := u.ReleaseRepo().Get(ctx, in.ReleaseID)
	if existing != nil {
		return u.Commit()
	}

	r := release.New(in.ReleaseID, in.ImageTags, in.ManifestsURI, in.Bootstrap, d.Clock.Now())
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseReceived(ctx, in.ReleaseID)
	return nil
}
