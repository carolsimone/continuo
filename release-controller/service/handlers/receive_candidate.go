package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ReceiveCandidateInput carries the fields required to register a new release
// candidate. Service, ReleaseID, and ImageTag are mandatory; Bootstrap is
// optional (defaults false) and, when true, promotes the release without
// validation. The manifest-key assembly and image-tag collection for other
// services happen later in AdvanceQueue, not here, so that we always read the
// live service_prod pointers at the moment this release becomes active.
type ReceiveCandidateInput struct {
	Service   string `json:"service"`
	ReleaseID string `json:"release_id"`
	ImageTag  string `json:"image_tag"`
	Bootstrap bool   `json:"bootstrap"`
}

func (i ReceiveCandidateInput) validate() error {
	if i.ReleaseID == "" {
		return errors.New("release_id is required")
	}
	if i.Service == "" {
		return errors.New("service is required")
	}
	if i.ImageTag == "" {
		return errors.New("image_tag is required")
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

	r := release.New(in.ReleaseID, in.Service, in.ImageTag, in.Bootstrap, d.Clock.Now())
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseReceived(ctx, in.ReleaseID)
	return nil
}
