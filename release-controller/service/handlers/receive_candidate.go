package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ReceiveCandidateInput carries the fields required to register a new release
// candidate. Service, ReleaseID, ImageTag, Repo, and CommitSHA are mandatory;
// Bootstrap is optional (defaults false) and, when true, promotes the release
// without validation. Repo (GitHub owner/name) and CommitSHA (full SHA) identify
// the source change so a downstream agent can locate it; they are opaque to
// release-controller. The manifest-key assembly and image-tag collection for
// other services happen later in AdvanceQueue, not here, so that we always read
// the live service_prod pointers at the moment this release becomes active.
type ReceiveCandidateInput struct {
	Service   string `json:"service"`
	ReleaseID string `json:"release_id"`
	ImageTag  string `json:"image_tag"`
	Bootstrap bool   `json:"bootstrap"`
	Repo      string `json:"repo"`
	CommitSHA string `json:"commit_sha"`
	// Kind selects how this service's artifact is parsed: "dbt"
	// (manifest.json — the default when absent, so existing CI callers are
	// untouched) or "python" (contract.yaml, uploaded by the domain repo's CI
	// before this POST). Anything else is rejected (HTTP 400).
	Kind string `json:"kind"`
	// Shadow marks a fix-verification release posted by agent-remediation: it
	// runs the normal parse+validation pipeline but stops at StatusValidated
	// instead of promoting to production. Absent means false — the default
	// for every existing caller.
	Shadow bool `json:"shadow"`
	// SourceOverlayURI locates a tarball of project-relative source files the
	// compile leg lays over the checked-in project before running, so the
	// release verifies a proposed fix instead of the committed source. Accepted
	// only together with Shadow; a production release always compiles exactly
	// what is committed.
	SourceOverlayURI string `json:"source_overlay_uri"`
	// VerifiesReleaseID names the rejected release this shadow release verifies
	// a fix for. The fix may edit a different service than the one whose release
	// was rejected, so the rejected release's own changed service is assembled
	// from ITS candidate instead of from the live production pointer — otherwise
	// the fix would be judged against code the rejection was never about.
	// Accepted only together with Shadow.
	VerifiesReleaseID string `json:"verifies_release_id"`
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
	if i.Repo == "" {
		return errors.New("repo is required")
	}
	if i.CommitSHA == "" {
		return errors.New("commit_sha is required")
	}
	if _, err := i.manifestKind(); err != nil {
		return err
	}
	if i.SourceOverlayURI != "" && !i.Shadow {
		return errors.New("source_overlay_uri is accepted only on a shadow release")
	}
	if i.VerifiesReleaseID != "" && !i.Shadow {
		return errors.New("verifies_release_id is accepted only on a shadow release")
	}
	return nil
}

// manifestKind normalizes the optional wire kind: absent/empty means dbt.
func (i ReceiveCandidateInput) manifestKind() (release.ManifestKind, error) {
	if i.Kind == "" {
		return release.ManifestKindDbt, nil
	}
	return release.ParseManifestKind(i.Kind)
}

// ReceiveCandidate persists a new Release row in StatusReceived, idempotent on
// the release_id PK. The caller (HTTP handler) is responsible for returning
// 202 Accepted to CI.
func ReceiveCandidate(ctx context.Context, d *Deps, in ReceiveCandidateInput) error {
	if err := in.validate(); err != nil {
		return err
	}
	kind, err := in.manifestKind()
	if err != nil {
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

	r := release.New(in.ReleaseID, in.Service, in.ImageTag, in.Bootstrap, in.Shadow, in.Repo, in.CommitSHA, kind, d.Clock.Now())
	r.SetSourceOverlayURI(in.SourceOverlayURI)
	r.SetVerifiesReleaseID(in.VerifiesReleaseID)
	if err := u.ReleaseRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseReceived(ctx, in.ReleaseID)
	return nil
}
