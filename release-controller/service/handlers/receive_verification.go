package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ErrInvalidInput marks a submission the caller must fix; the HTTP layer
// answers 400 with the wrapped message.
var ErrInvalidInput = errors.New("invalid input")

// ErrRunKindConflict marks a submission whose id already names a run of the
// other kind; the HTTP layer answers 409.
var ErrRunKindConflict = errors.New("run id already names a run of another kind")

// ReceiveVerificationInput is the POST /verification-runs body: a
// fix-verification run agent-remediation submits for one edited service.
type ReceiveVerificationInput struct {
	RunID    string `json:"run_id"`
	Service  string `json:"service"`
	ImageTag string `json:"image_tag"`
	// Kind is the service's manifest kind, "dbt" or "python". Required: an
	// absent kind is a caller bug, not a default.
	Kind string `json:"kind"`
	// VerifiesReleaseID names the rejected candidate this run verifies a fix
	// for; that release's own candidate is assembled for its changed service.
	VerifiesReleaseID string `json:"verifies_release_id"`
	// Attempt is which attempt of the release's remediation this run belongs to.
	Attempt int `json:"attempt"`
	// SourceOverlayURI is the tarball of proposed source a dbt service's
	// compile and seed-build Jobs lay over the project. A python service is
	// verified by its packaged contract instead and carries none.
	SourceOverlayURI string `json:"source_overlay_uri"`
}

func (i ReceiveVerificationInput) validate() (release.ManifestKind, error) {
	switch {
	case i.RunID == "":
		return "", fmt.Errorf("%w: run_id is required", ErrInvalidInput)
	case i.Service == "":
		return "", fmt.Errorf("%w: service is required", ErrInvalidInput)
	case i.ImageTag == "":
		return "", fmt.Errorf("%w: image_tag is required", ErrInvalidInput)
	case i.Kind == "":
		return "", fmt.Errorf("%w: kind is required (dbt or python)", ErrInvalidInput)
	case i.VerifiesReleaseID == "":
		return "", fmt.Errorf("%w: verifies_release_id is required", ErrInvalidInput)
	case i.Attempt < 1:
		return "", fmt.Errorf("%w: attempt must be at least 1", ErrInvalidInput)
	}
	kind, err := release.ParseManifestKind(i.Kind)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if kind == release.ManifestKindPython && i.SourceOverlayURI != "" {
		return "", fmt.Errorf("%w: source_overlay_uri is accepted only with kind dbt", ErrInvalidInput)
	}
	return kind, nil
}

// ReceiveVerification persists a received verification run, idempotent on
// run_id. An existing verification with the id is accepted again; an
// existing run of another kind is a conflict. The caller advances the queue.
func ReceiveVerification(ctx context.Context, d *Deps, in ReceiveVerificationInput) error {
	kind, err := in.validate()
	if err != nil {
		return err
	}
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	existing, err := u.RunRepo().Get(ctx, in.RunID)
	if err != nil {
		return fmt.Errorf("get run: %w", err)
	}
	if existing != nil {
		if existing.Kind() != pipeline.KindVerification {
			return fmt.Errorf("%w: %s is a %s", ErrRunKindConflict, in.RunID, existing.Kind())
		}
		return u.Commit()
	}
	r := pipeline.NewVerification(in.RunID, in.Service, in.ImageTag, in.VerifiesReleaseID, in.Attempt, in.SourceOverlayURI, kind, d.Clock.Now())
	if err := u.RunRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save verification: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseReceived(ctx, in.RunID)
	return nil
}
