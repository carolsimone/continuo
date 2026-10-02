package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// ErrInvalidInput marks a submission the caller must fix; the HTTP layer
// answers 400 with the wrapped message.
var ErrInvalidInput = errors.New("invalid input")

// ErrRunKindConflict marks a submission whose id already names a run of the
// other kind; the HTTP layer answers 409. It aliases the repository sentinel.
// Both receive paths raise it after loading the run that holds the id, whether
// that run was committed before the submission or by a concurrent submission
// whose insert won.
var ErrRunKindConflict = repository.ErrRunKindConflict

// ReceiveVerificationInput is the POST /verification-runs body: a
// fix-verification run agent-remediation submits for one edited service.
type ReceiveVerificationInput struct {
	RunID    string
	Service  string
	ImageTag string
	// Kind is the service's manifest kind, "dbt" or "python". Required: an
	// absent kind is a caller bug, not a default.
	Kind string
	// VerifiesReleaseID names the rejected candidate this run verifies a fix
	// for; that release's own candidate is assembled for its changed service.
	VerifiesReleaseID string
	// Attempt is which attempt of the release's remediation this run belongs to.
	Attempt int
	// SourceOverlayURI is the tarball of proposed source a dbt service's
	// compile and seed-build Jobs lay over the project. A python service is
	// verified by its packaged contract instead and carries none.
	SourceOverlayURI string
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
// run_id: an existing verification with the id is accepted again whatever its
// facts, and an existing run of another kind is a conflict. The insert is
// Create, never an upsert, so two concurrent submissions of one id cannot
// overwrite each other's row: the one whose insert is refused re-reads the
// committed run and answers as a resubmit of it. The caller advances the queue.
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

	existing, err := u.RunRepo().Load(ctx, in.RunID)
	if err != nil {
		return fmt.Errorf("load run: %w", err)
	}
	if existing == nil {
		r := pipeline.NewVerification(in.RunID, in.Service, in.ImageTag, in.VerifiesReleaseID, in.Attempt, in.SourceOverlayURI, kind, d.Clock.Now())
		inserted, err := u.RunRepo().Create(ctx, r)
		if err != nil {
			return fmt.Errorf("create verification: %w", err)
		}
		if inserted {
			if err := u.Commit(); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
			d.Telemetry.ReleaseReceived(ctx, in.RunID)
			return nil
		}
		// A concurrent submission of this id committed its run after the Load
		// above found none, so this submission is a resubmit of that run.
		if existing, err = u.RunRepo().Load(ctx, in.RunID); err != nil {
			return fmt.Errorf("load run: %w", err)
		}
		if existing == nil {
			return fmt.Errorf("verification %s was neither inserted nor found", in.RunID)
		}
	}
	if existing.Kind() != pipeline.KindVerification {
		return fmt.Errorf("%w: %s is a %s", ErrRunKindConflict, in.RunID, existing.Kind())
	}
	return u.Commit()
}
