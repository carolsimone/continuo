package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// ErrInvalidInput marks a submission the caller must fix; the HTTP layer
// answers 400 with the wrapped message.
var ErrInvalidInput = errors.New("invalid input")

// ErrCheckKindConflict marks a submission whose id already names a check of the
// other kind; the HTTP layer answers 409. It aliases the repository sentinel.
// Both receive paths raise it after loading the check that holds the id, whether
// that check was committed before the submission or by a concurrent submission
// whose insert won.
var ErrCheckKindConflict = repository.ErrCheckKindConflict

// ReceiveFixInput is the POST /release-checks body: a
// fix's release check agent-remediation submits for one edited project.
type ReceiveFixInput struct {
	CheckID  string
	Project  string
	ImageTag string
	// Kind is the project's manifest kind: "dbt" or "python".
	// Required: an absent kind is a caller bug, not a default.
	Kind string
	// VerifiesReleaseID names the rejected candidate this check verifies a fix
	// for; that release's own candidate is assembled for its changed project.
	VerifiesReleaseID string
	// Attempt is which attempt of the release's remediation this check belongs to.
	Attempt int
	// SourceOverlayURI is the tarball of proposed source a dbt project's
	// compile and seed-build Jobs lay over the project. A python
	// project is verified by its packaged bundle instead and carries none.
	SourceOverlayURI string
}

func (i ReceiveFixInput) validate() (release.ManifestKind, error) {
	switch {
	case i.CheckID == "":
		return "", fmt.Errorf("%w: check_id is required", ErrInvalidInput)
	case i.Project == "":
		return "", fmt.Errorf("%w: project is required", ErrInvalidInput)
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
	if kind == release.ManifestKindOntology {
		return "", refusal{
			text: fmt.Sprintf("a fix's release check cannot have kind %q", release.ManifestKindOntology),
			as:   ErrInvalidInput,
		}
	}
	if kind == release.ManifestKindPython && i.SourceOverlayURI != "" {
		return "", fmt.Errorf("%w: source_overlay_uri is accepted only with kind dbt", ErrInvalidInput)
	}
	return kind, nil
}

// ReceiveFix persists a received fix's release check, idempotent on
// check_id: an existing fix's release check with the id is accepted again whatever its
// facts, and an existing check of another kind is a conflict. The insert is
// Create, never an upsert, so two concurrent submissions of one id cannot
// overwrite each other's row: the one whose insert is refused re-reads the
// committed check and answers as a resubmit of it. The caller advances the queue.
func ReceiveFix(ctx context.Context, d *Deps, in ReceiveFixInput) error {
	kind, err := in.validate()
	if err != nil {
		return err
	}
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	existing, err := u.ReleaseCheckRepo().Load(ctx, in.CheckID)
	if err != nil {
		return fmt.Errorf("load check: %w", err)
	}
	if existing == nil {
		r := releasecheck.NewFix(in.CheckID, in.Project, in.ImageTag, in.VerifiesReleaseID, in.Attempt, in.SourceOverlayURI, kind, d.Clock.Now())
		inserted, err := u.ReleaseCheckRepo().Create(ctx, r)
		if err != nil {
			return fmt.Errorf("create fix's release check: %w", err)
		}
		if inserted {
			if err := u.Commit(); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
			d.Telemetry.ReleaseReceived(ctx, in.CheckID)
			return nil
		}
		// A concurrent submission of this id committed its check after the Load
		// above found none, so this submission is a resubmit of that check.
		if existing, err = u.ReleaseCheckRepo().Load(ctx, in.CheckID); err != nil {
			return fmt.Errorf("load check: %w", err)
		}
		if existing == nil {
			return fmt.Errorf("fix's release check %s was neither inserted nor found", in.CheckID)
		}
	}
	if existing.Kind() != releasecheck.KindFix {
		return fmt.Errorf("%w: %s is a %s", ErrCheckKindConflict, in.CheckID, existing.Kind())
	}
	return u.Commit()
}
