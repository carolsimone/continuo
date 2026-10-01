package handlers

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// ErrInvalidCandidate marks a submission the caller got wrong (a missing or
// unsupported field), as opposed to an infrastructure failure while storing it.
// Callers answer it as a client error and everything else as a server error.
var ErrInvalidCandidate = errors.New("invalid candidate")

// ErrReleaseIDConflict marks a submission whose release id already names a
// candidate with a different service, image tag, manifest kind, bootstrap
// flag or source change. Callers answer it as a conflict, as they do
// ErrRunKindConflict.
var ErrReleaseIDConflict = errors.New("release id already names a different candidate")

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
	// Shadow, SourceOverlayURI, and VerifiesReleaseID are fix-verification
	// concepts and are refused here (HTTP 400): a fix-verification run is
	// submitted to POST /verification-runs instead. The three fields stay on
	// this struct so a stale caller still posting them is answered with a
	// clear error rather than having the values silently ignored.
	Shadow            bool   `json:"shadow"`
	SourceOverlayURI  string `json:"source_overlay_uri"`
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
	if i.Shadow || i.SourceOverlayURI != "" || i.VerifiesReleaseID != "" {
		return errors.New("shadow, source_overlay_uri and verifies_release_id are not accepted here; a fix-verification run is posted to POST /verification-runs")
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
// the release_id PK: submitting a release id again with the same facts is a
// no-op, whatever status the stored candidate has reached. A bad submission
// returns an error wrapping ErrInvalidCandidate; a run-id kind clash wraps
// ErrRunKindConflict; a release id that names a candidate with different facts
// wraps ErrReleaseIDConflict; any other error is a storage failure. The caller
// (HTTP handler) is responsible for returning 202 Accepted to CI.
func ReceiveCandidate(ctx context.Context, d *Deps, in ReceiveCandidateInput) error {
	if err := in.validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCandidate, err)
	}
	kind, err := in.manifestKind()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCandidate, err)
	}
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	existing, err := u.RunRepo().Get(ctx, in.ReleaseID)
	if err != nil {
		return fmt.Errorf("get run: %w", err)
	}
	if existing != nil {
		if existing.Kind() != pipeline.KindCandidate {
			return fmt.Errorf("%w: %s is a %s", ErrRunKindConflict, in.ReleaseID, existing.Kind())
		}
		submitted := pipeline.CandidateSubmission{
			Service: in.Service, ImageTag: in.ImageTag, Kind: kind,
			Bootstrap: in.Bootstrap, Repo: in.Repo, CommitSHA: in.CommitSHA,
		}
		if !existing.MatchesSubmission(submitted) {
			return fmt.Errorf("%w: release id %q already exists with a different service, image tag, kind, bootstrap flag or source change",
				ErrReleaseIDConflict, in.ReleaseID)
		}
		return u.Commit()
	}

	r := pipeline.NewCandidate(in.ReleaseID, in.Service, in.ImageTag, in.Bootstrap, in.Repo, in.CommitSHA, kind, d.Clock.Now())
	if err := u.RunRepo().Save(ctx, r); err != nil {
		return fmt.Errorf("save release: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseReceived(ctx, in.ReleaseID)
	return nil
}
