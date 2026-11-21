package handlers

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/carolsimone/continuo/pkg/maintenance"
	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/uow"
)

// ErrInvalidCandidate marks a submission the caller got wrong (a missing or
// unsupported field), as opposed to an infrastructure failure while storing it.
// Callers answer it as a client error and everything else as a server error.
var ErrInvalidCandidate = errors.New("invalid candidate")

// refusal is an error whose message is the whole text a caller reads, and that
// matches the sentinel it is answered as.
type refusal struct {
	text string
	as   error
}

func (r refusal) Error() string        { return r.text }
func (r refusal) Is(target error) bool { return target == r.as }

// ErrOntologyDisabled marks a release of kind ontology submitted while the
// install's ontology switch is off. It is an ErrInvalidCandidate, so callers
// answer it as a client error.
var ErrOntologyDisabled error = refusal{
	text: fmt.Sprintf("kind %q is accepted only when the install's ontology switch is on (contracts.ontology.enabled)",
		release.ManifestKindOntology),
	as: ErrInvalidCandidate,
}

// ErrOntologyServiceConflict marks a release that breaks the rules of the
// ontology service: a second ontology service, or a service that changes
// between the kind ontology and a node kind. Callers answer it as a conflict.
var ErrOntologyServiceConflict = errors.New("ontology service conflict")

// ErrReleaseIDConflict marks a submission whose release id already names a
// candidate with a different service, image tag, manifest kind, bootstrap
// flag, rebaseline flag or source change. Callers answer it as a conflict, as they do
// ErrRunKindConflict.
var ErrReleaseIDConflict = errors.New("release id already names a different candidate")

// ReceiveCandidateInput carries the fields required to register a new release
// candidate. Service, ReleaseID, ImageTag, Repo, and CommitSHA are mandatory;
// Bootstrap is optional (defaults false) and, when true, promotes the release
// without validation. Rebaseline is optional (defaults false), is accepted only
// together with Bootstrap, and marks the release as one that re-baselines
// fingerprints. Repo (GitHub owner/name) and CommitSHA (full SHA) identify
// the source change so a downstream agent can locate it; they are opaque to
// release-controller. The manifest-key assembly and image-tag collection for
// other services happen later in AdvanceQueue, not here, so that we always read
// the live service_prod pointers at the moment this release becomes active.
type ReceiveCandidateInput struct {
	Service   string
	ReleaseID string
	ImageTag  string
	Bootstrap bool
	// Rebaseline asks a bootstrap release to tell run-controller to record
	// every live seed as loaded at its new hash instead of reloading it.
	Rebaseline bool
	Repo       string
	CommitSHA  string
	// Kind selects how this service's artifact is parsed: "dbt"
	// (manifest.json — the default when absent, so existing CI callers are
	// untouched), "python-odcs" (odcs-bundle.json, uploaded by the domain
	// repo's CI before this POST) or "ontology" (ontology.yaml, the install's
	// company ontology, accepted only while the install's ontology switch is
	// on). Anything else, the python kind included, is rejected (HTTP 400).
	Kind string
	// Shadow, SourceOverlayURI, and VerifiesReleaseID are fix-verification
	// concepts and are refused here (HTTP 400): a fix-verification run is
	// submitted to POST /verification-runs instead. The three fields stay on
	// this struct so a stale caller still posting them is answered with a
	// clear error rather than having the values silently ignored.
	Shadow            bool
	SourceOverlayURI  string
	VerifiesReleaseID string
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
	if i.Rebaseline && !i.Bootstrap {
		return errors.New("rebaseline is accepted only together with bootstrap")
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

// resubmitOutcome decides a submission whose release id already names a run:
// nil when it is the same candidate, ErrRunKindConflict when the id names a
// verification run, ErrReleaseIDConflict when it names a different candidate.
func resubmitOutcome(existing *pipeline.Run, in ReceiveCandidateInput, kind release.ManifestKind) error {
	if existing.Kind() != pipeline.KindCandidate {
		return fmt.Errorf("%w: %s is a %s", ErrRunKindConflict, in.ReleaseID, existing.Kind())
	}
	submitted := pipeline.CandidateSubmission{
		Service: in.Service, ImageTag: in.ImageTag, Kind: kind,
		Bootstrap: in.Bootstrap, Rebaseline: in.Rebaseline, Repo: in.Repo, CommitSHA: in.CommitSHA,
	}
	if !existing.MatchesSubmission(submitted) {
		return fmt.Errorf("%w: %q has a different service, image tag, kind, bootstrap or rebaseline flag, or source change",
			ErrReleaseIDConflict, in.ReleaseID)
	}
	return nil
}

// ReceiveCandidate persists a new Release row in StatusReceived, idempotent on
// the release_id PK: submitting a release id again with the same facts is a
// no-op, whatever status the stored candidate has reached. A bad submission
// returns an error wrapping ErrInvalidCandidate; a run-id kind clash wraps
// ErrRunKindConflict; a release id that names a candidate with different facts
// wraps ErrReleaseIDConflict; any other error is a storage failure. The caller
// (HTTP handler) is responsible for returning 202 Accepted to CI. It refuses
// with maintenance.ErrActive while maintenance mode is on.
//
// A release of kind ontology is refused with ErrOntologyDisabled while the
// install's ontology switch is off. When the submission would create a run,
// the ontology service's rules apply (ErrOntologyServiceConflict): an install
// has one ontology service, and a service never changes between the kind
// ontology and a node kind. A release id the install already stored is
// answered as a resubmit whatever the install holds now.
func ReceiveCandidate(ctx context.Context, d *Deps, in ReceiveCandidateInput) error {
	if d.Maintenance {
		return maintenance.ErrActive
	}
	if err := in.validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCandidate, err)
	}
	kind, err := in.manifestKind()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCandidate, err)
	}
	if kind == release.ManifestKindOntology && !d.OntologyEnabled {
		return ErrOntologyDisabled
	}
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	if kind == release.ManifestKindOntology {
		// The release-queue lock serializes ontology submissions with each
		// other and with promotions, so two submissions for two services
		// cannot both pass the one-ontology-service check, and a promotion
		// cannot change service_prod between the check and the insert. It is
		// taken before the run row is loaded, as every holder of both locks
		// takes them.
		if err := u.LockReleaseQueue(ctx); err != nil {
			return fmt.Errorf("lock release queue: %w", err)
		}
	}

	existing, err := u.RunRepo().Load(ctx, in.ReleaseID)
	if err != nil {
		return fmt.Errorf("load run: %w", err)
	}
	if existing == nil {
		if err := refuseOntologyConflict(ctx, u, in.Service, kind); err != nil {
			return err
		}
		r := pipeline.NewCandidate(in.ReleaseID, in.Service, in.ImageTag, in.Bootstrap, in.Rebaseline, in.Repo, in.CommitSHA, kind, d.Clock.Now())
		inserted, err := u.RunRepo().Create(ctx, r)
		if err != nil {
			return fmt.Errorf("create release: %w", err)
		}
		if inserted {
			if err := u.Commit(); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
			d.Telemetry.ReleaseReceived(ctx, in.ReleaseID)
			return nil
		}
		// A concurrent submission of this id committed its run after the Load
		// above found none, so this submission is a resubmit of that run.
		if existing, err = u.RunRepo().Load(ctx, in.ReleaseID); err != nil {
			return fmt.Errorf("load run: %w", err)
		}
		if existing == nil {
			return fmt.Errorf("release %s was neither inserted nor found", in.ReleaseID)
		}
	}
	if err := resubmitOutcome(existing, in, kind); err != nil {
		return err
	}
	return u.Commit()
}

// kindChangeRefusalText is the refusal for a release of a service whose
// service_prod row holds the kind stored, when the release's kind is on the
// other side of the line between the ontology and the node kinds. Intake
// answers it as a conflict and activation ends the run with it as its detail.
func kindChangeRefusalText(service string, stored release.ManifestKind) string {
	return fmt.Sprintf("service %q is released as kind %q: a service cannot change between kind %q and a node kind",
		service, stored, release.ManifestKindOntology)
}

// refuseOntologyConflict returns ErrOntologyServiceConflict when a new release
// of kind for service breaks the rules of the ontology service: the service is
// released with a kind on the other side of the line between the ontology and
// the node kinds, or another service already is the install's ontology, in
// service_prod or in a candidate run that has not ended.
func refuseOntologyConflict(ctx context.Context, u uow.UnitOfWork, service string, kind release.ManifestKind) error {
	stored, err := u.ServiceProdRepo().Get(ctx, service)
	if err != nil {
		return fmt.Errorf("get service_prod %q: %w", service, err)
	}
	isOntology := kind == release.ManifestKindOntology
	if stored != nil && (stored.ManifestKind() == release.ManifestKindOntology) != isOntology {
		return refusal{
			text: kindChangeRefusalText(service, stored.ManifestKind()),
			as:   ErrOntologyServiceConflict,
		}
	}
	if !isOntology {
		return nil
	}
	released, err := u.ServiceProdRepo().ServicesWithKind(ctx, release.ManifestKindOntology)
	if err != nil {
		return fmt.Errorf("list the services released as kind %q: %w", release.ManifestKindOntology, err)
	}
	queued, err := u.RunRepo().ActiveCandidateServicesWithKind(ctx, release.ManifestKindOntology)
	if err != nil {
		return fmt.Errorf("list the candidate runs of kind %q: %w", release.ManifestKindOntology, err)
	}
	others := slices.DeleteFunc(slices.Concat(released, queued), func(name string) bool { return name == service })
	if len(others) == 0 {
		return nil
	}
	return refusal{
		text: fmt.Sprintf("service %q is the install's ontology: an install has one ontology service", slices.Min(others)),
		as:   ErrOntologyServiceConflict,
	}
}
