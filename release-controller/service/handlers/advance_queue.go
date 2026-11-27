package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/service/uow"
	"github.com/google/uuid"
)

// AdvanceQueue promotes the oldest Received release to active — but only if
// no release is already active (Compiling, Parsing, SeedBuilding, or
// Validating). Safe to call repeatedly; it is a no-op when the queue is empty
// or when a release is already in flight.
//
// The assembled manifest set (for all projects) is computed once here, before
// the branch below, so both paths record the same SetAssembledImageTags map
// and read the other projects' production pointers exactly once. Those
// pointers can change as earlier-queued releases are promoted, so we must
// read them at activation time to guarantee we see the live state.
//
// The release's kind then decides how it proceeds:
//   - dbt: TransitionToCompiling and emit compile.requested:v1 — CI's manifest
//     still needs a fresh dbt compile before it can be parsed. The
//     manifest-key set is assembled again in HandleCompileResult (ok path),
//     reading the production pointers a second time when the release advances
//     to Parsing.
//   - python and ontology: no compile leg exists — CI already uploaded
//     the odcs-bundle.json or the ontology.yaml before POST /releases — so the
//     release goes straight to TransitionToParsing and emits
//     release.requested:v1 with the set assembled here.
//
// A check of the kind ontology met while the install's ontology switch is off (a
// check accepted while it was on) is never activated: it is rejected
// invalid_artifact with ErrOntologyDisabled as its detail
// (rejectOntologyCheckWhileSwitchOff), before anything is assembled, and the check
// behind it is activated instead. So is a check whose kind is on the other side of the
// line between the ontology and the node kinds from its project's production
// pointer: it is rejected invalid_artifact with the kind-change refusal as its
// detail.
//
// While maintenance mode is on it activates only the release checks of fixes.
func AdvanceQueue(ctx context.Context, d *Deps) error {
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

	// Serialise across all callers (HTTP POST path + stream-consumer paths)
	// before reading active/next state. Without this lock two concurrent
	// AdvanceQueue calls both observe "no active release", both promote the
	// same queued row, and both write release.requested:v1 outbox entries.
	if err := u.LockReleaseQueue(ctx); err != nil {
		return fmt.Errorf("lock release queue: %w", err)
	}

	active, err := u.ReleaseCheckRepo().Active(ctx)
	if err != nil {
		return fmt.Errorf("active release: %w", err)
	}
	if active != nil {
		return u.Commit()
	}

	next, err := nextToActivate(ctx, u, d.Maintenance)
	if err != nil {
		return fmt.Errorf("next queued: %w", err)
	}
	if next == nil {
		return u.Commit()
	}
	if isOntologyCheckWhileSwitchOff(d, next) {
		// Nothing needs assembling for a check that is not activated. The
		// rejection ends the check and commits, so the queue is advanced again
		// for the check behind it.
		if err := rejectOntologyCheckWhileSwitchOff(ctx, d, u, next, d.Clock.Now()); err != nil {
			return err
		}
		return AdvanceQueue(ctx, d)
	}

	// Activation guard. The full topology is reconstructed from the production
	// pointers plus this release's changed project. When the live release lists
	// a project that has no production pointer, activating now would assemble
	// only the projects that have one and, on promote, retire every other one.
	// Refuse to activate until every project of the live release has a
	// production pointer (or is this release's changed project). The release
	// stays Received; once the operator runs seed-production-pointers, a later
	// AdvanceQueue tick proceeds automatically.
	//
	// A fresh install (nothing promoted, so no live release) is unaffected:
	// there are no live projects to drop, so the first bootstrap release
	// proceeds normally.
	live, err := u.LiveReleaseRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("get live release: %w", err)
	}
	// Read the production pointers once: the same set drives both the
	// activation guard and the manifest-set assembly below. Reading them now
	// (when the release becomes active) reflects any promotion an earlier-queued
	// release made meanwhile.
	pointers, err := u.ProductionPointerRepo().List(ctx)
	if err != nil {
		return fmt.Errorf("list production pointers: %w", err)
	}
	// Intake refuses a kind change against the production pointer it sees, but a
	// check queued before another check of the project promoted meets the pointer
	// that promotion wrote only here, where the queue lock is held and the pointers
	// are read. Assembling the set for such a check would swap the project's
	// manifest for an artifact of the other side and drop or replace its nodes.
	if stored := pointerOf(pointers, next.ChangedProject()); stored != nil &&
		(stored.ManifestKind() == release.ManifestKindOntology) != (next.ManifestKind() == release.ManifestKindOntology) {
		d.Logger.Warn("rejecting a check that changes its project between the kind ontology and a node kind",
			"release_id", next.ID(), "project", next.ChangedProject(),
			"stored_kind", string(stored.ManifestKind()), "kind", string(next.ManifestKind()))
		detail := kindChangeRefusalText(next.ChangedProject(), stored.ManifestKind())
		if err := rejectInvalidArtifactCheck(ctx, d, u, next, detail, d.Clock.Now()); err != nil {
			return err
		}
		return AdvanceQueue(ctx, d)
	}
	if live != nil && live.ReleaseID() != "" {
		liveTopology, err := loadLiveTopology(ctx, d, live)
		if err != nil {
			return err
		}
		if missing := uncoveredLiveProjects(liveTopology, pointers, next.ChangedProject()); len(missing) > 0 {
			d.Logger.Warn(
				"release activation blocked: projects of the live release have no production pointer; run seed-production-pointers before accepting releases",
				"release_id", next.ID(),
				"missing_projects", missing,
			)
			return u.Commit()
		}
	}

	now := d.Clock.Now()
	imageTag := next.ImageTags()[next.ChangedProject()]
	set := assembleFor(ctx, u, d.Logger, next, pointers, d.Bucket)
	next.SetAssembledImageTags(set.ImageTags)

	if !next.ManifestKind().HasCompileLeg() {
		// A release of any kind but dbt has no compile leg: CI uploaded its
		// artifact (the contract bundle or the ontology file) before
		// POST /releases, so activation goes straight to Parsing and requests
		// the manifest load with keys assembled from the same production
		// pointers the activation guard read.
		if err := next.TransitionToParsing(now); err != nil {
			return fmt.Errorf("transition to parsing: %w", err)
		}
		if err := u.ReleaseCheckRepo().Save(ctx, next); err != nil {
			return fmt.Errorf("save release: %w", err)
		}
		if err := emitReleaseRequested(ctx, u, next.ID(), set.ManifestKeys, next.ImageTags()); err != nil {
			return err
		}
		if err := u.Commit(); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
		d.Telemetry.ReleaseParseRequested(ctx, next.ID())
		return nil
	}

	if err := next.TransitionToCompiling(now); err != nil {
		return fmt.Errorf("transition to compiling: %w", err)
	}
	if err := u.ReleaseCheckRepo().Save(ctx, next); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	type compileRequestedPayload struct {
		ReleaseID        string `json:"release_id"`
		Project          string `json:"project"`
		ImageTag         string `json:"image_tag"`
		Bucket           string `json:"bucket"`
		CandidateSchema  string `json:"candidate_schema"`
		SourceOverlayURI string `json:"source_overlay_uri,omitempty"`
	}
	payload, err := json.Marshal(compileRequestedPayload{
		ReleaseID:        next.ID(),
		Project:          next.ChangedProject(),
		ImageTag:         imageTag,
		Bucket:           d.Bucket,
		CandidateSchema:  CandidateSchemaFor(next.ID()),
		SourceOverlayURI: next.SourceOverlayURI(),
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(next.ID()),
		EventType:     "compile_requested",
		Payload:       payload,
		StreamName:    streams.CompileRequestedV1,
		Status:        "pending",
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseParseRequested(ctx, next.ID())
	return nil
}

// pointerOf returns the production pointer of project, or nil when the
// project has none.
func pointerOf(pointers []*release.ProductionPointer, project string) *release.ProductionPointer {
	for _, p := range pointers {
		if p.ProjectName() == project {
			return p
		}
	}
	return nil
}

// uncoveredLiveProjects returns the sorted set of project names in the live
// release's topology that have no production pointer and are not the
// release's changed project. A non-empty result means assembling the full
// topology now would silently drop those projects on promote. An unknown-external node
// belongs to no project, so it is skipped: no production pointer covers it and
// promoting without one drops nothing.
func uncoveredLiveProjects(liveTopology release.Topology, pointers []*release.ProductionPointer, changedProject string) []string {
	covered := map[string]struct{}{changedProject: {}}
	for _, p := range pointers {
		covered[p.ProjectName()] = struct{}{}
	}

	missingSet := map[string]struct{}{}
	for _, n := range liveTopology {
		if n.IsExternal() {
			continue
		}
		if _, ok := covered[n.ProjectName]; !ok {
			missingSet[n.ProjectName] = struct{}{}
		}
	}

	missing := make([]string, 0, len(missingSet))
	for s := range missingSet {
		missing = append(missing, s)
	}
	sort.Strings(missing)
	return missing
}

// nextToActivate is the check the queue activates next: the oldest received check
// of either kind or, while maintenance mode is on, the oldest received
// fix's release check. Maintenance holds candidate releases in the queue, in
// creation order, but lets the release checks of remediation tries
// already in progress finish.
func nextToActivate(ctx context.Context, u uow.UnitOfWork, maintenance bool) (*releasecheck.Check, error) {
	if maintenance {
		return u.ReleaseCheckRepo().NextQueuedOfKind(ctx, releasecheck.KindFix)
	}
	return u.ReleaseCheckRepo().NextQueued(ctx)
}
