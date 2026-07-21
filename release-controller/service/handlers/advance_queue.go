package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/google/uuid"
)

// AdvanceQueue promotes the oldest Received release to Compiling and emits
// compile.requested:v1 into the outbox — but only if no release is already
// active (Compiling, Parsing, SeedBuilding, or Validating). Safe to call
// repeatedly; it is a no-op when the queue is empty or when a release is
// already in flight.
//
// The assembled image-tag set (for all services) is computed here so that
// SetAssembledImageTags can record the full multi-service map on the release.
// The other services' service_prod pointers can change as earlier-queued
// releases are promoted, so we must read them at activation time to guarantee
// we see the live state. The manifest-key set is assembled again in
// HandleCompileResult (ok path), reading live service_prod a second time when
// the release advances to Parsing.
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

	active, err := u.ReleaseRepo().ActiveRelease(ctx)
	if err != nil {
		return fmt.Errorf("active release: %w", err)
	}
	if active != nil {
		return u.Commit()
	}

	next, err := u.ReleaseRepo().NextQueuedRelease(ctx)
	if err != nil {
		return fmt.Errorf("next queued: %w", err)
	}
	if next == nil {
		return u.Commit()
	}

	// Activation guard for the per-service model. The full topology is
	// reconstructed from the service_prod pointer table plus this release's
	// changed service. After an upgrade from the old whole-snapshot model,
	// current_prod still lists every live service but service_prod starts empty,
	// so activating now would assemble only the changed service and, on promote,
	// retire every unseeded service. Refuse to activate until every service live
	// in current_prod is covered by a service_prod pointer (or is this release's
	// changed service). The release stays Received; once the operator runs
	// seed-service-prod, a later AdvanceQueue tick proceeds automatically.
	//
	// A fresh install (current_prod unseeded) is unaffected: there are no live
	// services to amputate, so the first bootstrap release proceeds normally.
	cp, err := u.CurrentProdRepo().Get(ctx)
	if err != nil {
		return fmt.Errorf("current prod: %w", err)
	}
	// Read the live service_prod pointers once: the same set drives both the
	// activation guard and the manifest-set assembly below. Reading them now
	// (when the release becomes active) reflects any promotion an earlier-queued
	// release made meanwhile.
	pointers, err := u.ServiceProdRepo().List(ctx)
	if err != nil {
		return fmt.Errorf("list service prod: %w", err)
	}
	if cp != nil && cp.ReleaseID() != "" {
		if missing := uncoveredProdServices(cp, pointers, next.ChangedService()); len(missing) > 0 {
			d.Logger.Warn(
				"release activation blocked: service_prod is missing pointers for live services; run seed-service-prod before accepting releases",
				"release_id", next.ID(),
				"missing_services", missing,
			)
			return u.Commit()
		}
	}

	now := d.Clock.Now()
	if err := next.TransitionToCompiling(now); err != nil {
		return fmt.Errorf("transition to compiling: %w", err)
	}

	imageTag := next.ImageTags()[next.ChangedService()]
	set := AssembleManifestSet(pointers, d.Bucket, next.ChangedService(), next.ID(), imageTag)
	next.SetAssembledImageTags(set.ImageTags)

	if err := u.ReleaseRepo().Save(ctx, next); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	type compileRequestedPayload struct {
		ReleaseID       string `json:"release_id"`
		Service         string `json:"service"`
		ImageTag        string `json:"image_tag"`
		Bucket          string `json:"bucket"`
		CandidateSchema string `json:"candidate_schema"`
	}
	payload, err := json.Marshal(compileRequestedPayload{
		ReleaseID:       next.ID(),
		Service:         next.ChangedService(),
		ImageTag:        imageTag,
		Bucket:          d.Bucket,
		CandidateSchema: CandidateSchemaFor(next.ID()),
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
		MaxRetries:    pkgoutbox.DefaultMaxRetries,
		CreatedAt:     now,
	}); err != nil {
		return fmt.Errorf("outbox insert: %w", err)
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	d.Telemetry.ReleaseParseRequested(ctx, next.ID())
	return nil
}

// uncoveredProdServices returns the sorted set of service names live in the
// current_prod topology that have no service_prod pointer and are not the
// release's changed service. A non-empty result means assembling the full
// topology now would silently drop those services on promote.
func uncoveredProdServices(cp *release.CurrentProd, pointers []*release.ServiceProd, changedService string) []string {
	covered := map[string]struct{}{changedService: {}}
	for _, p := range pointers {
		covered[p.ServiceName()] = struct{}{}
	}

	missingSet := map[string]struct{}{}
	for _, n := range cp.TopologySnapshot() {
		if _, ok := covered[n.ServiceName]; !ok {
			missingSet[n.ServiceName] = struct{}{}
		}
	}

	missing := make([]string, 0, len(missingSet))
	for s := range missingSet {
		missing = append(missing, s)
	}
	sort.Strings(missing)
	return missing
}
