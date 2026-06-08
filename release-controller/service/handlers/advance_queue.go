package handlers

import (
	"context"
	"encoding/json"
	"fmt"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// AdvanceQueue promotes the oldest Received release to Parsing and emits
// release.requested:v1 into the outbox — but only if no release is already
// active (Parsing or Validating). Safe to call repeatedly; it is a no-op when
// the queue is empty or when a release is already in flight.
//
// Assembly of the full manifest-key set happens here (Parsing transition), not
// at receive time. The other services' service_prod pointers can change as
// earlier-queued releases are promoted, so we must read them at the moment this
// release becomes active to guarantee we see the live state for all services.
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

	now := d.Clock.Now()
	if err := next.TransitionToParsing(now); err != nil {
		return fmt.Errorf("transition to parsing: %w", err)
	}

	// Assemble the full manifest-key set now that this release is becoming active.
	// We read the other services' service_prod pointers at this moment so that any
	// promotions from earlier-queued releases are already reflected.
	imageTag := next.ImageTags()[next.ChangedService()]
	set, err := AssembleManifestSet(ctx, u.ServiceProdRepo(), d.Bucket, next.ChangedService(), next.ID(), imageTag)
	if err != nil {
		return fmt.Errorf("assemble manifest set: %w", err)
	}
	next.SetAssembledImageTags(set.ImageTags)

	if err := u.ReleaseRepo().Save(ctx, next); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	type manifestKeyDTO struct {
		Service string `json:"service"`
		S3URI   string `json:"s3_uri"`
	}
	type releaseRequestedPayload struct {
		ReleaseID    string           `json:"release_id"`
		ManifestKeys []manifestKeyDTO `json:"manifest_keys"`
	}
	keys := make([]manifestKeyDTO, len(set.ManifestKeys))
	for i, k := range set.ManifestKeys {
		keys[i] = manifestKeyDTO{Service: k.Service, S3URI: k.S3URI}
	}
	payload, err := json.Marshal(releaseRequestedPayload{
		ReleaseID:    next.ID(),
		ManifestKeys: keys,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if err := u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: "release-controller",
		AggregateID:   AggregateIDForRelease(next.ID()),
		EventType:     "release_requested",
		Payload:       payload,
		StreamName:    streams.ReleaseRequestedV1,
		Status:        "pending",
		MaxRetries:    3,
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
