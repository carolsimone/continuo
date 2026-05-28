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
func AdvanceQueue(ctx context.Context, d *Deps) error {
	u := d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer u.Rollback() //nolint:errcheck

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
	if err := u.ReleaseRepo().Save(ctx, next); err != nil {
		return fmt.Errorf("save release: %w", err)
	}

	payload, err := json.Marshal(map[string]string{
		"release_id":    next.ID(),
		"manifests_uri": next.ManifestsURI(),
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
