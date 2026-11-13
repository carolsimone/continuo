package runner

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/streams"
)

// flowRejectedRelease is the flow a heal trigger for a rejected release runs.
const flowRejectedRelease = "rejected-release"

// Accepted is one heal trigger as the stream delivered it: the decoded trigger,
// the payload bytes it was decoded from, the message's id and stream fields,
// and the id of the producer's outbox entry when the message carries one.
type Accepted struct {
	Trigger       remediation.Trigger
	Payload       []byte
	MessageID     string
	OutboxEntryID *uuid.UUID
	Fields        map[string]string
}

// Accept records a heal trigger as a running remediation, in one transaction
// that also claims the message: a redelivered message, or the same outbox entry
// republished under a new message id, commits nothing and returns nil. A
// trigger for a release and round that already has a remediation only marks
// its message completed. A trigger with no failing node is acknowledged with
// nothing written. The work itself starts when a worker drives the
// remediation.
func (r *Runner) Accept(ctx context.Context, a Accepted) error {
	t := a.Trigger
	if t.Round < 1 {
		t.Round = 1
	}
	if t.TenantID == "" {
		t.TenantID = events.DefaultTenantID
	}
	if len(t.Nodes) == 0 {
		r.logger.Info("remediation.requested trigger carries no failing nodes — nothing to fix",
			"release", t.ReleaseID, "message_id", a.MessageID)
		return nil
	}

	u := r.d.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin (accept): %w", err)
	}
	defer func() { _ = u.Rollback() }()

	msgID, dup, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, u.MessageProcessingRepo(), r.logger,
		a.MessageID, streams.RemediationRequestedV2, a.Payload, a.OutboxEntryID,
	)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if dup {
		r.logger.Info("duplicate remediation.requested trigger — skipping",
			"message_id", a.MessageID, "release", t.ReleaseID)
		return nil
	}

	now := r.clock.Now()
	rem := remediation.Remediation{
		ID:               uuid.New(),
		TenantID:         t.TenantID,
		ReleaseID:        t.ReleaseID,
		Round:            t.Round,
		Flow:             flowRejectedRelease,
		Status:           remediation.StatusRunning,
		TriggerPayload:   a.Payload,
		TriggerMessageID: a.MessageID,
		TriggerFields:    a.Fields,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	created, err := u.RemediationRepo().Create(ctx, rem)
	if err != nil {
		return fmt.Errorf("create remediation: %w", err)
	}
	if !created {
		r.logger.Info("remediation.requested trigger for a release round that already has a remediation — nothing to start",
			"message_id", a.MessageID, "release", t.ReleaseID, "round", t.Round)
	}
	if err := u.MessageProcessingRepo().UpdateState(ctx, msgID, messageprocessing.StateCompleted); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit (accept): %w", err)
	}
	if created {
		r.logger.Info("remediation accepted", "remediation", rem.ID, "release", t.ReleaseID, "round", t.Round, "nodes", len(t.Nodes))
	}
	return nil
}
