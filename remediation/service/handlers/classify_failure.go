// Package handlers holds the remediation application layer. Handlers are thin:
// they orchestrate ports and the domain classifier inside a unit of work and
// hold no infrastructure dependencies directly.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/domain/event"
	"github.com/carolsimone/continuo/remediation/domain/failure"
	"github.com/carolsimone/continuo/remediation/domain/repository"
	"github.com/carolsimone/continuo/remediation/service/ports"
	"github.com/carolsimone/continuo/remediation/service/uow"
)

// Deps holds the collaborators ClassifyFailure needs, all behind ports.
type Deps struct {
	NewUoW    func() uow.UnitOfWork
	LogReader ports.LogReader
	Clock     ports.Clock
	Logger    *slog.Logger
}

// ClassifyFailure triages one failed node: it fetches the dbt log, classifies
// it deterministically, and in a single transaction records the decision
// (always — emit and drop alike) and, for a healable and newly-recorded node,
// enqueues a remediation.requested trigger. Idempotency is enforced by the
// decision repository's natural key, so a redelivered rejection neither
// double-records nor double-emits.
func ClassifyFailure(ctx context.Context, deps Deps, ev failure.FailureEvidence) error {
	logText, err := deps.LogReader.Fetch(ctx, ev.DBTLogURI)
	if err != nil {
		if err != ports.ErrLogNotFound {
			return fmt.Errorf("fetch dbt log %q: %w", ev.DBTLogURI, err)
		}
		logText = "" // not found → classify unknown:log_unavailable
	}

	c := failure.Classify(ev, logText)

	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	inserted, err := u.DecisionRepo().Upsert(ctx, repository.ClassificationDecision{
		Source:         ev.Source,
		ReleaseID:      ev.ReleaseID,
		NodeID:         ev.NodeID,
		Category:       c.Category,
		ErrorSignature: c.Signature,
		Decision:       c.Decision,
		Reason:         c.Reason,
		DBTLogURI:      ev.DBTLogURI,
		CreatedAt:      deps.Clock.Now(),
	})
	if err != nil {
		return fmt.Errorf("upsert decision: %w", err)
	}

	if inserted && c.Category.Healable() {
		if err := enqueueTrigger(ctx, u, deps, ev, c); err != nil {
			return err
		}
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	deps.Logger.Info("classified failure",
		"node", ev.NodeID, "release", ev.ReleaseID,
		"category", c.Category, "decision", c.Decision, "reason", c.Reason,
		"emitted", inserted && c.Category.Healable())
	return nil
}

func enqueueTrigger(ctx context.Context, u uow.UnitOfWork, deps Deps, ev failure.FailureEvidence, c failure.Classification) error {
	payload := event.RemediationRequested{
		EventID:         event.RemediationEventID(ev.ReleaseID, ev.NodeID).String(),
		Source:          string(ev.Source),
		ReleaseID:       ev.ReleaseID,
		NodeID:          ev.NodeID,
		Category:        string(c.Category),
		ErrorSignature:  c.Signature,
		DBTLogURI:       ev.DBTLogURI,
		CandidateSQLURI: ev.CandidateSQLURI,
		Repo:            ev.Repo,
		CommitSHA:       ev.CommitSHA,
		ClassifiedAt:    deps.Clock.Now().Format("2006-01-02T15:04:05Z07:00"),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal trigger: %w", err)
	}
	now := deps.Clock.Now()
	return u.OutboxRepo().Create(ctx, &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(payload.EventID)),
		AggregateType: "remediation",
		AggregateID:   event.AggregateIDForRelease(ev.ReleaseID),
		EventType:     event.EventType,
		Payload:       body,
		StreamName:    streams.RemediationRequestedV1,
		Status:        "pending",
		MaxRetries:    outbox.DefaultMaxRetries,
		CreatedAt:     now,
	})
}
