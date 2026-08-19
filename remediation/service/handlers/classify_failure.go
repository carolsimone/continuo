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

// ClassifyFailure triages one failed node: it gathers whatever evidence the
// source needs, classifies it deterministically, and in a single transaction
// records the decision (always — emit and drop alike) and, for a healable and
// newly-recorded node, enqueues a remediation.requested trigger. Idempotency is
// enforced by the decision repository's natural key, so a redelivered
// rejection neither double-records nor double-emits.
func ClassifyFailure(ctx context.Context, deps Deps, ev failure.FailureEvidence) error {
	c, err := classify(ctx, deps, &ev)
	if err != nil {
		return err
	}

	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	// A shadow release verifies a remediation-agent fix proposal: it never
	// promotes and never touches current_prod, so its rejection means the
	// proposed fix did not work. The decision is still recorded — so the drop
	// is never invisible — but overridden to drop, because emitting a trigger
	// for it would remediate a failed fix attempt with another fix attempt,
	// looping forever.
	decision, reason := c.Decision, c.Reason
	if ev.Shadow {
		decision, reason = failure.DecisionDrop, "shadow_verification"
	}

	inserted, err := u.DecisionRepo().Upsert(ctx, repository.ClassificationDecision{
		Source:         ev.Source,
		ReleaseID:      ev.ReleaseID,
		NodeID:         ev.NodeID,
		Category:       c.Category,
		ErrorSignature: c.Signature,
		Decision:       decision,
		Reason:         reason,
		DBTLogURI:      ev.DBTLogURI,
		CreatedAt:      deps.Clock.Now(),
	})
	if err != nil {
		return fmt.Errorf("upsert decision: %w", err)
	}

	emitted := inserted && c.Category.Healable() && !ev.Shadow
	if emitted {
		if err := enqueueTrigger(ctx, u, deps, ev, c); err != nil {
			return err
		}
	}

	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	deps.Logger.Info("classified failure",
		"node", ev.NodeID, "release", ev.ReleaseID,
		"category", c.Category, "decision", decision, "reason", reason,
		"shadow", ev.Shadow, "emitted", emitted)
	return nil
}

// classify produces the classification for one piece of evidence, fetching only
// what the source actually needs. A duplicate-relation rejection is classified
// from the evidence alone: it happens at parse time, before any Job runs, so
// there is no dbt log to read. ev is a pointer because the compile path fills
// in FilePath from the log text.
func classify(ctx context.Context, deps Deps, ev *failure.FailureEvidence) (failure.Classification, error) {
	if ev.Source == failure.SourceDuplicateTable {
		return failure.ClassifyDuplicateTable(*ev), nil
	}

	logText, err := deps.LogReader.Fetch(ctx, ev.DBTLogURI)
	if err != nil {
		if err != ports.ErrLogNotFound {
			return failure.Classification{}, fmt.Errorf("fetch dbt log %q: %w", ev.DBTLogURI, err)
		}
		logText = "" // not found → classify unknown:log_unavailable (or structured, below)
	}

	// Prefer the structured validation result when present; a fetch/parse failure
	// degrades to the text log rather than failing the message.
	var structured *failure.StructuredResult
	if ev.RunResultsURI != "" {
		body, ferr := deps.LogReader.Fetch(ctx, ev.RunResultsURI)
		if ferr != nil && ferr != ports.ErrLogNotFound {
			return failure.Classification{}, fmt.Errorf("fetch run results %q: %w", ev.RunResultsURI, ferr)
		}
		if ferr == nil {
			if sr, perr := failure.ParseStructuredResult([]byte(body)); perr != nil {
				deps.Logger.Warn("run_results parse failed — falling back to text log",
					"uri", ev.RunResultsURI, "error", perr)
			} else {
				structured = sr
			}
		}
	}

	// For compile-stage failures, extract the offending source file path from
	// the log text so the remediation agent can read the file directly. Compile
	// failures have a synthetic service-name NodeID (not a real dbt node), so
	// the log is the only source of the file path. Seed_build failures carry
	// FilePath and Service from the candidate topology via the rejection
	// payload, so no extraction is needed.
	if ev.Source == failure.SourceCompile && ev.FilePath == "" {
		ev.FilePath = failure.ExtractDbtFilePath(logText)
	}

	return failure.ClassifyWithStructured(*ev, structured, logText), nil
}

func enqueueTrigger(ctx context.Context, u uow.UnitOfWork, deps Deps, ev failure.FailureEvidence, c failure.Classification) error {
	payload := event.RemediationRequested{
		EventID:              event.RemediationEventID(ev.ReleaseID, ev.NodeID).String(),
		Source:               string(ev.Source),
		ReleaseID:            ev.ReleaseID,
		NodeID:               ev.NodeID,
		RelationID:           ev.RelationID,
		Category:             string(c.Category),
		ErrorSignature:       c.Signature,
		Reason:               c.Reason,
		ErrorExcerpt:         c.Excerpt,
		CodeBundleURI:        ev.CodeBundleURI,
		DBTLogURI:            ev.DBTLogURI,
		CandidateArtifactURI: ev.CandidateArtifactURI,
		FilePath:             ev.FilePath,
		Service:              ev.Service,
		NodeType:             ev.NodeType,
		OtherService:         ev.OtherService,
		OtherFilePath:        ev.OtherFilePath,
		Repo:                 ev.Repo,
		CommitSHA:            ev.CommitSHA,
		ClassifiedAt:         deps.Clock.Now().Format("2006-01-02T15:04:05Z07:00"),
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
