// Package handlers holds the remediation-agent application layer. Handlers are
// thin: they orchestrate ports and the domain prompt/diff inside a unit of work
// and hold no infrastructure dependencies directly.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation-agent/domain/event"
	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
	"github.com/carolsimone/continuo/remediation-agent/service/uow"
)

// Trigger is the decoded remediation.requested:v1 payload that drives one
// ProposeFix invocation.
type Trigger struct {
	Source          string
	ReleaseID       string
	NodeID          string
	ErrorSignature  string
	Category        string
	DBTLogURI       string
	CandidateSQLURI string
	Repo            string
	CommitSHA       string
}

// Deps holds every collaborator ProposeFix needs, all behind ports so the
// handler imports no adapter or infrastructure package.
type Deps struct {
	NewUoW      func() uow.UnitOfWork
	LLM         ports.LLMProvider
	Evidence    ports.EvidenceReader
	Ancestry    ports.AncestryClient
	Sanitizer   ports.LogSanitizer
	Artifacts   ports.ArtifactWriter
	Clock       ports.Clock
	Logger      *slog.Logger
	MaxAttempts int
	Bucket      string
}

// ProposeFix turns one healable failure trigger into a fix proposal. It counts
// prior attempts, enforces the per-(node, error_signature) cap, calls the LLM
// behind the sanitizer seam, writes the proposed SQL and diff to object
// storage, and in a single transaction records the proposal row and (on
// success) enqueues a remediation.proposed:v1 outbox trigger.
func ProposeFix(ctx context.Context, deps Deps, t Trigger) error {
	attempts, err := countAttempts(ctx, deps, t)
	if err != nil {
		return err
	}
	attempt := attempts + 1

	// Per-(node, error_signature) attempt cap: record escalated, emit nothing.
	if attempts >= deps.MaxAttempts {
		return record(ctx, deps, t, attempt, proposal.Proposal{
			Status: proposal.StatusEscalated,
		}, false)
	}

	// No candidate SQL (e.g. seed nodes): record skipped, emit nothing.
	if t.CandidateSQLURI == "" {
		return record(ctx, deps, t, attempt, proposal.Proposal{
			Status: proposal.StatusSkipped,
		}, false)
	}

	candidateSQL, err := deps.Evidence.Fetch(ctx, t.CandidateSQLURI)
	if err != nil {
		return fmt.Errorf("fetch candidate sql: %w", err)
	}
	rawLog, err := deps.Evidence.Fetch(ctx, t.DBTLogURI)
	if err != nil && err != ports.ErrNotFound {
		return fmt.Errorf("fetch dbt log: %w", err)
	}
	dbtLog := deps.Sanitizer.Sanitize(rawLog)

	ancestors, err := deps.Ancestry.Ancestors(ctx, t.NodeID)
	if err != nil {
		// Ancestry is best-effort: proceed without upstream context.
		deps.Logger.Warn("ancestry unavailable; proceeding without upstream context",
			"node", t.NodeID, "error", err)
		ancestors = nil
	}

	res, err := deps.LLM.Propose(ctx, prompt.Assemble(prompt.Evidence{
		NodeID:         t.NodeID,
		ErrorSignature: t.ErrorSignature,
		CandidateSQL:   candidateSQL,
		DBTLog:         dbtLog,
		Repo:           t.Repo,
		CommitSHA:      t.CommitSHA,
		Ancestors:      ancestors,
	}))
	if err != nil {
		// Transient LLM error: return so the message is redelivered.
		return fmt.Errorf("llm propose: %w", err)
	}
	if res.ProposedSQL == "" {
		return record(ctx, deps, t, attempt, proposal.Proposal{
			Status: proposal.StatusFailed,
		}, false)
	}

	// Write proposed SQL and its unified diff to object storage.
	sqlKey := fmt.Sprintf("proposed-fix/%s/%s.sql", t.ReleaseID, t.NodeID)
	diffKey := fmt.Sprintf("proposed-fix/%s/%s.diff", t.ReleaseID, t.NodeID)

	sqlURI, err := deps.Artifacts.Write(ctx, sqlKey, res.ProposedSQL, "text/plain")
	if err != nil {
		return fmt.Errorf("write proposed sql: %w", err)
	}
	diffBody := proposal.ComputeUnifiedDiff(candidateSQL, res.ProposedSQL, t.NodeID)
	diffURI, err := deps.Artifacts.Write(ctx, diffKey, diffBody, "text/plain")
	if err != nil {
		return fmt.Errorf("write diff: %w", err)
	}

	return record(ctx, deps, t, attempt, proposal.Proposal{
		Status:         proposal.StatusProposed,
		Confidence:     normalizeConfidence(res.Confidence),
		Rationale:      res.Rationale,
		ProposedSQLURI: sqlURI,
		DiffURI:        diffURI,
		Model:          res.Model,
	}, true, res.SuspectedRootCauseNode)
}

// countAttempts opens a read-only transaction to count prior proposals for
// this (source, node, error_signature) triple.
func countAttempts(ctx context.Context, deps Deps, t Trigger) (int, error) {
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return 0, fmt.Errorf("begin (count): %w", err)
	}
	defer func() { _ = u.Rollback() }()
	n, err := u.ProposalRepo().CountAttempts(ctx, t.Source, t.NodeID, t.ErrorSignature)
	if err != nil {
		return 0, fmt.Errorf("count attempts: %w", err)
	}
	return n, nil
}

// record inserts the proposal row and, when emit is true, enqueues the
// remediation.proposed:v1 outbox trigger — all in a single transaction.
// The variadic suspectedRoot lets successful proposals forward the optional
// LLM field without a separate struct.
func record(ctx context.Context, deps Deps, t Trigger, attempt int, p proposal.Proposal, emit bool, suspectedRoot ...string) error {
	p.Source = t.Source
	p.ReleaseID = t.ReleaseID
	p.NodeID = t.NodeID
	p.ErrorSignature = t.ErrorSignature
	p.Attempt = attempt
	p.CreatedAt = deps.Clock.Now()

	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = u.Rollback() }()

	if err := u.ProposalRepo().Insert(ctx, p); err != nil {
		return fmt.Errorf("insert proposal: %w", err)
	}
	if emit {
		root := ""
		if len(suspectedRoot) > 0 {
			root = suspectedRoot[0]
		}
		if err := enqueue(ctx, u, deps, t, p, root); err != nil {
			return err
		}
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	deps.Logger.Info("proposal recorded",
		"node", t.NodeID, "release", t.ReleaseID,
		"status", p.Status, "attempt", attempt, "emitted", emit)
	return nil
}

// enqueue builds the deterministic remediation.proposed:v1 outbox entry and
// creates it on the repository bound to the caller's transaction.
func enqueue(ctx context.Context, u uow.UnitOfWork, deps Deps, t Trigger, p proposal.Proposal, suspectedRoot string) error {
	eventID := event.RemediationEventID(t.ReleaseID, t.NodeID, p.Attempt)
	payload := event.RemediationProposed{
		EventID:                eventID.String(),
		Source:                 t.Source,
		ReleaseID:              t.ReleaseID,
		NodeID:                 t.NodeID,
		ErrorSignature:         t.ErrorSignature,
		ProposedSQLURI:         p.ProposedSQLURI,
		DiffURI:                p.DiffURI,
		Rationale:              p.Rationale,
		Confidence:             string(p.Confidence),
		SuspectedRootCauseNode: suspectedRoot,
		Model:                  p.Model,
		Attempt:                p.Attempt,
		ProposedAt:             deps.Clock.Now().Format(time.RFC3339),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal proposed event: %w", err)
	}
	now := deps.Clock.Now()
	return u.OutboxRepo().Create(ctx, &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID.String())),
		AggregateType: "remediation_agent",
		AggregateID:   event.AggregateIDForRelease(t.ReleaseID),
		EventType:     event.EventType,
		Payload:       body,
		StreamName:    streams.RemediationProposedV1,
		Status:        "pending",
		MaxRetries:    outbox.DefaultMaxRetries,
		CreatedAt:     now,
	})
}

// normalizeConfidence maps the LLM's free-form confidence string to the
// domain's three-value enum, defaulting to medium on unrecognised values.
func normalizeConfidence(c string) proposal.Confidence {
	switch c {
	case "low":
		return proposal.ConfidenceLow
	case "high":
		return proposal.ConfidenceHigh
	default:
		return proposal.ConfidenceMedium
	}
}
