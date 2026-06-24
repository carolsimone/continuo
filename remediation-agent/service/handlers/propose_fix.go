// Package handlers holds the remediation-agent application layer. Handlers are
// thin: they orchestrate ports and the domain prompt/diff inside a unit of work
// and hold no infrastructure dependencies directly.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/pkg/messageprocessing"
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
	// MessageID is the Redis Stream message ID of the inbound
	// remediation.requested:v1 message. It is the primary dedup key.
	MessageID string
	// OutboxEntryID, when non-nil, is the upstream pkg/outbox row UUID carried
	// in the message's outbox_entry_id field. It provides a secondary dedup axis
	// that catches the case where the classifier's outbox Processor crashed
	// between XADD and MarkProcessed and republished the same outbox row with a
	// fresh Redis message_id.
	OutboxEntryID *uuid.UUID
	// RawPayload is the raw bytes of the message payload, stored in the
	// message_processing row for audit/replay purposes.
	RawPayload []byte
}

// Deps holds every collaborator ProposeFix needs, all behind ports so the
// handler imports no adapter or infrastructure package.
type Deps struct {
	NewUoW      func() uow.UnitOfWork
	LLM         ports.LLMProvider
	Evidence    ports.EvidenceReader
	Ancestry    ports.AncestryClient
	Source      ports.SourceReader
	Sanitizer   ports.LogSanitizer
	Artifacts   ports.ArtifactWriter
	Clock       ports.Clock
	Logger      *slog.Logger
	MaxAttempts int
	Bucket      string
	// ServiceRepoPaths maps a dbt service_name to its project root within the
	// source repo, e.g. "service-1" → "services/service-1". Used to construct
	// the full GitHub file path for Step-2 source resolution.
	ServiceRepoPaths map[string]string
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
		}, false, false)
	}

	// No candidate SQL (e.g. seed nodes): record skipped, emit nothing.
	if t.CandidateSQLURI == "" {
		return record(ctx, deps, t, attempt, proposal.Proposal{
			Status: proposal.StatusSkipped,
		}, false, false)
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

	// Ancestry is best-effort: proceed without upstream context on error.
	// filePath and serviceName are forwarded to resolveSource so it does not
	// need a second NodeContext call.
	filePath, serviceName, ancestors, err := deps.Ancestry.NodeContext(ctx, t.NodeID)
	if err != nil {
		deps.Logger.Warn("ancestry unavailable; proceeding without upstream context",
			"node", t.NodeID, "error", err)
		ancestors = nil
		filePath, serviceName = "", ""
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
		}, false, false)
	}

	// Step 1 — candidate artifacts. Written unconditionally for audit: the
	// candidate is the LLM's fix applied to the pre-compiled SQL extracted from
	// object storage (not the real model source).
	candSQLKey := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.sql", t.ReleaseID, t.NodeID, attempt)
	candDiffKey := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.diff", t.ReleaseID, t.NodeID, attempt)

	candSQLURI, err := deps.Artifacts.Write(ctx, candSQLKey, res.ProposedSQL, "text/plain")
	if err != nil {
		return fmt.Errorf("write candidate sql: %w", err)
	}
	candDiffURI, err := deps.Artifacts.Write(ctx, candDiffKey, proposal.ComputeUnifiedDiff(candidateSQL, res.ProposedSQL, t.NodeID), "text/plain")
	if err != nil {
		return fmt.Errorf("write candidate diff: %w", err)
	}

	// Defaults: final URIs fall back to the candidate unless Step 2 succeeds.
	finalSQLURI, finalDiffURI := candSQLURI, candDiffURI
	sourceResolved := false

	// Step 2 — real-source fix. Fetches the model source from version control
	// and asks the LLM to apply the Step-1 diagnosis to it. Degrades silently
	// when the file path, service mapping, source read, or LLM result is
	// unavailable, or when the LLM did not improve the source.
	if src, ok := deps.resolveSource(ctx, t, filePath, serviceName, res); ok {
		srcDiff := proposal.ComputeUnifiedDiff(src.original, src.corrected, t.NodeID)
		srcSQLURI, err := deps.Artifacts.Write(ctx,
			fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source.sql", t.ReleaseID, t.NodeID, attempt),
			src.corrected, "text/plain")
		if err != nil {
			return fmt.Errorf("write source sql: %w", err)
		}
		srcDiffURI, err := deps.Artifacts.Write(ctx,
			fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source.diff", t.ReleaseID, t.NodeID, attempt),
			srcDiff, "text/plain")
		if err != nil {
			return fmt.Errorf("write source diff: %w", err)
		}
		finalSQLURI, finalDiffURI, sourceResolved = srcSQLURI, srcDiffURI, true
	}

	return record(ctx, deps, t, attempt, proposal.Proposal{
		Status:              proposal.StatusProposed,
		Confidence:          normalizeConfidence(res.Confidence),
		Rationale:           res.Rationale,
		ProposedSQLURI:      finalSQLURI,
		DiffURI:             finalDiffURI,
		CandidateFixSQLURI:  candSQLURI,
		CandidateFixDiffURI: candDiffURI,
		SourceResolved:      sourceResolved,
		Model:               res.Model,
	}, true, sourceResolved, res.SuspectedRootCauseNode)
}

// resolvedSource holds the original model source and the Step-2 corrected
// version produced by the LLM.
type resolvedSource struct{ original, corrected string }

// resolveSource performs Step 2: fetch the real model source from version
// control and ask the LLM to apply the Step-1 diagnosis to it. filePath and
// serviceName come from the single NodeContext call already made by the caller.
// Returns ok=false on any degraded path (missing file path or service name, no
// repo mapping, source read error, empty/unchanged LLM result, or
// low-confidence LLM result); the caller then keeps the candidate proposal.
func (d Deps) resolveSource(ctx context.Context, t Trigger, filePath, serviceName string, step1 ports.ProposeResult) (resolvedSource, bool) {
	if filePath == "" || serviceName == "" {
		d.Logger.Warn("source fix: file path or service name unavailable; using candidate proposal",
			"node", t.NodeID, "file_path", filePath, "service_name", serviceName)
		return resolvedSource{}, false
	}
	repoPrefix, ok := d.ServiceRepoPaths[serviceName]
	if !ok {
		d.Logger.Warn("source fix: no repo path mapping for service; using candidate proposal",
			"node", t.NodeID, "service_name", serviceName)
		return resolvedSource{}, false
	}
	fullPath := path.Join(repoPrefix, filePath)
	original, err := d.Source.ReadFile(ctx, t.Repo, t.CommitSHA, fullPath)
	if err != nil {
		d.Logger.Warn("source fix: github read failed; using candidate proposal",
			"node", t.NodeID, "path", fullPath, "error", err)
		return resolvedSource{}, false
	}
	out, err := d.LLM.Propose(ctx, prompt.AssembleSourceFix(d.Sanitizer.Sanitize(original), t.NodeID, step1.Rationale))
	if err != nil {
		d.Logger.Warn("source fix: llm step 2 failed; using candidate proposal",
			"node", t.NodeID, "error", err)
		return resolvedSource{}, false
	}
	if out.ProposedSQL == "" || out.ProposedSQL == original {
		d.Logger.Warn("source fix: llm step 2 produced no improvement; using candidate proposal",
			"node", t.NodeID)
		return resolvedSource{}, false
	}
	if strings.EqualFold(out.Confidence, "low") {
		d.Logger.Warn("source fix: llm step 2 low confidence; using candidate proposal",
			"node", t.NodeID)
		return resolvedSource{}, false
	}
	return resolvedSource{original: original, corrected: out.ProposedSQL}, true
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
// sourceResolved indicates whether the real-source Step-2 fix succeeded; it is
// threaded into the outbox event. The variadic suspectedRoot lets successful
// proposals forward the optional LLM field without a separate struct.
//
// Inbound dedup is performed atomically inside the transaction: the
// message_processing claim, the proposal insert, and the optional outbox enqueue
// all commit or roll back together. A redelivered trigger collides on the claim
// and causes a rollback with a nil return (consumer ACKs, no duplicate written).
// A transient error rolls back without persisting the claim, so the message is
// cleanly retried.
func record(ctx context.Context, deps Deps, t Trigger, attempt int, p proposal.Proposal, emit bool, sourceResolved bool, suspectedRoot ...string) error {
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

	// Claim this inbound trigger atomically within the write transaction. A
	// duplicate (redelivered or replayed message) returns dup=true: log and
	// return nil so the consumer ACKs without writing anything. The rollback
	// deferred above discards the tx without persisting the claim.
	msgProcID, dup, err := messageprocessing.DedupWithOutboxEntryID(
		ctx, u.MessageProcessingRepo(), deps.Logger,
		t.MessageID, streams.RemediationRequestedV1, t.RawPayload, t.OutboxEntryID,
	)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if dup {
		deps.Logger.Info("duplicate remediation.requested trigger — skipping",
			"message_id", t.MessageID, "node", t.NodeID, "release", t.ReleaseID)
		return nil
	}

	if err := u.ProposalRepo().Insert(ctx, p); err != nil {
		return fmt.Errorf("insert proposal: %w", err)
	}
	if emit {
		root := ""
		if len(suspectedRoot) > 0 {
			root = suspectedRoot[0]
		}
		if err := enqueue(ctx, u, deps, t, p, root, sourceResolved, msgProcID); err != nil {
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
// sourceResolved indicates whether the real-source Step-2 fix succeeded.
// msgProcID is the message_processing row UUID for the inbound trigger;
// it is stored on the outbox entry for provenance.
func enqueue(ctx context.Context, u uow.UnitOfWork, deps Deps, t Trigger, p proposal.Proposal, suspectedRoot string, sourceResolved bool, msgProcID uuid.UUID) error {
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
		SourceResolved:         sourceResolved,
		ProposedAt:             deps.Clock.Now().Format(time.RFC3339),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal proposed event: %w", err)
	}
	now := deps.Clock.Now()
	entry := &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID.String())),
		AggregateType: "remediation_agent",
		AggregateID:   event.AggregateIDForRelease(t.ReleaseID),
		EventType:     event.EventType,
		Payload:       body,
		StreamName:    streams.RemediationProposedV1,
		Status:        "pending",
		MaxRetries:    outbox.DefaultMaxRetries,
		CreatedAt:     now,
	}
	if msgProcID != uuid.Nil {
		id := msgProcID
		entry.MessageProcessingID = &id
	}
	return u.OutboxRepo().Create(ctx, entry)
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
