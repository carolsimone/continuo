// Package handlers holds the agent-remediation application layer. Handlers are
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

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/fixer"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// Trigger is the decoded remediation.requested:v1 payload that drives one
// ProposeFix invocation.
type Trigger struct {
	Source    string
	ReleaseID string
	NodeID    string
	// RelationID is the contested physical relation for a duplicate-relation
	// trigger, distinct from NodeID (the target claimant's own unique_id) —
	// the two differ whenever the target already carries an alias. Empty for
	// every other source.
	RelationID     string
	ErrorSignature string
	Category       string
	// Reason is the classifier's finer-grained reason within Category; with
	// Category it forms the fallback precedent-lookup key when the exact
	// signature has no recorded matches.
	Reason string
	// ErrorExcerpt is the classifier's key error line for this failure (capped
	// at 4 KiB).
	ErrorExcerpt         string
	DBTLogURI            string
	CandidateArtifactURI string
	// CodeBundleURI locates the release's code-bundle document, which carries
	// every parsed node's raw source. Empty only for a compile-stage
	// rejection, which precedes the parse that produces the bundle; every
	// post-parse rejection (duplicate_table included) carries it.
	CodeBundleURI string
	// FilePath is the offending dbt-project-relative source path. For compile
	// failures it is extracted from the dbt log. For validation, seed_build,
	// and duplicate_table failures it is threaded from the candidate topology
	// (OriginalFilePath on release.Node). Empty on a validation or seed_build
	// trigger falls back to the orchestrator graph's NodeLocator; a
	// duplicate_table trigger has no such fallback.
	FilePath string
	// Service is the owning dbt service name for the failing node. Set for
	// validation, seed_build, and duplicate_table failures from the candidate
	// topology. Empty for compile failures where NodeID acts as the service
	// discriminator.
	Service string
	// NodeType is the failing node's kind (dbt-model, dbt-seed,
	// dbt-snapshot, python-model, or python-csv), set on validation and
	// duplicate-relation failures. It selects the Fixer for a validation
	// failure — a python node, whose source is not a single readable file,
	// is fixed in the contract yaml declaring it — and lets the
	// duplicate-table Fixer skip a python node without a topology lookup of
	// its own.
	NodeType string
	// OtherService and OtherFilePath locate the competing node that also
	// produces the contested relation (RelationID), set on a duplicate-relation
	// failure.
	OtherService  string
	OtherFilePath string
	Repo          string
	CommitSHA     string
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

// idempotencyKey identifies this inbound trigger for LLM response caching. It
// mirrors the message-processing dedup identity: the upstream OutboxEntryID when
// present (stable across a Redis republish of the same logical trigger with a
// fresh message id), otherwise the Redis MessageID. Two distinct triggers — for
// example successive remediation attempts for the same failure — never share a
// key, so the cache dedupes redeliveries without suppressing legitimate retries.
func (t Trigger) idempotencyKey() string {
	if t.OutboxEntryID != nil {
		return "outbox:" + t.OutboxEntryID.String()
	}
	return "msg:" + t.MessageID
}

// Deps holds every collaborator ProposeFix needs, all behind ports so the
// handler imports no adapter or infrastructure package.
type Deps struct {
	NewUoW      func() uow.UnitOfWork
	LLM         ports.LLMProvider
	Evidence    ports.EvidenceReader
	Source      ports.SourceReader
	Sanitizer   ports.LogSanitizer
	Artifacts   ports.ArtifactWriter
	Clock       ports.Clock
	Logger      *slog.Logger
	MaxAttempts int
	// ServiceRepoPaths maps a dbt service_name to its project root within the
	// source repo, e.g. "service-1" → "services/service-1". Used by the fixers
	// to build the full GitHub file path for the offending source file.
	ServiceRepoPaths map[string]string
	Locator          ports.NodeLocator
	Upstream         ports.UpstreamChangeReader
	Versions         ports.VersionReader
	Precedents       ports.PrecedentReader
	CandidateSource  ports.CandidateSourceReader
	// RepoArchive fetches a full repo checkout at a commit, for a fixer that
	// needs to search across many files (e.g. locating a python node's
	// declaring contract yaml) rather than read one file at a time.
	RepoArchive ports.RepoArchive
	// ContractLocator finds which contract yaml in that checkout declares a
	// given python node.
	ContractLocator ports.ContractLocator
	// ContractInspector reads the node declarations out of a contract yaml
	// document, so a proposed edit can be checked against the declarations the
	// repository already held.
	ContractInspector ports.ContractInspector
	// Packager merges a directory of python-node contract yaml files into the
	// wire contract a release is submitted with.
	Packager ports.ContractPackager
	// Releases submits shadow verification releases and reads a release's
	// image tag.
	Releases ports.ReleaseGateway
	// PriorAttempts reads the attempts already recorded for a failing node, so
	// a fixer can show the model what earlier attempts tried.
	PriorAttempts repository.AttemptLister
	// SQLDialect is the sqlglot dialect the operator's warehouse engine
	// speaks, used when packaging a proposed contract fix.
	SQLDialect string
}

// ProposeFix turns one healable failure trigger into a fix proposal. It counts
// prior attempts, enforces the per-(node, error_signature) cap, dispatches to
// the per-error-class Fixer resolved by fixer.For, and in a single transaction
// records the proposal row and (on a proposed outcome) enqueues a
// remediation.proposed:v1 outbox trigger.
func ProposeFix(ctx context.Context, deps Deps, t Trigger) error {
	// Read-only dedup pre-check before any write. A redelivery of a trigger that
	// was already handled (including one re-emitted with a fresh Redis message id
	// but the same upstream outbox_entry_id) is ACKed here without touching the
	// database. This is what keeps a completed trigger from minting a phantom
	// in-flight 'generating' row for a fresh attempt number that record()'s dedup
	// claim would then abandon.
	done, err := alreadyProcessed(ctx, deps, t)
	if err != nil {
		return err
	}
	if done {
		deps.Logger.Info("remediation.requested trigger already processed — skipping",
			"message_id", t.MessageID, "node", t.NodeID, "release", t.ReleaseID)
		return nil
	}

	attempts, err := countAttempts(ctx, deps, t)
	if err != nil {
		return err
	}
	attempt := attempts + 1

	// Per-(release, node, error_signature) attempt cap: record escalated, emit nothing.
	if attempts >= deps.MaxAttempts {
		return record(ctx, deps, t, attempt, proposal.Proposal{
			Status: proposal.StatusEscalated,
		}, false, false)
	}

	fx, err := fixer.For(t.Source, t.NodeType)
	if err != nil {
		return err // unknown error class: surfaced loudly, not silently skipped
	}

	// The dbt log is not fetched here: each Fixer reads it (via loadDBTLog) only
	// when its error class needs it, so a class that skips early — e.g. a
	// validation trigger with no candidate SQL — is not blocked by a transiently
	// unreadable log.
	in := fixer.Input{
		Source:               t.Source,
		ReleaseID:            t.ReleaseID,
		NodeID:               t.NodeID,
		RelationID:           t.RelationID,
		ErrorSignature:       t.ErrorSignature,
		Category:             t.Category,
		Reason:               t.Reason,
		ErrorExcerpt:         t.ErrorExcerpt,
		Repo:                 t.Repo,
		CommitSHA:            t.CommitSHA,
		FilePath:             t.FilePath,
		Service:              t.Service,
		NodeType:             t.NodeType,
		OtherService:         t.OtherService,
		OtherFilePath:        t.OtherFilePath,
		DBTLogURI:            t.DBTLogURI,
		CandidateArtifactURI: t.CandidateArtifactURI,
		CodeBundleURI:        t.CodeBundleURI,
		Attempt:              attempt,
	}
	svc := fixer.Services{
		LLM: deps.LLM, Source: deps.Source, Evidence: deps.Evidence,
		Sanitizer: deps.Sanitizer, Artifacts: deps.Artifacts,
		Logger: deps.Logger, ServiceRepoPaths: deps.ServiceRepoPaths,
		Locator: deps.Locator, Upstream: deps.Upstream, Versions: deps.Versions,
		Precedents: deps.Precedents, CandidateSource: deps.CandidateSource,
		Archive: deps.RepoArchive, ContractLocator: deps.ContractLocator,
		ContractInspector: deps.ContractInspector,
		Packager:          deps.Packager, Releases: deps.Releases,
		PriorAttempts: deps.PriorAttempts, SQLDialect: deps.SQLDialect,
	}

	// Mark this attempt in-flight in its own committed transaction, right before
	// the model is called, so the release UI can show a "Generating fix…"
	// indicator for the healable node while the fix is produced. It runs after the
	// attempt-cap guard so an escalated (unhealable) node never shows the chip. A
	// Fixer that internally skips finalizes the row to a terminal state; a brief
	// generating→blank flicker for that case is acceptable.
	if err := markGenerating(ctx, deps, t, attempt); err != nil {
		return err
	}

	// Scope any LLM response caching to this specific inbound trigger. A
	// redelivery of the same trigger reuses the cached completion; a new trigger
	// (e.g. a later attempt for the same failure) misses and calls the model
	// again. The caching decorator reads this key off the context.
	llmCtx := llmcache.ContextWithIdempotencyKey(ctx, t.idempotencyKey())

	r, err := fx.Propose(llmCtx, svc, in)
	if err != nil {
		return err // transient (LLM / non-404 read): message is redelivered
	}
	emit := r.Proposal.Status == proposal.StatusProposed
	return record(ctx, deps, t, attempt, r.Proposal, emit, r.Proposal.SourceResolved, r.SuspectedRoot)
}

// alreadyProcessed opens a read-only transaction and reports whether this
// inbound trigger was already handled, on either dedup axis (message id/stream
// or upstream outbox_entry_id). It performs no write, so re-emitted completed
// triggers are ACKed without claiming a fresh message_processing row.
func alreadyProcessed(ctx context.Context, deps Deps, t Trigger) (bool, error) {
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return false, fmt.Errorf("begin (dedup pre-check): %w", err)
	}
	defer func() { _ = u.Rollback() }()
	done, err := u.MessageProcessingRepo().AlreadyProcessed(
		ctx, t.MessageID, streams.RemediationRequestedV1, t.OutboxEntryID)
	if err != nil {
		return false, fmt.Errorf("dedup pre-check: %w", err)
	}
	return done, nil
}

// countAttempts opens a read-only transaction to count prior TERMINAL proposals
// for this (release, source, node, error_signature) key. The cap is a
// per-release budget — a later release is new code and starts its own count,
// even when the node fails with the same signature. In-flight generating rows
// are excluded by the repository so the in-progress attempt keeps a stable
// attempt number across a redelivery.
func countAttempts(ctx context.Context, deps Deps, t Trigger) (int, error) {
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return 0, fmt.Errorf("begin (count): %w", err)
	}
	defer func() { _ = u.Rollback() }()
	n, err := u.ProposalRepo().CountAttempts(ctx, t.ReleaseID, t.Source, t.NodeID, t.ErrorSignature)
	if err != nil {
		return 0, fmt.Errorf("count attempts: %w", err)
	}
	return n, nil
}

// markGenerating writes an in-flight 'generating' proposal row for the attempt
// in its own committed transaction, just before the model is called. The insert
// is idempotent (ON CONFLICT DO NOTHING on the attempt's natural key), so a
// redelivery of an in-flight attempt re-uses the existing row rather than
// creating a second one.
func markGenerating(ctx context.Context, deps Deps, t Trigger, attempt int) error {
	u := deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return fmt.Errorf("begin (mark generating): %w", err)
	}
	defer func() { _ = u.Rollback() }()
	p := proposal.Proposal{
		Source:         t.Source,
		ReleaseID:      t.ReleaseID,
		NodeID:         t.NodeID,
		ErrorSignature: t.ErrorSignature,
		Attempt:        attempt,
		Status:         proposal.StatusGenerating,
		CreatedAt:      deps.Clock.Now(),
	}
	if err := u.ProposalRepo().InsertGenerating(ctx, p); err != nil {
		return fmt.Errorf("mark generating: %w", err)
	}
	if err := u.Commit(); err != nil {
		return fmt.Errorf("commit (mark generating): %w", err)
	}
	return nil
}

// record inserts the proposal row and, when emit is true, enqueues the
// remediation.proposed:v1 outbox trigger — all in a single transaction.
// sourceResolved indicates whether the fixer resolved and rewrote the real
// source file; it is threaded into the outbox event. The variadic
// suspectedRoot lets successful proposals forward the optional LLM field
// without a separate struct.
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
	// An attempt whose fix is being verified by a shadow release stores the
	// trigger that produced it, so the reconciler resolving that release can
	// rebuild the trigger and retry with the shadow's error as new evidence.
	// Every other status resolves within this call and needs nothing to replay.
	if p.Status == proposal.StatusVerifying {
		p.TriggerPayload = t.RawPayload
	}

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

	// Normalize the single-file scalars to edits[0] on the finalized aggregate,
	// before it is either persisted or turned into the outbox payload below, so
	// the stored row and the emitted event cannot disagree about which file was
	// fixed.
	p.NormalizeSingleFileView()
	if err := u.ProposalRepo().Upsert(ctx, p); err != nil {
		return fmt.Errorf("upsert proposal: %w", err)
	}
	if emit {
		root := ""
		if len(suspectedRoot) > 0 {
			root = suspectedRoot[0]
		}
		if err := Enqueue(ctx, u, deps.Clock, p, root, sourceResolved, msgProcID); err != nil {
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

// Enqueue builds the deterministic remediation.proposed:v1 outbox entry for a
// proposal that is ready for human review, and creates it on the repository
// bound to the caller's transaction. Two writers announce a fix and both go
// through here, so the event they emit cannot drift apart: the driver, for a
// fix judged as it was produced, and the shadow-verification reconciler, for
// one a release proved afterwards. p carries the whole announcement, including
// the failure it addresses (source, release, node, error signature).
// sourceResolved indicates whether the fix rewrote real version-controlled
// source. msgProcID is the message_processing row UUID of the inbound trigger
// behind the write, stored on the outbox entry for provenance; uuid.Nil for a
// write no inbound message drove.
func Enqueue(ctx context.Context, u uow.UnitOfWork, clock ports.Clock, p proposal.Proposal, suspectedRoot string, sourceResolved bool, msgProcID uuid.UUID) error {
	eventID := event.RemediationEventID(p.ReleaseID, p.NodeID, p.Attempt)
	payload := event.RemediationProposed{
		EventID:                eventID.String(),
		Source:                 p.Source,
		ReleaseID:              p.ReleaseID,
		NodeID:                 p.NodeID,
		ErrorSignature:         p.ErrorSignature,
		ProposedSQLURI:         p.ProposedSQLURI,
		DiffURI:                p.DiffURI,
		Rationale:              p.Rationale,
		Confidence:             string(p.Confidence),
		SuspectedRootCauseNode: suspectedRoot,
		Model:                  p.Model,
		Attempt:                p.Attempt,
		SourceResolved:         sourceResolved,
		ProposedAt:             clock.Now().Format(time.RFC3339),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal proposed event: %w", err)
	}
	now := clock.Now()
	entry := &outbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte(eventID.String())),
		AggregateType: "remediation_agent",
		AggregateID:   event.AggregateIDForRelease(p.ReleaseID),
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
