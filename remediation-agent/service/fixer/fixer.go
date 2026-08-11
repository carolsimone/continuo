// Package fixer holds the per-error-class fix strategies for the
// remediation-agent. Each error class (compile, seed_build, validation) is a
// Fixer that decides which source files to read, which prompt to send, and how
// to read the model's answer. The shared driver in service/handlers owns the
// attempt cap, dedup, proposal row, and outbox emit; a Fixer only produces the
// proposal. This package imports no adapter: every collaborator is a port.
package fixer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// Input is the failure evidence a Fixer needs, projected from the inbound
// remediation.requested trigger. DBTLogURI is the raw object-storage URI of the
// dbt log; each Fixer fetches and sanitizes it (via loadDBTLog) only when its
// error class actually needs it, so a class that can skip early does not depend
// on the log being readable.
type Input struct {
	Source    string
	ReleaseID string
	NodeID    string
	// RelationID is the contested physical relation for a duplicate-relation
	// failure, distinct from NodeID (the target claimant's own unique_id) —
	// the two differ whenever the target already carries an alias. The
	// duplicate-table Fixer's prompt names RelationID, not NodeID, as the
	// relation the model must stop producing.
	RelationID     string
	ErrorSignature string
	Repo           string
	CommitSHA      string
	FilePath       string
	Service        string
	// NodeType is the target claimant's kind, set on a duplicate-relation
	// failure. The duplicate-table Fixer skips a python target — its relation
	// is declared in the service's contract.yaml, whose repository path this
	// system does not carry, so the file named by FilePath cannot contain the
	// fix.
	NodeType string
	// OtherService and OtherFilePath locate the competing node that also
	// produces NodeID. Set on a duplicate-relation failure so the prompt can
	// name the relation's other producer without reading its source.
	OtherService         string
	OtherFilePath        string
	DBTLogURI            string
	CandidateArtifactURI string
	Attempt              int
}

// Services bundles the ports a Fixer uses to produce a proposal.
type Services struct {
	LLM              ports.LLMProvider
	Source           ports.SourceReader
	Evidence         ports.EvidenceReader
	Sanitizer        ports.LogSanitizer
	Ancestry         ports.AncestryClient
	Artifacts        ports.ArtifactWriter
	Logger           *slog.Logger
	ServiceRepoPaths map[string]string
}

// Result is what a Fixer returns: the proposal (Status always set; artifact
// URIs, diff, and FilePath populated on a proposed outcome) plus the optional
// suspected-root-cause node forwarded to the outbox event.
type Result struct {
	Proposal      proposal.Proposal
	SuspectedRoot string
}

// Gathered holds every source file a single-shot Fixer read, keyed by
// repo-relative path, in a deterministic Order for prompt rendering. Primary
// is the offending file (the compile .sql or the seed .csv).
type Gathered struct {
	Files   map[string]string
	Order   []string
	Primary string
}

// Outcome is the interpreted LLM answer for a single-shot Fixer.
type Outcome struct {
	Status           proposal.Status
	TargetFile       string
	CorrectedContent string
	Confidence       string
	Rationale        string
	Model            string
	SuspectedRoot    string
}

// Fixer produces a fix proposal from failure evidence for one error class.
type Fixer interface {
	Propose(ctx context.Context, svc Services, in Input) (Result, error)
}

// For resolves the Fixer for a trigger's error class. An unknown source is a
// programming error (the classifier produces only the four known values).
func For(source string) (Fixer, error) {
	switch source {
	case sourceCompile:
		return compileFixer{}, nil
	case sourceSeed:
		return seedFixer{}, nil
	case sourceValidation:
		return validationFixer{}, nil
	case sourceDuplicateTable:
		return duplicateTableFixer{}, nil
	default:
		return nil, fmt.Errorf("fixer: unknown error class %q", source)
	}
}

// Error-class discriminators carried on remediation.requested:v1.
const (
	sourceCompile        = "compile"
	sourceSeed           = "seed_build"
	sourceValidation     = "validation"
	sourceDuplicateTable = "duplicate_table"
)

// loadDBTLog fetches and sanitizes the dbt log a Fixer needs for its prompt. An
// empty uri means this failure class has no log at all (e.g. a duplicate-table
// rejection, which happens at parse time before any dbt Job runs), so it
// returns an empty string without touching the evidence reader. A missing log
// (ErrNotFound) is likewise not fatal — a fix can still be proposed from the
// other evidence — so it also yields an empty string; any other fetch error is
// transient and returned so the driver redelivers. Each Fixer calls this only
// when its error class actually needs the log, keeping the read off the paths
// (e.g. an empty validation candidate) that skip before proposing anything.
func loadDBTLog(ctx context.Context, svc Services, uri string) (string, error) {
	if uri == "" {
		return "", nil
	}
	raw, err := svc.Evidence.Fetch(ctx, uri)
	if err != nil && !errors.Is(err, ports.ErrNotFound) {
		return "", fmt.Errorf("fetch dbt log: %w", err)
	}
	return svc.Sanitizer.Sanitize(raw), nil
}

// isLowConfidence reports whether the model's free-form confidence is "low"
// (case-insensitive). A low-confidence answer is the model's signal that it
// could not determine a safe fix, so a Fixer must not turn it into a proposal.
func isLowConfidence(c string) bool {
	return strings.EqualFold(strings.TrimSpace(c), "low")
}

// writeSourceArtifacts writes the corrected source and its unified diff (against
// the original) as the attempt's source artifacts and returns their URIs. Both
// singleShot (compile/seed) and the validation fixer's real-source step use it,
// so the artifact key layout and content type live in one place.
func writeSourceArtifacts(ctx context.Context, svc Services, in Input, original, corrected string) (sqlURI, diffURI string, err error) {
	diff := proposal.ComputeUnifiedDiff(original, corrected, in.NodeID)
	sqlURI, err = svc.Artifacts.Write(ctx,
		fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source.sql", in.ReleaseID, in.NodeID, in.Attempt),
		corrected, "text/plain")
	if err != nil {
		return "", "", fmt.Errorf("write source sql: %w", err)
	}
	diffURI, err = svc.Artifacts.Write(ctx,
		fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source.diff", in.ReleaseID, in.NodeID, in.Attempt),
		diff, "text/plain")
	if err != nil {
		return "", "", fmt.Errorf("write source diff: %w", err)
	}
	return sqlURI, diffURI, nil
}

// singleShot builds a Fixer whose flow is: gather source files → build one
// prompt → one LLM call → interpret → (on a proposed outcome) diff the chosen
// file against its original content and write the source + diff artifacts.
// compileFixer and seedFixer embed it.
type singleShot struct {
	gather    func(ctx context.Context, svc Services, in Input) (Gathered, bool, error)
	build     func(svc Services, g Gathered, in Input, dbtLog string) prompt.ProposeRequest
	interpret func(res ports.ProposeResult, g Gathered, in Input) Outcome
}

func (s singleShot) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	g, skip, err := s.gather(ctx, svc, in)
	if err != nil {
		return Result{}, err // transient (non-404) read error: driver redelivers
	}
	if skip {
		return Result{Proposal: proposal.Proposal{Status: proposal.StatusSkipped}}, nil
	}
	// The log is fetched only after gather commits to producing a fix, so a
	// skipped class never depends on the log being readable.
	dbtLog, err := loadDBTLog(ctx, svc, in.DBTLogURI)
	if err != nil {
		return Result{}, err // transient log read error: driver redelivers
	}
	res, err := svc.LLM.Propose(ctx, s.build(svc, g, in, dbtLog))
	if err != nil {
		return Result{}, fmt.Errorf("llm propose: %w", err)
	}
	out := s.interpret(res, g, in)
	if out.Status != proposal.StatusProposed {
		return Result{Proposal: proposal.Proposal{Status: out.Status}}, nil
	}
	// Diff and artifacts are computed against the raw original (g.Files), not the
	// sanitized copy sent to the LLM, because the fix is applied to the real file.
	original := g.Files[out.TargetFile]
	sqlURI, diffURI, err := writeSourceArtifacts(ctx, svc, in, original, out.CorrectedContent)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Proposal: proposal.Proposal{
			Status:         proposal.StatusProposed,
			Confidence:     normalizeConfidence(out.Confidence),
			Rationale:      out.Rationale,
			ProposedSQLURI: sqlURI,
			DiffURI:        diffURI,
			SourceResolved: true,
			Model:          out.Model,
			Repo:           in.Repo,
			CommitSHA:      in.CommitSHA,
			FilePath:       out.TargetFile,
		},
		SuspectedRoot: out.SuspectedRoot,
	}, nil
}

// normalizeConfidence maps the model's free-form confidence to the domain enum,
// defaulting to medium on unrecognised values.
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
