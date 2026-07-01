// Package fixer holds the per-error-class fix strategies for the
// remediation-agent. Each error class (compile, seed_build, validation) is a
// Fixer that decides which source files to read, which prompt to send, and how
// to read the model's answer. The shared driver in service/handlers owns the
// attempt cap, dedup, proposal row, and outbox emit; a Fixer only produces the
// proposal. This package imports no adapter: every collaborator is a port.
package fixer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// Input is the failure evidence a Fixer needs, projected from the inbound
// remediation.requested trigger. DBTLog is already sanitized by the driver.
type Input struct {
	Source          string
	ReleaseID       string
	NodeID          string
	ErrorSignature  string
	Repo            string
	CommitSHA       string
	FilePath        string
	Service         string
	DBTLog          string
	CandidateSQLURI string
	Attempt         int
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
// programming error (the classifier produces only the three known values).
func For(source string) (Fixer, error) {
	switch source {
	case sourceCompile:
		return compileFixer{}, nil
	case sourceSeed:
		return seedFixer{}, nil
	case sourceValidation:
		return validationFixer{}, nil
	default:
		return nil, fmt.Errorf("fixer: unknown error class %q", source)
	}
}

// Error-class discriminators carried on remediation.requested:v1.
const (
	sourceCompile    = "compile"
	sourceSeed       = "seed_build"
	sourceValidation = "validation"
)

// singleShot builds a Fixer whose flow is: gather source files → build one
// prompt → one LLM call → interpret → (on a proposed outcome) diff the chosen
// file against its original content and write the source + diff artifacts.
// compileFixer and seedFixer embed it.
type singleShot struct {
	gather    func(ctx context.Context, svc Services, in Input) (Gathered, bool, error)
	build     func(g Gathered, in Input) prompt.ProposeRequest
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
	res, err := svc.LLM.Propose(ctx, s.build(g, in))
	if err != nil {
		return Result{}, fmt.Errorf("llm propose: %w", err)
	}
	out := s.interpret(res, g, in)
	if out.Status != proposal.StatusProposed {
		return Result{Proposal: proposal.Proposal{Status: out.Status}}, nil
	}
	original := g.Files[out.TargetFile]
	diff := proposal.ComputeUnifiedDiff(original, out.CorrectedContent, in.NodeID)
	sqlURI, err := svc.Artifacts.Write(ctx,
		fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source.sql", in.ReleaseID, in.NodeID, in.Attempt),
		out.CorrectedContent, "text/plain")
	if err != nil {
		return Result{}, fmt.Errorf("write source sql: %w", err)
	}
	diffURI, err := svc.Artifacts.Write(ctx,
		fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source.diff", in.ReleaseID, in.NodeID, in.Attempt),
		diff, "text/plain")
	if err != nil {
		return Result{}, fmt.Errorf("write source diff: %w", err)
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
