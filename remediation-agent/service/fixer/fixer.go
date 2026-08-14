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
	// Category is the classifier's failure category; with Reason it forms the
	// fallback precedent-lookup key.
	Category string
	// Reason is the classifier's finer-grained reason within the failure
	// category.
	Reason    string
	Repo      string
	CommitSHA string
	FilePath  string
	Service   string
	// NodeType is the target claimant's kind, set on a duplicate-relation
	// failure. The duplicate-table Fixer skips a python target — its relation
	// is declared in the service's contract.yaml, whose repository path this
	// system does not carry, so the file named by FilePath cannot contain the
	// fix.
	NodeType string
	// OtherService and OtherFilePath locate the competing node that also
	// produces the contested relation (RelationID). Set on a duplicate-relation
	// failure so the prompt can name the relation's other producer without
	// reading its source.
	OtherService         string
	OtherFilePath        string
	DBTLogURI            string
	CandidateArtifactURI string
	// CodeBundleURI locates the release's code-bundle document, which carries
	// every parsed node's raw source. Empty only for a compile-stage
	// rejection, which precedes the parse that produces the bundle; every
	// post-parse rejection (duplicate_table included) carries it.
	CodeBundleURI string
	Attempt       int
}

// Services bundles the ports a Fixer uses to produce a proposal.
type Services struct {
	LLM              ports.LLMProvider
	Source           ports.SourceReader
	Evidence         ports.EvidenceReader
	Sanitizer        ports.LogSanitizer
	Artifacts        ports.ArtifactWriter
	Logger           *slog.Logger
	ServiceRepoPaths map[string]string
	Locator          ports.NodeLocator
	Upstream         ports.UpstreamChangeReader
	Versions         ports.VersionReader
	Precedents       ports.PrecedentReader
	CandidateSource  ports.CandidateSourceReader
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

// maxPrecedentsFetch bounds how many precedents one lookup requests; the
// renderer separately bounds how many carry full diffs.
const maxPrecedentsFetch = 10

// loadPrecedents fetches precedent for this failure: by exact signature
// first, then — when the signature has no recorded matches — by the broader
// (category, reason) class. The failure's own rejection is filtered out (it
// IS this failure, not precedent for it). Best-effort: any error degrades to
// no section, never a blocked heal. Each Fixer calls this only after its
// skip checks pass, so skip paths never pay the lookup.
func loadPrecedents(ctx context.Context, svc Services, in Input) []prompt.Precedent {
	fetch := func(q ports.PrecedentQuery) []prompt.Precedent {
		ps, err := svc.Precedents.Precedents(ctx, q)
		if err != nil {
			svc.Logger.Warn("precedent lookup unavailable; proceeding without precedent",
				"node", in.NodeID, "error", err)
			return nil
		}
		return ps
	}
	var ps []prompt.Precedent
	if in.ErrorSignature != "" {
		ps = fetch(ports.PrecedentQuery{Signature: in.ErrorSignature, Limit: maxPrecedentsFetch})
	}
	if len(ps) == 0 && in.Category != "" && in.Reason != "" {
		ps = fetch(ports.PrecedentQuery{Category: in.Category, Reason: in.Reason, Limit: maxPrecedentsFetch})
	}
	out := ps[:0]
	for _, p := range ps {
		if p.ReleaseID == in.ReleaseID && p.NodeID == in.NodeID {
			continue
		}
		// A precedent's resolution diff and error excerpt are source text from
		// another team's model and dbt run — the same content class as every
		// other diff and log sent to the LLM elsewhere in this package.
		// Sanitized here, the one place every Fixer's precedents pass through,
		// same as loadDBTLog sanitizes the current failure's own log. Both
		// arrive already bounded (the excerpt is capped by the classifier, the
		// diff by the orchestrator's renderer), so no truncation is needed here.
		p.ResolutionDiff = svc.Sanitizer.Sanitize(p.ResolutionDiff)
		p.ErrorExcerpt = svc.Sanitizer.Sanitize(p.ErrorExcerpt)
		out = append(out, p)
	}
	return out
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
	build     func(svc Services, g Gathered, in Input, dbtLog string, precedents []prompt.Precedent) prompt.ProposeRequest
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
	precedents := loadPrecedents(ctx, svc, in)
	res, err := svc.LLM.Propose(ctx, s.build(svc, g, in, dbtLog, precedents))
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
