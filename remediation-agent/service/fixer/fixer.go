// Package fixer holds the per-error-class fix strategies for the
// remediation-agent. Each error class (compile, seed_build, validation) is a
// Fixer that decides which source files to read, which prompt to send, and how
// to read the model's answer. The shared driver in service/handlers owns the
// attempt cap, dedup, proposal row, and outbox emit; a Fixer only produces the
// proposal. This package imports no adapter, and every collaborator is a port
// with one exception: the python contract fixer writes the model's answer
// straight onto the repository checkout RepoArchive extracted (see applyFiles),
// because the packager it then runs is a subprocess that must see the corrected
// tree on real disk.
package fixer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
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
	Reason string
	// ErrorExcerpt is the classifier's key error line for this failure (capped
	// at 4 KiB).
	ErrorExcerpt string
	Repo         string
	CommitSHA    string
	FilePath     string
	Service      string
	// NodeType is the failing node's kind (dbt-model, dbt-seed, dbt-snapshot,
	// or python-model), set on validation and duplicate-relation failures. It
	// selects which Fixer runs for a validation failure (see For), and every
	// Fixer that can be reached by a python node also checks it before reading
	// anything: the duplicate-table Fixer's target relation is declared in the
	// service's contract.yaml, whose repository path this system does not
	// carry, so the file named by FilePath cannot contain the fix, and the dbt
	// validation Fixer's candidate artifact is a JSON validation spec rather
	// than SQL, so no source fix can be built from what it carries.
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
	// Archive fetches a whole repository checkout at a commit, for a fix that
	// has to search the tree rather than read one known file.
	Archive ports.RepoArchive
	// ContractLocator finds which contract yaml in that checkout declares the
	// python node being fixed.
	ContractLocator ports.ContractLocator
	// Packager turns a directory of python-node contract yaml files into the
	// merged wire contract a release is submitted with.
	Packager ports.ContractPackager
	// Releases submits a fix as a shadow verification release and reads the
	// failing release's image tag.
	Releases ports.ReleaseGateway
	// PriorAttempts reads the attempts already recorded for the failing node,
	// so a later attempt's prompt can show what earlier ones tried.
	PriorAttempts repository.AttemptLister
	// SQLDialect is the sqlglot dialect the warehouse this install runs
	// speaks, resolved from the operator's configured warehouse engine and
	// passed to the packager so a contract is rendered under the same rules
	// the release pipeline validated it against.
	SQLDialect string
}

// Result is what a Fixer returns: the proposal (Status always set; artifact
// URIs, diff, and FilePath populated on a proposed outcome, and on a verifying
// one whose fix a shadow release is still judging) plus the optional
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

// For resolves the Fixer for a trigger's error class and failing node kind. An
// unknown source is a programming error (the classifier produces only the four
// known values).
//
// nodeType selects a lane only for validation failures, where the two node
// kinds need entirely different fixes: a dbt model is corrected in its SQL
// file, while a python node is corrected in the contract yaml that declares it
// and verified by a shadow release. Every other error class ignores nodeType —
// those classes have no python fix to offer, and each already refuses a python
// node in its own way.
func For(source, nodeType string) (Fixer, error) {
	switch source {
	case sourceCompile:
		return compileFixer{}, nil
	case sourceSeed:
		return seedFixer{}, nil
	case sourceValidation:
		if nodeType == string(pkg_model.NodeTypePythonModel) {
			return pythonValidationFixer{}, nil
		}
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
// first, then — when removing this failure's own recorded rejection leaves
// the exact-signature result empty — by the broader (category, reason) class.
// The orchestrator records this same remediation.requested trigger into the
// case base this reads, so on a first occurrence of a signature the exact
// match can consist of nothing but that self row; withoutSelf therefore runs
// on each fetch BEFORE the fallback decision, not after, or a
// first-of-its-kind signature would see len(ps)==1, skip the fallback as
// unnecessary, and only then have the self-filter empty the result — losing
// the fallback precisely when it is the only thing that could help.
// Best-effort: any lookup error degrades to no section, never a blocked heal.
// Each Fixer calls this only after its skip checks pass, so skip paths never
// pay the lookup.
func loadPrecedents(ctx context.Context, svc Services, in Input) []prompt.Precedent {
	fetch := func(q ports.PrecedentQuery) []prompt.Precedent {
		ps, err := svc.Precedents.Precedents(ctx, q)
		if err != nil {
			svc.Logger.Warn("precedent lookup unavailable; proceeding without precedent",
				"node", in.NodeID, "error", err)
			return nil
		}
		return withoutSelf(ps, in, svc.Sanitizer)
	}
	var ps []prompt.Precedent
	if in.ErrorSignature != "" {
		ps = fetch(ports.PrecedentQuery{Signature: in.ErrorSignature, Limit: maxPrecedentsFetch})
	}
	if len(ps) == 0 && in.Category != "" && in.Reason != "" {
		ps = fetch(ports.PrecedentQuery{Category: in.Category, Reason: in.Reason, Limit: maxPrecedentsFetch})
	}
	return ps
}

// withoutSelf drops this failure's own recorded rejection from a precedent
// result set (it IS this failure, not precedent for it) and sanitizes each
// remaining precedent's free-text fields — the one place every Fixer's
// precedents pass through, same as loadDBTLog sanitizes the current failure's
// own log. Both arrive already bounded (the excerpt is capped by the
// classifier, the diff by the orchestrator's renderer), so no truncation is
// needed here. It allocates a fresh slice rather than filtering ps in place,
// since ps is backed by whatever the PrecedentReader returned — a test fake's
// fixture map today, potentially a caching adapter tomorrow — and reusing its
// backing array would corrupt that value across calls.
func withoutSelf(ps []prompt.Precedent, in Input, sanitizer ports.LogSanitizer) []prompt.Precedent {
	out := make([]prompt.Precedent, 0, len(ps))
	for _, p := range ps {
		if p.ReleaseID == in.ReleaseID && p.NodeID == in.NodeID {
			continue
		}
		p.ResolutionDiff = sanitizer.Sanitize(p.ResolutionDiff)
		p.ErrorExcerpt = sanitizer.Sanitize(p.ErrorExcerpt)
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

// writeSourceArtifacts writes the corrected source and its unified diff
// (against the original) as the attempt's source artifacts and returns them as
// a complete FileEdit naming filePath and both URIs. Both singleShot
// (compile/seed) and the validation fixer's real-source step use it, so the
// artifact key layout and content type live in one place.
func writeSourceArtifacts(ctx context.Context, svc Services, in Input, filePath, original, corrected string) (proposal.FileEdit, error) {
	diff := proposal.ComputeUnifiedDiff(original, corrected, filePath)
	sqlURI, err := svc.Artifacts.Write(ctx,
		fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source.sql", in.ReleaseID, in.NodeID, in.Attempt),
		corrected, "text/plain")
	if err != nil {
		return proposal.FileEdit{}, fmt.Errorf("write source sql: %w", err)
	}
	diffURI, err := svc.Artifacts.Write(ctx,
		fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source.diff", in.ReleaseID, in.NodeID, in.Attempt),
		diff, "text/plain")
	if err != nil {
		return proposal.FileEdit{}, fmt.Errorf("write source diff: %w", err)
	}
	return proposal.FileEdit{Path: filePath, ContentURI: sqlURI, DiffURI: diffURI}, nil
}

// writeEditArtifacts writes one file's corrected content and its unified diff
// (against what the repository held) as artifacts of a multi-file attempt, and
// returns them as a complete FileEdit. The attempt's edits share a directory
// and are numbered within it, so two edits of the same attempt can never write
// the same key — a collision would leave both FileEdits pointing at whichever
// file was written last.
func writeEditArtifacts(ctx context.Context, svc Services, in Input, attempt, index int, filePath, original, corrected string) (proposal.FileEdit, error) {
	prefix := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d/edit-%d", in.ReleaseID, in.NodeID, attempt, index)
	contentURI, err := svc.Artifacts.Write(ctx, prefix+".content", corrected, "text/plain")
	if err != nil {
		return proposal.FileEdit{}, fmt.Errorf("write edit content for %s: %w", filePath, err)
	}
	diffURI, err := svc.Artifacts.Write(ctx, prefix+".diff",
		proposal.ComputeUnifiedDiff(original, corrected, filePath), "text/plain")
	if err != nil {
		return proposal.FileEdit{}, fmt.Errorf("write edit diff for %s: %w", filePath, err)
	}
	return proposal.FileEdit{Path: filePath, ContentURI: contentURI, DiffURI: diffURI}, nil
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
	edit, err := writeSourceArtifacts(ctx, svc, in, out.TargetFile, original, out.CorrectedContent)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Proposal: proposal.Proposal{
			Status:         proposal.StatusProposed,
			Confidence:     normalizeConfidence(out.Confidence),
			Rationale:      out.Rationale,
			ProposedSQLURI: edit.ContentURI,
			DiffURI:        edit.DiffURI,
			SourceResolved: true,
			Model:          out.Model,
			Repo:           in.Repo,
			CommitSHA:      in.CommitSHA,
			FilePath:       out.TargetFile,
			Edits:          []proposal.FileEdit{edit},
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
