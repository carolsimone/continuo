package fixer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/domain/repository"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// pythonValidationFixer handles a python node's validation rejection. A python
// node is not a SQL file: it is an entry in a contract yaml declaring the
// relations it reads, the columns it produces, and the script that produces
// them, and validation rejects it when what the script produced does not match
// what the contract promised. So the fix is made in that yaml, and it cannot be
// judged by reading it back — only by running it.
//
// The fixer therefore ends in a shadow release rather than a proposal: it
// checks out the team's repository at the failing commit, finds the file
// declaring the node, asks the model to correct it, packages the result with
// the same tool the team's own CI runs after a merge, and submits it as a real
// release that runs the full parse -> candidate-schema -> validation pipeline
// but stops at "validated" instead of promoting. The proposal it returns is
// 'verifying'; a reconciler resolves it to 'proposed' or 'failed' once that
// release reaches a verdict. Nothing here re-implements any validation rule —
// dialect, bind, and hash semantics are identical to a normal release by
// construction, because it IS one.
type pythonValidationFixer struct{}

func (pythonValidationFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	schema, table, ok := splitNodeID(in.NodeID)
	if !ok {
		return skipPython(svc, in, fmt.Sprintf(
			"node id %q does not name a schema and a table, so the contract file declaring it cannot be found", in.NodeID))
	}
	// The service names both the object-storage location the merged contract
	// is uploaded to and the release the fix is submitted as, so a trigger
	// without one has nowhere to put the fix and nothing to submit it under.
	if in.Service == "" {
		return skipPython(svc, in, "the trigger names no service, so the fix has no release to be verified under")
	}

	// Step 1 — the repository at the failing commit. A python node's declaring
	// yaml path is recorded nowhere in the control plane, so the whole tree is
	// searched rather than one known file read.
	root, cleanup, err := svc.Archive.Fetch(ctx, in.Repo, in.CommitSHA)
	// A repository or commit that does not exist is permanent: redelivering the
	// trigger would retry it forever, so it ends the attempt with the reason
	// recorded. Any other fetch failure is transient and is returned.
	if errors.Is(err, ports.ErrSourceNotFound) {
		return skipPython(svc, in, fmt.Sprintf("repository %s at commit %s is not available", in.Repo, in.CommitSHA))
	}
	if err != nil {
		return Result{}, fmt.Errorf("fetch repo %s@%s: %w", in.Repo, in.CommitSHA, err)
	}
	defer cleanup()

	// Step 2 — the declaring contract file. Neither "no file declares it" nor
	// "several files declare it" is a transient failure, and neither may be
	// guessed past: both end the attempt with the reason recorded, so an
	// operator sees why the heal stopped instead of finding nothing at all.
	located, err := svc.ContractLocator.Locate(root, schema, table)
	if errors.Is(err, ports.ErrNodeNotDeclared) || errors.Is(err, ports.ErrAmbiguousDeclaration) {
		return skipPython(svc, in, err.Error())
	}
	if err != nil {
		return Result{}, fmt.Errorf("locate contract for %s: %w", in.NodeID, err)
	}

	// Step 3 — evidence. The failure itself is required; every other section
	// degrades to its own absence rather than blocking the heal.
	ev, err := pythonEvidence(ctx, svc, in, located)
	if err != nil {
		return Result{}, err // transient read: the driver redelivers
	}

	// Step 4/5 — one model call returning complete files.
	res, err := svc.LLM.Propose(ctx, prompt.AssemblePythonContractFix(ev))
	if err != nil {
		return Result{}, fmt.Errorf("llm propose: %w", err)
	}
	if len(res.Files) == 0 {
		return failPython(svc, in, "the model returned no files to change")
	}
	// Every path must be inside the contract directory the model was shown, and
	// no file may appear twice. A repeated path has no single answer: applying
	// the list leaves the LAST entry on disk — which is what gets packaged and
	// what the shadow release verifies — while the recorded edits, and the
	// proposal's single-file view built from the first of them, would describe
	// an earlier entry. A human would then approve content that was never the
	// content validated. Paths are compared in cleaned form so two spellings of
	// one file are recognised as the same file.
	seen := make(map[string]struct{}, len(res.Files))
	for _, f := range res.Files {
		if !withinDir(located.ContractDir, f.Path) {
			return failPython(svc, in, fmt.Sprintf(
				"the model proposed a change to %q, which is outside the contract directory %q that declares this node",
				f.Path, located.ContractDir))
		}
		key := path.Clean(filepath.ToSlash(f.Path))
		if _, dup := seen[key]; dup {
			return failPython(svc, in, fmt.Sprintf(
				"the model returned %q more than once, so the content to record could not be told apart from the content to apply",
				f.Path))
		}
		seen[key] = struct{}{}
	}

	// Step 6 — apply, then package. The order is the whole point: packaging the
	// checkout before the answer is written to it would produce, and verify,
	// the contract that already failed.
	originals, err := applyFiles(root, res.Files)
	if err != nil {
		return Result{}, err
	}
	merged, err := svc.Packager.Merge(ctx,
		filepath.Join(root, filepath.FromSlash(located.ContractDir)),
		filepath.Join(root, filepath.FromSlash(located.RepoRoot)),
		in.Service, svc.SQLDialect)
	if err != nil {
		return Result{}, fmt.Errorf("package contract for %s: %w", in.NodeID, err)
	}

	// Step 7 — the merged contract goes to the per-release artifact location
	// release-controller reads a python service's release payload from, under
	// the shadow release's own id.
	shadowID := shadowReleaseID(in)
	if _, err := svc.Artifacts.Write(ctx,
		fmt.Sprintf("%s/%s/contract.yaml", in.Service, shadowID), string(merged), "application/yaml"); err != nil {
		return Result{}, fmt.Errorf("write shadow contract: %w", err)
	}

	// Step 8 — submit. The image tag is read from the ORIGINAL failing release
	// (the trigger carries none, and the shadow runs the same image); the
	// submission itself runs under the shadow's own id. This happens after the
	// upload because release-controller reads that object as soon as the
	// submission is accepted.
	imageTag, err := svc.Releases.ImageTag(ctx, in.ReleaseID, in.Service)
	if err != nil {
		return Result{}, fmt.Errorf("read image tag for %s: %w", in.Service, err)
	}
	if err := svc.Releases.Submit(ctx, ports.ShadowSubmission{
		ReleaseID: shadowID,
		Service:   in.Service,
		ImageTag:  imageTag,
		Repo:      in.Repo,
		CommitSHA: in.CommitSHA,
	}); err != nil {
		return Result{}, fmt.Errorf("submit shadow release %s: %w", shadowID, err)
	}

	// Step 9 — one audit artifact pair per edited file, diffed against what the
	// repository held before the answer was applied.
	edits := make([]proposal.FileEdit, 0, len(res.Files))
	for i, f := range res.Files {
		edit, werr := writeEditArtifacts(ctx, svc, in, in.Attempt, i, f.Path, originals[f.Path], f.Content)
		if werr != nil {
			return Result{}, werr
		}
		edits = append(edits, edit)
	}

	svc.Logger.Info("python contract fix submitted for shadow verification",
		"node", in.NodeID, "release", in.ReleaseID, "shadow_release", shadowID, "files", len(edits))

	return Result{Proposal: proposal.Proposal{
		Status:          proposal.StatusVerifying,
		ShadowReleaseID: shadowID,
		Confidence:      normalizeConfidence(res.Confidence),
		Rationale:       res.Rationale,
		Model:           res.Model,
		Repo:            in.Repo,
		CommitSHA:       in.CommitSHA,
		FilePath:        edits[0].Path,
		ProposedSQLURI:  edits[0].ContentURI,
		DiffURI:         edits[0].DiffURI,
		// The edits name real files at a real commit, which is what a pull
		// request needs; the shadow release decides whether they are right.
		SourceResolved: true,
		Edits:          edits,
	}}, nil
}

// skipPython records a skipped attempt whose reason is kept as the proposal's
// rationale, so the operator reading the release sees why no fix was attempted
// rather than an unexplained absence.
func skipPython(svc Services, in Input, reason string) (Result, error) {
	svc.Logger.Info("python contract fix skipped", "node", in.NodeID, "reason", reason)
	return Result{Proposal: proposal.Proposal{Status: proposal.StatusSkipped, Rationale: reason}}, nil
}

// failPython records a failed attempt — one where a fix was attempted and the
// model's answer could not be used — keeping the reason as the rationale for
// the same purpose skipPython does: a recorded outcome an operator can read is
// never left as a log line beside an unexplained row.
func failPython(svc Services, in Input, reason string) (Result, error) {
	svc.Logger.Warn("python contract fix failed", "node", in.NodeID, "reason", reason)
	return Result{Proposal: proposal.Proposal{Status: proposal.StatusFailed, Rationale: reason}}, nil
}

// splitNodeID reads the schema and table out of a node id. A python node's
// identity is "<schema>.<table>", and the trailing two dot-separated segments
// are what the runtime matches a node by, so they are what the contract search
// is given. ok is false for an id with no dot, from which no node can be
// addressed.
func splitNodeID(nodeID string) (schema, table string, ok bool) {
	parts := strings.Split(nodeID, ".")
	if len(parts) < 2 {
		return "", "", false
	}
	schema, table = parts[len(parts)-2], parts[len(parts)-1]
	if schema == "" || table == "" {
		return "", "", false
	}
	return schema, table, true
}

// maxShadowReleaseIDLen bounds the minted id so every name derived from it
// downstream stays legal. The tightest consumer is the candidate schema
// release-controller creates for the release, built as "_candidate_" followed
// by the release id with every character outside [A-Za-z0-9_] replaced
// one-for-one — so the schema is exactly 11 bytes longer than the id.
// PostgreSQL truncates an identifier past 63 bytes rather than rejecting it,
// and what it cuts is the tail: the attempt suffix, then the node name. Two
// attempts at one node would then share a schema and each would validate
// against the other's leftovers, so the id is bounded here instead.
const maxShadowReleaseIDLen = 63 - len("_candidate_")

// shadowIDPrefix marks a release as a fix verification at a glance, in release
// listings and in every log line that names one.
const shadowIDPrefix = "shadow-"

// shadowReleaseID mints the id of the release that verifies this attempt. It
// embeds the failing release, node, and attempt so it is unique per attempt and
// legible in release listings, and so a redelivery of the same attempt reuses
// the same id — release-controller's submission is idempotent on it.
//
// Past maxShadowReleaseIDLen the middle — the failing release and node — is
// shortened and given a digest of what it held. The prefix and the attempt
// number are kept whole because they are what a reader identifies the release
// by, and the digest is what keeps two nodes whose names diverge only past the
// cut on separate candidate schemas.
func shadowReleaseID(in Input) string {
	middle := sanitizeIDSegment(in.ReleaseID) + "-" + sanitizeIDSegment(in.NodeID)
	suffix := fmt.Sprintf("-a%d", in.Attempt)
	if len(shadowIDPrefix)+len(middle)+len(suffix) <= maxShadowReleaseIDLen {
		return shadowIDPrefix + middle + suffix
	}
	digest := sha256.Sum256([]byte(middle))
	tail := hex.EncodeToString(digest[:4]) // 8 hex chars
	// Reserve the digest and its separator before cutting, so shortening eats
	// the readable head rather than the part that restores uniqueness.
	budget := maxShadowReleaseIDLen - len(shadowIDPrefix) - len(suffix) - len(tail) - 1
	if budget < 0 {
		budget = 0
	}
	return shadowIDPrefix + strings.TrimRight(middle[:budget], "-") + "-" + tail + suffix
}

// sanitizeIDSegment reduces a value to the characters a release id and the S3
// key derived from it can safely carry, replacing every other character with a
// dash. Dots, dashes, and underscores survive, so a node id reads unchanged.
func sanitizeIDSegment(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '.', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, s)
}

// withinDir reports whether a model-proposed repository path resolves inside
// dir. The answer is written straight onto a checkout, so a path that escapes
// the contract directory — by naming another service, by traversing upwards, or
// by being absolute — would edit files this fix has no business touching.
func withinDir(dir, p string) bool {
	if p == "" || filepath.IsAbs(p) || path.IsAbs(p) {
		return false
	}
	clean := path.Clean(filepath.ToSlash(p))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return false
	}
	dir = path.Clean(filepath.ToSlash(dir))
	if dir == "." {
		return true // the contract directory is the repository root
	}
	return clean == dir || strings.HasPrefix(clean, dir+"/")
}

// applyFiles writes each proposed file into the checkout at root and returns
// what each path held beforehand, keyed by the path as the model named it. The
// originals are read before anything is written so the recorded diffs describe
// the change against the repository's own content; a path the repository does
// not have yet reads as empty, which diffs as a new file.
func applyFiles(root string, files []ports.ProposedFile) (map[string]string, error) {
	originals := make(map[string]string, len(files))
	for _, f := range files {
		full := filepath.Join(root, filepath.FromSlash(f.Path))
		existing, err := os.ReadFile(full) //nolint:gosec // G304: full is a checkout-relative path already confirmed to resolve inside the contract directory.
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("read %s before applying the fix: %w", f.Path, err)
		}
		originals[f.Path] = string(existing)
	}
	for _, f := range files {
		full := filepath.Join(root, filepath.FromSlash(f.Path))
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			return nil, fmt.Errorf("create directory for %s: %w", f.Path, err)
		}
		if err := os.WriteFile(full, []byte(f.Content), 0o600); err != nil {
			return nil, fmt.Errorf("apply fix to %s: %w", f.Path, err)
		}
	}
	return originals, nil
}

// pythonEvidence assembles everything the model is shown. The failure text and
// the declaring file are required; the runner log, the bundle's contract entry,
// upstream changes, precedent, and prior attempts each degrade to their own
// absence, because a missing evidence section costs the model context while a
// blocked heal costs the operator the fix entirely. Only a transient
// object-storage failure is returned as an error, so the trigger is redelivered
// rather than answered from evidence that could not be read.
func pythonEvidence(ctx context.Context, svc Services, in Input, located ports.Located) (prompt.PythonEvidence, error) {
	runnerLog, err := loadDBTLog(ctx, svc, in.DBTLogURI)
	if err != nil {
		return prompt.PythonEvidence{}, err
	}

	contractEntry, err := pythonContractEntry(ctx, svc, in)
	if err != nil {
		return prompt.PythonEvidence{}, err
	}

	// Upstream ancestor diffs arrive capped by the orchestrator but not
	// sanitized; each is run through the LogSanitizer here, like every other
	// source string that reaches the LLM.
	upstream, err := svc.Upstream.UpstreamChanges(ctx, in.NodeID)
	if err != nil {
		svc.Logger.Warn("upstream changes unavailable; proceeding without upstream context",
			"node", in.NodeID, "error", err)
		upstream = nil
	} else {
		for i := range upstream {
			upstream[i].CodeDiff = svc.Sanitizer.Sanitize(upstream[i].CodeDiff)
			upstream[i].ConfigDiff = svc.Sanitizer.Sanitize(upstream[i].ConfigDiff)
		}
	}

	return prompt.PythonEvidence{
		NodeID:          in.NodeID,
		ErrorExcerpt:    svc.Sanitizer.Sanitize(in.ErrorExcerpt),
		RunnerLog:       runnerLog,
		ContractEntry:   contractEntry,
		YAMLPath:        located.YAMLPath,
		YAMLText:        svc.Sanitizer.Sanitize(located.YAMLText),
		UpstreamChanges: upstream,
		Precedents:      loadPrecedents(ctx, svc, in),
		PriorAttempts:   loadPriorAttempts(ctx, svc, in),
	}, nil
}

// pythonContractEntry returns the failing node's entry from the release's code
// bundle: its normalized contract as canonical JSON, which is what the release
// actually parsed and validated. This is the one place the system's usual rule
// inverts — every dbt path refuses a bundle entry whose runtime is not dbt,
// because there the entry is not model source; here a python runtime is exactly
// what is expected, and any other runtime means the trigger and the bundle
// disagree about what this node is, so the entry is left out rather than shown
// as something it is not. A permanent miss also degrades to no section; only a
// transient fetch error is returned.
func pythonContractEntry(ctx context.Context, svc Services, in Input) (string, error) {
	src, err := svc.CandidateSource.NodeSource(ctx, in.CodeBundleURI, in.NodeID, in.ReleaseID)
	switch {
	case err == nil && src.Runtime == ports.RuntimePython:
		return svc.Sanitizer.Sanitize(src.RawCode), nil
	case err == nil:
		svc.Logger.Warn("code bundle records a non-python runtime for a python node; omitting the contract entry",
			"node", in.NodeID, "runtime", src.Runtime)
		return "", nil
	case errors.Is(err, ports.ErrNotFound):
		svc.Logger.Warn("contract entry not in the code bundle; proceeding without it",
			"node", in.NodeID, "bundle_uri", in.CodeBundleURI)
		return "", nil
	default:
		return "", fmt.Errorf("fetch code bundle: %w", err)
	}
}

// loadPriorAttempts returns the earlier attempts at this same failure, oldest
// first, each with the error its shadow release reported and the diffs it
// applied. This is what makes attempt N+1 better informed than attempt N; it is
// still best-effort, so an unreadable attempt history costs the section rather
// than the heal. The in-flight attempt's own row is excluded — it is this
// attempt, not precedent for it. No row limit is applied: the driver's
// per-failure attempt cap already bounds how many rows can exist.
func loadPriorAttempts(ctx context.Context, svc Services, in Input) []prompt.PriorAttempt {
	if svc.PriorAttempts == nil {
		return nil
	}
	views, err := svc.PriorAttempts.List(ctx, repository.ProposalFilter{
		ReleaseID: in.ReleaseID,
		Source:    in.Source,
		NodeID:    in.NodeID,
	})
	if err != nil {
		svc.Logger.Warn("prior attempts unavailable; proceeding without them", "node", in.NodeID, "error", err)
		return nil
	}
	out := make([]prompt.PriorAttempt, 0, len(views))
	for _, v := range views {
		if v.Attempt >= in.Attempt {
			continue
		}
		out = append(out, prompt.PriorAttempt{
			Attempt:     v.Attempt,
			VerifyError: svc.Sanitizer.Sanitize(v.VerifyError),
			Diffs:       attemptDiffs(ctx, svc, v),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Attempt < out[j].Attempt })
	return out
}

// attemptDiffs resolves one recorded attempt's edits to their stored unified
// diffs. A diff that cannot be read is omitted: the attempt still shows as
// tried, with its error, which is the more important half.
func attemptDiffs(ctx context.Context, svc Services, v proposal.View) []prompt.AttemptDiff {
	out := make([]prompt.AttemptDiff, 0, len(v.Edits))
	for _, e := range v.Edits {
		if e.DiffURI == "" {
			continue
		}
		raw, err := svc.Evidence.Fetch(ctx, e.DiffURI)
		if err != nil {
			svc.Logger.Warn("prior attempt diff unavailable; omitting it",
				"attempt", v.Attempt, "diff_uri", e.DiffURI, "error", err)
			continue
		}
		out = append(out, prompt.AttemptDiff{Path: e.Path, Diff: svc.Sanitizer.Sanitize(raw)})
	}
	return out
}
