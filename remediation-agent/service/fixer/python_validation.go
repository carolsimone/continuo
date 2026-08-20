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

	// Step 3 — is a fix for this node verifiable at all? The fix is packaged
	// from the whole contract directory, so the shadow release re-runs every
	// node declared in it. Another node in that directory that also failed the
	// original release therefore rejects the shadow however correct the fix is,
	// and the attempt ends up front rather than spending a full validation
	// release — and the single release slot — proving it.
	sibling, err := siblingFailure(ctx, svc, in, located, root)
	if err != nil {
		return Result{}, err
	}
	if sibling != "" {
		return skipPython(svc, in, fmt.Sprintf(
			"%s also failed validation in release %s and is declared in the same contract directory (%s) as %s. "+
				"A fix is packaged from that whole directory, so any shadow release verifying it would re-run both nodes "+
				"and be rejected by %s. Fix both nodes together.",
			sibling, in.ReleaseID, located.ContractDir, in.NodeID, sibling))
	}

	// Step 4 — evidence. The failure itself is required; every other section
	// degrades to its own absence rather than blocking the heal.
	ev, err := pythonEvidence(ctx, svc, in, located)
	if err != nil {
		return Result{}, err // transient read: the driver redelivers
	}

	// Step 5 — one model call returning complete files.
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

	// Step 7 — the node under repair must have survived its own repair. A
	// shadow release can only reject what the packaged contract still declares,
	// so an answer that deleted or renamed the failing node — or deleted the
	// read that could not bind — leaves the release nothing to fail on: it
	// validates, and the edit that removed the broken thing is recorded as a
	// proven fix and offered to a human. The same holds for a sibling declared
	// beside it, which this attempt was never asked to touch at all.
	if reason := declarationBreach(svc, res.Files, originals); reason != "" {
		return failPython(svc, in, reason)
	}
	// The before/after comparison above sees only the files the answer returned.
	// Re-running the search over the patched checkout covers what it cannot: the
	// node must still be declared, exactly once, and still inside the directory
	// this fix packages.
	relocated, err := svc.ContractLocator.Locate(root, schema, table)
	switch {
	case errors.Is(err, ports.ErrNodeNotDeclared), errors.Is(err, ports.ErrAmbiguousDeclaration):
		return failPython(svc, in, fmt.Sprintf(
			"after applying the model's answer, %s is no longer declared by exactly one contract file (%s); "+
				"a fix may correct what a node declares but never change which node is declared", in.NodeID, err))
	case err != nil:
		return Result{}, fmt.Errorf("re-locate contract for %s after applying the fix: %w", in.NodeID, err)
	case relocated.ContractDir != located.ContractDir:
		return failPython(svc, in, fmt.Sprintf(
			"the model's answer moved %s out of the contract directory %q into %q, so the fix would not be packaged with the node it repairs",
			in.NodeID, located.ContractDir, relocated.ContractDir))
	}

	merged, err := svc.Packager.Merge(ctx,
		filepath.Join(root, filepath.FromSlash(located.ContractDir)),
		filepath.Join(root, filepath.FromSlash(located.RepoRoot)),
		in.Service, svc.SQLDialect)
	// A contract the packaging tool read and refused is settled: the tool is
	// deterministic on its input, and a redelivery rebuilds that same input from
	// the cached model answer, so retrying would refuse it again until the
	// stream dropped the message and left the attempt in flight for good. The
	// attempt ends here instead, keeping the tool's complaint as the reason. Any
	// other packaging failure is the tool never having answered, and is returned
	// so the trigger is redelivered.
	if errors.Is(err, ports.ErrContractRejected) {
		return failPython(svc, in, fmt.Sprintf(
			"the tool that packages a contract for release refused the model's answer, so no release could be built from it: %v", err))
	}
	if err != nil {
		return Result{}, fmt.Errorf("package contract for %s: %w", in.NodeID, err)
	}

	// Step 8 — the merged contract goes to the per-release artifact location
	// release-controller reads a python service's release payload from, under
	// the shadow release's own id.
	shadowID := shadowReleaseID(in)
	if _, err := svc.Artifacts.Write(ctx,
		fmt.Sprintf("%s/%s/contract.yaml", in.Service, shadowID), string(merged), "application/yaml"); err != nil {
		return Result{}, fmt.Errorf("write shadow contract: %w", err)
	}

	// Step 9 — submit. The image tag is read from the ORIGINAL failing release
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

	// Step 10 — one audit artifact pair per edited file, diffed against what the
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

// declarationBreach compares the node declarations the answer's own files held
// before it was applied with the ones they hold after, and returns the reason
// to refuse the answer — or "" when every declaration survived intact.
//
// Only the returned files are examined, and that is complete: a declaration can
// only change if the file carrying it changed, and every changed file is in
// this list. Both sides are read as a whole rather than file by file, so moving
// an entry between two of the answer's own files — which packages identically —
// is not mistaken for deleting it from one.
//
// A node the answer ADDS is not refused here, and neither is a read it adds or
// rewrites. Each is declared in the packaged directory, so the shadow release
// runs and bind-checks it like any other, which is an honest verdict rather than
// a false one. What is refused is subtraction: a node or a read that was there
// and no longer is leaves the release less to judge than the failure it was
// asked to repair.
func declarationBreach(svc Services, files []ports.ProposedFile, originals map[string]string) string {
	before := map[string]ports.NodeDeclaration{}
	after := map[string]ports.NodeDeclaration{}

	for _, f := range files {
		if !isContractYAMLPath(f.Path) {
			continue
		}
		// A prior content that does not parse declared nothing this answer can
		// be held to: the contract search skips such a file too, so the node it
		// might have named was never the node under repair.
		if decls, err := svc.ContractInspector.Declarations(originals[f.Path]); err == nil {
			for _, d := range decls {
				before[identityKey(d.Identity)] = d
			}
		}
		decls, err := svc.ContractInspector.Declarations(f.Content)
		if err != nil {
			return fmt.Sprintf("the model returned %q, which is not a readable contract file: %v", f.Path, err)
		}
		for _, d := range decls {
			after[identityKey(d.Identity)] = d
		}
	}

	// Sorted so an answer that moves several nodes always names the same one,
	// and a re-run of the same attempt records the same rationale.
	keys := make([]string, 0, len(before))
	for k := range before {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		was := before[k]
		now, still := after[k]
		if !still {
			return fmt.Sprintf(
				"the model's answer no longer declares %s. A fix corrects what a node declares; removing or renaming "+
					"the node itself would leave the release nothing to validate, so the change could never be proven correct", k)
		}
		if now.Identity != was.Identity {
			return fmt.Sprintf(
				"the model's answer changed the fields that identify %s (%s). Those fields say which node the entry is, "+
					"so changing one makes it a different node rather than a repaired one", k, identityDelta(was.Identity, now.Identity))
		}
		if dropped := droppedReads(was.ReadKeys, now.ReadKeys); len(dropped) > 0 {
			return fmt.Sprintf(
				"the model's answer no longer declares the read %s of %s. Validation bind-checks the reads a contract still "+
					"declares, so deleting one hides the failure instead of repairing it: the node's script — which this fix "+
					"does not change — still performs that read, and no release would catch it again. A read may be corrected "+
					"or replaced, never dropped", strings.Join(quoteAll(dropped), ", "), k)
		}
	}
	return ""
}

// droppedReads returns the read names present before an edit and absent after
// it, sorted, so an answer that deletes several always names them in the same
// order. Names are compared verbatim: a read's name is how the node's script
// asks for it, so a re-spelling is a rename, and a rename drops the name the
// script uses.
func droppedReads(was, now []string) []string {
	kept := make(map[string]struct{}, len(now))
	for _, k := range now {
		kept[k] = struct{}{}
	}
	var gone []string
	for _, k := range was {
		if _, ok := kept[k]; !ok {
			gone = append(gone, k)
		}
	}
	sort.Strings(gone)
	return gone
}

// quoteAll wraps each value in quotes so a rationale naming several reads reads
// as a list of names rather than as running text.
func quoteAll(vs []string) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = fmt.Sprintf("%q", v)
	}
	return out
}

// identityKey is the node id a contract entry declares — "<schema>.<table>",
// lowercased, the same form every trigger and release verdict names a node by.
// Folding case is what makes a re-spelling of a name show up as a mutated
// entry rather than as one node deleted and an unrelated one added.
func identityKey(id ports.NodeIdentity) string {
	return strings.ToLower(id.Schema + "." + id.Table)
}

// identityDelta renders the identity fields that differ between two versions of
// one entry, so a refused answer says which field moved instead of only that
// something did.
func identityDelta(was, now ports.NodeIdentity) string {
	fields := []struct {
		name     string
		was, now string
	}{
		{"schema", was.Schema, now.Schema},
		{"table", was.Table, now.Table},
		{"script", was.Script, now.Script},
		{"owner", was.Owner, now.Owner},
		{"schedule", was.Schedule, now.Schedule},
		{"criticality", was.Criticality, now.Criticality},
	}
	changed := make([]string, 0, len(fields))
	for _, f := range fields {
		if f.was != f.now {
			changed = append(changed, fmt.Sprintf("%s %q -> %q", f.name, f.was, f.now))
		}
	}
	return strings.Join(changed, ", ")
}

// isContractYAMLPath reports whether a repository path is one the contract
// search would parse. Only those files can declare a node, so only those are
// compared across the edit; a script or a data file the answer also returns
// carries no declaration to preserve.
func isContractYAMLPath(p string) bool {
	switch strings.ToLower(path.Ext(filepath.ToSlash(p))) {
	case ".yml", ".yaml":
		return true
	default:
		return false
	}
}

// siblingFailure names another node that failed validation in the same release
// AND is declared in the same contract directory as the node being fixed, or ""
// when there is none.
//
// It exists because two things disagree about scope. The classifier emits one
// heal trigger per failing node, but a python fix is packaged from the whole
// directory the declaring yaml lives in — so the release that verifies it runs
// every node declared there. If a second one of those nodes is already broken,
// no fix for the first can ever be validated: the shadow is rejected by the
// node this attempt neither repaired nor was shown, that rejection becomes the
// next attempt's evidence, and the same thing happens again until the attempt
// cap is reached.
//
// The failing set is read from the ORIGINAL release's verdict, whose NodeErrors
// are keyed by node id, and each other id is resolved against the same checkout
// to see where it is declared. A node no contract yaml declares — a dbt model,
// or a python node in another service — is not packaged by this fix and so
// cannot reject its shadow release.
//
// A verdict that cannot be read ends the attempt with an error rather than a
// guess, so the driver redelivers: this decides whether the shadow release can
// mean anything at all, and it is the same release the image-tag read already
// treats as required.
func siblingFailure(ctx context.Context, svc Services, in Input, located ports.Located, root string) (string, error) {
	verdict, err := svc.Releases.Verdict(ctx, in.ReleaseID)
	if err != nil {
		return "", fmt.Errorf("read verdict of release %s: %w", in.ReleaseID, err)
	}
	others := make([]string, 0, len(verdict.NodeErrors))
	for nodeID := range verdict.NodeErrors {
		if nodeID != in.NodeID {
			others = append(others, nodeID)
		}
	}
	// Sorted so a release with several siblings always names the same one, and
	// a re-run of the same attempt records the same rationale.
	sort.Strings(others)

	for _, nodeID := range others {
		schema, table, ok := splitNodeID(nodeID)
		if !ok {
			continue
		}
		other, lerr := svc.ContractLocator.Locate(root, schema, table)
		if lerr != nil {
			// Not declared anywhere, declared twice, or unsearchable: in none of
			// those cases is this node known to share the directory being
			// packaged, and uncertainty about someone else's node is not a
			// reason to abandon a fix for this one.
			if !errors.Is(lerr, ports.ErrNodeNotDeclared) {
				svc.Logger.Warn("could not place another failing node; proceeding as if it is declared elsewhere",
					"node", nodeID, "release", in.ReleaseID, "error", lerr)
			}
			continue
		}
		if other.ContractDir == located.ContractDir {
			return nodeID, nil
		}
	}
	return "", nil
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
