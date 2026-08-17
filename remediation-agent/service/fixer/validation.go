package fixer

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"unicode/utf8"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// maxUpstreamDiffBytes caps the own-change diff (what this release changed in
// the failing model relative to the code that runs now) so one large refactor
// cannot dominate the prompt. Upstream ancestor diffs arrive already capped by
// the orchestrator's UpstreamChangeReader.
const maxUpstreamDiffBytes = 8192

// truncateDiff caps a diff at max bytes, appending a marker when it was cut. The
// cut is backed up to a UTF-8 rune boundary so truncation never splits a
// multibyte character (which would otherwise render as U+FFFD in the prompt).
func truncateDiff(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "\n… (diff truncated)"
}

// validationFixer handles validation-rejection failures, which carry a
// candidate SQL (the pre-compiled SQL extracted from object storage at the
// point of failure). It runs a two-step flow:
//
//  1. Ask the LLM to fix the candidate SQL given the dbt error, the diff of
//     what this release itself changed in the failing model, the diffs of its
//     recently-changed upstreams, and precedent from similar past failures.
//     The result is written as candidate artifacts unconditionally, for audit
//     — it is the LLM's fix applied to the pre-compiled SQL, not the real
//     model source.
//  2. Best-effort: resolve the real model source (the release's code bundle,
//     falling back to a GitHub read) and ask the LLM to apply the Step-1
//     diagnosis to it. On success the final proposal points to the source
//     artifacts; on any degraded path it falls back to the candidate.
type validationFixer struct{}

func (validationFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	// A python node is skipped before anything is read. Its candidate artifact
	// is a JSON validation spec (declared reads plus output columns), not SQL,
	// and the code bundle records its source as the normalized contract entry
	// rather than the script the repository holds — so neither the diagnosis
	// prompt nor a source fix can be built from what this path has. Deciding it
	// from the trigger's node_type keeps the LLM out of the python path
	// entirely, rather than discovering the node's kind after two calls.
	if in.NodeType == string(pkg_model.NodeTypePythonModel) {
		svc.Logger.Info("validation fix: failing node is a python node; skipping — "+
			"its candidate artifact is a validation spec and its bundle entry is a contract "+
			"entry, neither of which is model source a fix can be proposed against",
			"node", in.NodeID)
		return Result{Proposal: proposal.Proposal{Status: proposal.StatusSkipped}}, nil
	}

	// Validation with no candidate SQL: nothing to fix. This is decided before
	// any log fetch so a transiently unreadable log cannot turn the intended
	// skip into a redelivery.
	if in.CandidateArtifactURI == "" {
		return Result{Proposal: proposal.Proposal{Status: proposal.StatusSkipped}}, nil
	}

	candidateSQL, err := svc.Evidence.Fetch(ctx, in.CandidateArtifactURI)
	if err != nil {
		return Result{}, fmt.Errorf("fetch candidate sql: %w", err)
	}

	dbtLog, err := loadDBTLog(ctx, svc, in.DBTLogURI)
	if err != nil {
		return Result{}, err // transient log read error: driver redelivers
	}

	// PR targeting: prefer the source location the trigger carries, which
	// release-controller stamps from the candidate topology. Falling back to
	// the promoted topology is correct only for a trigger that carries none,
	// because the rejected release was never promoted: that lookup holds
	// nothing for a newly-added node and the previous release's path for a node
	// whose candidate moved it. Best-effort — a failed lookup degrades Step 2
	// to the candidate-only proposal.
	filePath, serviceName := in.FilePath, in.Service
	if filePath == "" {
		filePath, serviceName, err = svc.Locator.Locate(ctx, in.NodeID)
		if err != nil {
			svc.Logger.Warn("node location unavailable; source fix will degrade to candidate",
				"node", in.NodeID, "error", err)
			filePath, serviceName = "", ""
		}
	}

	// Candidate source from the release's code bundle; the repo read is only
	// the fallback. A permanent bundle miss is a degraded path, a transient
	// fetch error redelivers the trigger, and either a bundle entry for a
	// non-dbt node, or — only when the trigger carries no node_type — a
	// non-".sql" fallback path, skips the whole fix.
	candidateSource, sourceOrigin, err := resolveCandidateSource(ctx, svc, in, filePath, serviceName)
	if errors.Is(err, errNonDbtCandidate) {
		svc.Logger.Info("validation fix: failing node's candidate source is not a dbt model; skipping",
			"node", in.NodeID, "error", err)
		return Result{Proposal: proposal.Proposal{Status: proposal.StatusSkipped}}, nil
	}
	if err != nil {
		return Result{}, err
	}
	if sourceOrigin != "" {
		svc.Logger.Info("validation fix: candidate source resolved", "node", in.NodeID, "source_origin", sourceOrigin)
	}

	// Own-change diff: what this release changed relative to the code that
	// runs now. Absent history (a new node) simply omits the section. Sanitized
	// before truncation, like every other source string sent to the LLM, so a
	// secret in a changed line is redacted rather than sent to the external LLM.
	ownChangeDiff := ""
	if candidateSource != "" {
		if cur, ok, verr := svc.Versions.CurrentVersion(ctx, in.NodeID); verr != nil {
			svc.Logger.Warn("current version unavailable; omitting own-change diff", "node", in.NodeID, "error", verr)
		} else if ok {
			diff := proposal.ComputeUnifiedDiff(cur.RawCode, candidateSource, in.NodeID)
			ownChangeDiff = truncateDiff(svc.Sanitizer.Sanitize(diff), maxUpstreamDiffBytes)
		}
	}

	// Upstream ancestor diffs arrive already capped by the orchestrator, but not
	// sanitized — each is run through the LogSanitizer here before it reaches
	// the prompt, same as every other source string sent to the LLM.
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

	precedents := loadPrecedents(ctx, svc, in)

	res, err := svc.LLM.Propose(ctx, prompt.Assemble(prompt.Evidence{
		NodeID:          in.NodeID,
		ErrorSignature:  in.ErrorSignature,
		CandidateSQL:    candidateSQL,
		DBTLog:          dbtLog,
		OwnChangeDiff:   ownChangeDiff,
		UpstreamChanges: upstream,
		Precedents:      precedents,
	}))
	if err != nil {
		// Transient LLM error: return so the driver redelivers.
		return Result{}, fmt.Errorf("llm propose: %w", err)
	}
	if res.ProposedSQL == "" {
		return Result{Proposal: proposal.Proposal{Status: proposal.StatusFailed}}, nil
	}

	// Step 1 — candidate artifacts. Written unconditionally for audit: the
	// candidate is the LLM's fix applied to the pre-compiled SQL extracted from
	// object storage (not the real model source).
	candSQLKey := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.sql", in.ReleaseID, in.NodeID, in.Attempt)
	candDiffKey := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.diff", in.ReleaseID, in.NodeID, in.Attempt)

	candSQLURI, err := svc.Artifacts.Write(ctx, candSQLKey, res.ProposedSQL, "text/plain")
	if err != nil {
		return Result{}, fmt.Errorf("write candidate sql: %w", err)
	}
	candDiffURI, err := svc.Artifacts.Write(ctx, candDiffKey, proposal.ComputeUnifiedDiff(candidateSQL, res.ProposedSQL, in.NodeID), "text/plain")
	if err != nil {
		return Result{}, fmt.Errorf("write candidate diff: %w", err)
	}

	// Defaults: final URIs fall back to the candidate unless Step 2 succeeds.
	finalSQLURI, finalDiffURI := candSQLURI, candDiffURI
	sourceResolved := false
	var resolvedFilePath string

	// Step 2 — real-source fix. Asks the LLM to apply the Step-1 diagnosis to
	// the already-resolved candidate source. Degrades silently when the
	// candidate source, the file path, the service mapping, or the LLM result
	// is unavailable, or when the LLM did not improve the source.
	if src, fullPath, ok := resolveValidationSource(ctx, svc, in, filePath, serviceName, candidateSource, res); ok {
		srcSQLURI, srcDiffURI, err := writeSourceArtifacts(ctx, svc, in, src.original, src.corrected)
		if err != nil {
			return Result{}, err
		}
		finalSQLURI, finalDiffURI, sourceResolved = srcSQLURI, srcDiffURI, true
		resolvedFilePath = fullPath
	}

	p := proposal.Proposal{
		Status:              proposal.StatusProposed,
		Confidence:          normalizeConfidence(res.Confidence),
		Rationale:           res.Rationale,
		ProposedSQLURI:      finalSQLURI,
		DiffURI:             finalDiffURI,
		CandidateFixSQLURI:  candSQLURI,
		CandidateFixDiffURI: candDiffURI,
		SourceResolved:      sourceResolved,
		Model:               res.Model,
	}
	// Populate source-location fields only when the real-source step succeeded.
	if sourceResolved {
		p.Repo = in.Repo
		p.CommitSHA = in.CommitSHA
		p.FilePath = resolvedFilePath
	}
	return Result{Proposal: p, SuspectedRoot: res.SuspectedRootCauseNode}, nil
}

// errNonDbtCandidate reports that the failing node's candidate source is not a
// dbt model, on either of two signals: a code bundle entry whose recorded
// runtime is not dbt (its RawCode is then the node's normalized contract
// entry rather than model source) — checked regardless of node_type, since
// the bundle's own recorded runtime is authoritative whenever it is present —
// or, only when the trigger carries no node_type, a fallback repo path that
// does not end in ".sql" (a python node's script, which is the only non-dbt
// source this system tracks a path for; a trigger that does carry a node_type
// trusts it instead of the extension, since a dbt snapshot's source can
// legitimately be a .yml file). Neither can be sent to the LLM nor read as
// model source; the caller turns this into a skipped proposal.
var errNonDbtCandidate = errors.New("candidate source is not a dbt model")

// resolveCandidateSource returns the failing candidate's raw source. Order:
// the release's code bundle (exact failing source, keyed by unique_id, no
// path mapping needed), then the repo file at the release's commit, then ""
// — the caller degrades to the candidate-only proposal. A transient bundle
// fetch error propagates, so the trigger redelivers. A bundle entry that is
// not a dbt node returns errNonDbtCandidate immediately; every permanent bundle
// miss — including a bundle for a different release (NodeSource itself rejects
// the mismatch as ports.ErrNotFound) and a dbt entry with no source text —
// walks down to the repo fallback instead. There, an empty file path, an empty
// service name, or a service with no entry in ServiceRepoPaths still degrades
// quietly to an unresolved source (these are location gaps, not evidence about
// the node's kind). Only when the trigger carries no node_type does a resolved
// path that does not end in ".sql" also return errNonDbtCandidate: a trigger
// that does carry a node_type is trusted over the extension and always
// proceeds to the read (any error there degrades quietly, same as an
// unreadable ".sql" path does).
func resolveCandidateSource(ctx context.Context, svc Services, in Input, filePath, serviceName string) (source, origin string, err error) {
	src, berr := svc.CandidateSource.NodeSource(ctx, in.CodeBundleURI, in.NodeID, in.ReleaseID)
	if berr == nil {
		if src.Runtime != ports.RuntimeDbt {
			return "", "", fmt.Errorf("node %q runtime %q: %w", in.NodeID, src.Runtime, errNonDbtCandidate)
		}
		if src.RawCode != "" {
			return src.RawCode, "bundle", nil
		}
		svc.Logger.Warn("candidate source in bundle is empty; falling back to repo read",
			"node", in.NodeID, "bundle_uri", in.CodeBundleURI)
	} else if !errors.Is(berr, ports.ErrNotFound) {
		return "", "", fmt.Errorf("fetch code bundle: %w", berr)
	} else {
		svc.Logger.Warn("candidate source not in bundle; falling back to repo read",
			"node", in.NodeID, "bundle_uri", in.CodeBundleURI)
	}
	if filePath == "" || serviceName == "" {
		return "", "", nil
	}
	prefix, ok := svc.ServiceRepoPaths[serviceName]
	if !ok {
		return "", "", nil
	}
	// Extension inference applies only when the trigger carries no node_type:
	// that is the one shape where the fallback path's kind is genuinely
	// unknown, since node_type is authoritative whenever the trigger carries
	// one — python already ended the flow earlier in Propose, so every
	// node_type reaching here is some dbt kind, and a dbt snapshot's source
	// is legitimately a .yml file rather than .sql. With no node_type, a
	// non-.sql path is a python node's script (the only non-dbt source this
	// system ever tracks a path for), and is refused without ever being read.
	if in.NodeType == "" && !strings.HasSuffix(filePath, ".sql") {
		return "", "", fmt.Errorf("node %q path %q: %w", in.NodeID, filePath, errNonDbtCandidate)
	}
	full := path.Join(prefix, filePath)
	content, gerr := svc.Source.ReadFile(ctx, in.Repo, in.CommitSHA, full)
	if gerr != nil {
		svc.Logger.Warn("repo fallback read failed; using candidate proposal",
			"node", in.NodeID, "path", full, "error", gerr)
		return "", "", nil
	}
	return content, "github", nil
}

// resolvedValidationSource holds the original model source and the Step-2
// corrected version produced by the LLM.
type resolvedValidationSource struct{ original, corrected string }

// resolveValidationSource performs Step 2: ask the LLM to apply the Step-1
// diagnosis to candidateSource, the failing node's source already resolved by
// resolveCandidateSource. filePath and serviceName come from the single
// Locator call already made by the caller; the ServiceRepoPaths mapping is
// needed here only to build the repository-relative path recorded on the
// proposal for PR targeting, since the source read itself already happened.
// Returns the resolved source, that full path, and ok=true on success.
// Returns ok=false on any degraded path (no candidate source, missing file
// path or service name, no repo mapping, empty/unchanged LLM result, or
// low-confidence LLM result); the caller then keeps the candidate proposal.
// fullPath is empty when ok=false.
func resolveValidationSource(ctx context.Context, svc Services, in Input, filePath, serviceName, candidateSource string, step1 ports.ProposeResult) (resolvedValidationSource, string, bool) {
	if candidateSource == "" {
		svc.Logger.Warn("source fix: no candidate source resolved; using candidate proposal", "node", in.NodeID)
		return resolvedValidationSource{}, "", false
	}
	if filePath == "" || serviceName == "" {
		svc.Logger.Warn("source fix: file path or service name unavailable; using candidate proposal",
			"node", in.NodeID, "file_path", filePath, "service_name", serviceName)
		return resolvedValidationSource{}, "", false
	}
	repoPrefix, ok := svc.ServiceRepoPaths[serviceName]
	if !ok {
		svc.Logger.Warn("source fix: no repo path mapping for service; using candidate proposal",
			"node", in.NodeID, "service_name", serviceName)
		return resolvedValidationSource{}, "", false
	}
	fullPath := path.Join(repoPrefix, filePath)
	out, err := svc.LLM.Propose(ctx, prompt.AssembleSourceFix(svc.Sanitizer.Sanitize(candidateSource), in.NodeID, step1.Rationale))
	if err != nil {
		svc.Logger.Warn("source fix: llm step 2 failed; using candidate proposal",
			"node", in.NodeID, "error", err)
		return resolvedValidationSource{}, "", false
	}
	if out.ProposedSQL == "" || out.ProposedSQL == candidateSource {
		svc.Logger.Warn("source fix: llm step 2 produced no improvement; using candidate proposal",
			"node", in.NodeID)
		return resolvedValidationSource{}, "", false
	}
	if isLowConfidence(out.Confidence) {
		svc.Logger.Warn("source fix: llm step 2 low confidence; using candidate proposal",
			"node", in.NodeID)
		return resolvedValidationSource{}, "", false
	}
	return resolvedValidationSource{original: candidateSource, corrected: out.ProposedSQL}, fullPath, true
}
