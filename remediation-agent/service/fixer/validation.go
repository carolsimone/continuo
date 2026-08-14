package fixer

import (
	"context"
	"errors"
	"fmt"
	"path"
	"unicode/utf8"

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
	// Validation with no candidate SQL: nothing to fix. This is decided before
	// any log fetch so a transiently unreadable log cannot turn the intended
	// skip into a redelivery. A python node's validation rejection carries no
	// candidate SQL, so this is also what keeps this path from ever calling the
	// LLM for a python node.
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

	// PR targeting: the node's source path and owning service come from the
	// topology (the validation trigger carries neither). Best-effort — a
	// failed lookup degrades Step 2 to the candidate-only proposal.
	filePath, serviceName, err := svc.Locator.Locate(ctx, in.NodeID)
	if err != nil {
		svc.Logger.Warn("node location unavailable; source fix will degrade to candidate",
			"node", in.NodeID, "error", err)
		filePath, serviceName = "", ""
	}

	// Candidate source from the release's code bundle; the repo read is only
	// the fallback. A permanent bundle miss is a degraded path, a transient
	// fetch error redelivers the trigger.
	candidateSource, sourceOrigin, err := resolveCandidateSource(ctx, svc, in, filePath, serviceName)
	if err != nil {
		return Result{}, err
	}
	svc.Logger.Info("validation fix: candidate source resolved", "node", in.NodeID, "source_origin", sourceOrigin)

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

// resolveCandidateSource returns the failing candidate's raw source. Order:
// the release's code bundle (exact failing source, keyed by unique_id, no
// path mapping needed), then the repo file at the release's commit, then ""
// — the caller degrades to the candidate-only proposal. Only a transient
// bundle fetch error propagates, so the trigger redelivers; every permanent
// miss walks down the ladder.
func resolveCandidateSource(ctx context.Context, svc Services, in Input, filePath, serviceName string) (source, origin string, err error) {
	src, berr := svc.CandidateSource.NodeSource(ctx, in.CodeBundleURI, in.NodeID)
	if berr == nil {
		return src.RawCode, "bundle", nil
	}
	if !errors.Is(berr, ports.ErrNotFound) {
		return "", "", fmt.Errorf("fetch code bundle: %w", berr)
	}
	svc.Logger.Warn("candidate source not in bundle; falling back to repo read",
		"node", in.NodeID, "bundle_uri", in.CodeBundleURI)
	if filePath == "" || serviceName == "" {
		return "", "", nil
	}
	prefix, ok := svc.ServiceRepoPaths[serviceName]
	if !ok {
		return "", "", nil
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
