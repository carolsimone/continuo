package fixer

import (
	"context"
	"fmt"
	"path"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

const (
	// maxUpstreamDiffs bounds how many recently-changed ancestors are diffed into
	// the prompt, keeping the token footprint predictable on wide fan-in models.
	maxUpstreamDiffs = 5
	// maxUpstreamDiffBytes truncates a single upstream diff so one large refactor
	// cannot dominate the prompt.
	maxUpstreamDiffBytes = 8192
)

// gatherUpstreamDiffs fetches, best-effort, the diff of the recent change to each
// of the most-recently-changed upstream ancestors. Ancestors arrive already
// ordered most-recently-changed first. Only an ancestor that carries a stamped
// commit (i.e. changed in some release), a repo, a file path, and a known
// service-to-repo-path mapping is eligible; at most maxUpstreamDiffs are fetched.
// Any per-ancestor read error is logged and skipped, and each diff is truncated
// to maxUpstreamDiffBytes. A whole failure yields an empty slice, leaving the
// prompt at its metadata-only content.
func gatherUpstreamDiffs(ctx context.Context, svc Services, ancestors []prompt.Ancestor) []prompt.UpstreamDiff {
	out := make([]prompt.UpstreamDiff, 0, maxUpstreamDiffs)
	for _, a := range ancestors {
		if len(out) >= maxUpstreamDiffs {
			break
		}
		if a.LastCommitSHA == "" || a.LastRepo == "" || a.FilePath == "" {
			continue
		}
		prefix, ok := svc.ServiceRepoPaths[a.ServiceName]
		if !ok {
			continue
		}
		fullPath := path.Join(prefix, a.FilePath)
		diff, err := svc.Source.CommitFileDiff(ctx, a.LastRepo, a.LastCommitSHA, fullPath)
		if err != nil {
			svc.Logger.Warn("upstream diff unavailable; skipping ancestor",
				"node", a.NodeID, "path", fullPath, "commit", a.LastCommitSHA, "error", err)
			continue
		}
		out = append(out, prompt.UpstreamDiff{
			NodeID:      a.NodeID,
			ServiceName: a.ServiceName,
			Diff:        truncateDiff(diff, maxUpstreamDiffBytes),
		})
	}
	return out
}

// truncateDiff caps a diff at max bytes, appending a marker when it was cut.
func truncateDiff(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n… (diff truncated)"
}

// validationFixer handles validation-rejection failures, which carry a
// candidate SQL (the pre-compiled SQL extracted from object storage at the
// point of failure). It runs a two-step flow:
//
//  1. Ask the LLM to fix the candidate SQL given the dbt error. The result is
//     written as candidate artifacts unconditionally, for audit — it is the
//     LLM's fix applied to the pre-compiled SQL, not the real model source.
//  2. Best-effort: resolve the real model source from version control via
//     Ancestry + the service-to-repo-path mapping, and ask the LLM to apply the
//     Step-1 diagnosis to it. On success the final proposal points to the
//     source artifacts; on any degraded path it falls back to the candidate.
type validationFixer struct{}

func (validationFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	// Validation with no candidate SQL: nothing to fix. This is decided before
	// any log fetch so a transiently unreadable log cannot turn the intended
	// skip into a redelivery.
	if in.CandidateSQLURI == "" {
		return Result{Proposal: proposal.Proposal{Status: proposal.StatusSkipped}}, nil
	}

	candidateSQL, err := svc.Evidence.Fetch(ctx, in.CandidateSQLURI)
	if err != nil {
		return Result{}, fmt.Errorf("fetch candidate sql: %w", err)
	}

	dbtLog, err := loadDBTLog(ctx, svc, in.DBTLogURI)
	if err != nil {
		return Result{}, err // transient log read error: driver redelivers
	}

	// Ancestry is best-effort: proceed without upstream context on error.
	// filePath and serviceName are forwarded to resolveSource so it does not
	// need a second NodeContext call.
	filePath, serviceName, ancestors, err := svc.Ancestry.NodeContext(ctx, in.NodeID)
	if err != nil {
		svc.Logger.Warn("ancestry unavailable; proceeding without upstream context",
			"node", in.NodeID, "error", err)
		ancestors = nil
		filePath, serviceName = "", ""
	}

	// Best-effort: the diffs of what recently changed upstream are the highest-
	// signal evidence for a cross-service validation break. A failure here leaves
	// the prompt at its metadata-only content.
	upstreamDiffs := gatherUpstreamDiffs(ctx, svc, ancestors)

	res, err := svc.LLM.Propose(ctx, prompt.Assemble(prompt.Evidence{
		NodeID:         in.NodeID,
		ErrorSignature: in.ErrorSignature,
		CandidateSQL:   candidateSQL,
		DBTLog:         dbtLog,
		Repo:           in.Repo,
		CommitSHA:      in.CommitSHA,
		Ancestors:      ancestors,
		UpstreamDiffs:  upstreamDiffs,
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

	// Step 2 — real-source fix. Fetches the model source from version control
	// and asks the LLM to apply the Step-1 diagnosis to it. Degrades silently
	// when the file path, service mapping, source read, or LLM result is
	// unavailable, or when the LLM did not improve the source.
	if src, fullPath, ok := resolveValidationSource(ctx, svc, in, filePath, serviceName, res); ok {
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

// resolvedValidationSource holds the original model source and the Step-2
// corrected version produced by the LLM.
type resolvedValidationSource struct{ original, corrected string }

// resolveValidationSource performs Step 2: fetch the real model source from
// version control and ask the LLM to apply the Step-1 diagnosis to it.
// filePath and serviceName come from the single NodeContext call already made
// by the caller. Returns the resolved source, the full repository-relative
// file path used for the read, and ok=true on success. Returns ok=false on any
// degraded path (missing file path or service name, no repo mapping, source
// read error, empty/unchanged LLM result, or low-confidence LLM result); the
// caller then keeps the candidate proposal. fullPath is empty when ok=false.
func resolveValidationSource(ctx context.Context, svc Services, in Input, filePath, serviceName string, step1 ports.ProposeResult) (resolvedValidationSource, string, bool) {
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
	original, err := svc.Source.ReadFile(ctx, in.Repo, in.CommitSHA, fullPath)
	if err != nil {
		svc.Logger.Warn("source fix: github read failed; using candidate proposal",
			"node", in.NodeID, "path", fullPath, "error", err)
		return resolvedValidationSource{}, "", false
	}
	out, err := svc.LLM.Propose(ctx, prompt.AssembleSourceFix(svc.Sanitizer.Sanitize(original), in.NodeID, step1.Rationale))
	if err != nil {
		svc.Logger.Warn("source fix: llm step 2 failed; using candidate proposal",
			"node", in.NodeID, "error", err)
		return resolvedValidationSource{}, "", false
	}
	if out.ProposedSQL == "" || out.ProposedSQL == original {
		svc.Logger.Warn("source fix: llm step 2 produced no improvement; using candidate proposal",
			"node", in.NodeID)
		return resolvedValidationSource{}, "", false
	}
	if isLowConfidence(out.Confidence) {
		svc.Logger.Warn("source fix: llm step 2 low confidence; using candidate proposal",
			"node", in.NodeID)
		return resolvedValidationSource{}, "", false
	}
	return resolvedValidationSource{original: original, corrected: out.ProposedSQL}, fullPath, true
}
