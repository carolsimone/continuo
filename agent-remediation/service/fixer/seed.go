package fixer

import (
	"context"
	"errors"
	"path"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// seedFixer fixes a dbt seed (CSV) load failure. It reads the failing CSV
// (path + service threaded from the candidate topology, the orchestrator
// graph's NodeLocator as fallback) and asks for a corrected CSV, with an
// honest low-confidence skip when the bad value cannot be inferred.
type seedFixer struct{}

func (seedFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	return singleShot{gather: seedGather, build: seedBuild, interpret: seedInterpret}.Propose(ctx, svc, in)
}

func seedGather(ctx context.Context, svc Services, in Input) (Gathered, bool, error) {
	filePath, service := in.FilePath, in.Service
	if filePath == "" || service == "" {
		fp, svcName, err := svc.Locator.Locate(ctx, in.NodeID)
		if err != nil {
			svc.Logger.Warn("seed fix: node location unavailable; skipping", "node", in.NodeID, "error", err)
			return Gathered{}, true, nil
		}
		if filePath == "" {
			filePath = fp
		}
		if service == "" {
			service = svcName
		}
	}
	if filePath == "" || service == "" {
		svc.Logger.Warn("seed fix: file path or service unavailable; skipping", "node", in.NodeID)
		return Gathered{}, true, nil
	}
	prefix, ok := svc.ServiceRepoPaths[service]
	if !ok {
		svc.Logger.Warn("seed fix: no repo path mapping for service; skipping", "service", service)
		return Gathered{}, true, nil
	}
	full := path.Join(prefix, filePath)
	content, err := svc.Source.ReadFile(ctx, in.Repo, in.CommitSHA, full)
	if err != nil {
		if errors.Is(err, ports.ErrSourceNotFound) {
			svc.Logger.Warn("seed fix: csv not found; skipping", "path", full)
			return Gathered{}, true, nil
		}
		return Gathered{}, false, err // transient: redeliver
	}
	return Gathered{Files: map[string]string{full: content}, Order: []string{full}, Primary: full}, false, nil
}

func seedBuild(svc Services, g Gathered, in Input, dbtLog string, precedents []prompt.Precedent) prompt.ProposeRequest {
	// Sanitize the CSV before it leaves for the external LLM; the raw content is
	// kept in g.Files for the diff and no-op check.
	return prompt.AssembleSeedFix(g.Primary, svc.Sanitizer.Sanitize(g.Files[g.Primary]), dbtLog, in.NodeID, precedents)
}

func seedInterpret(res ports.ProposeResult, g Gathered, in Input) Outcome {
	if res.ProposedContent == "" || res.ProposedContent == g.Files[g.Primary] {
		return Outcome{Status: proposal.StatusFailed} // unchanged / no-op → not a fix
	}
	if isLowConfidence(res.Confidence) {
		// The model could not infer the bad value; do not propose a guessed CSV.
		return Outcome{Status: proposal.StatusFailed}
	}
	return Outcome{
		Status:           proposal.StatusProposed,
		TargetFile:       g.Primary,
		CorrectedContent: res.ProposedContent,
		Confidence:       res.Confidence,
		Rationale:        res.Rationale,
		Model:            res.Model,
	}
}
