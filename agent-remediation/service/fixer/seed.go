package fixer

import (
	"context"
	"fmt"

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
	return sourceFileFix{gather: seedGather, build: seedBuild, interpret: seedInterpret}.Propose(ctx, svc, in)
}

func seedGather(ctx context.Context, svc Services, in Input) (Gathered, string, error) {
	filePath, service := in.FilePath, in.Service
	if filePath == "" || service == "" {
		fp, svcName, err := svc.Locator.Locate(ctx, in.NodeID)
		if err != nil {
			return Gathered{}, fmt.Sprintf("the seed's source location could not be resolved from the graph: %v", err), nil
		}
		if filePath == "" {
			filePath = fp
		}
		if service == "" {
			service = svcName
		}
	}
	if filePath == "" || service == "" {
		return Gathered{}, "neither the trigger nor the graph names the seed's csv file and service, so there is no file to fix", nil
	}
	return readOffendingFile(ctx, svc, in, service, filePath)
}

func seedBuild(svc Services, g Gathered, in Input, dbtLog string, precedents []prompt.Precedent) prompt.ProposeRequest {
	// Sanitize the CSV before it leaves for the external LLM; the raw content is
	// kept in g.Files for the diff and no-op check.
	return prompt.AssembleSeedFix(g.Primary, svc.Sanitizer.Sanitize(g.Files[g.Primary]), dbtLog, in.NodeID, precedents)
}

func seedInterpret(res ports.ProposeResult, g Gathered, in Input) Outcome {
	if res.ProposedContent == "" || res.ProposedContent == g.Files[g.Primary] {
		return Outcome{Status: proposal.StatusFailed, Rationale: "the model returned the csv unchanged, which is not a fix"}
	}
	if isLowConfidence(res.Confidence) {
		// The model could not infer the bad value; do not propose a guessed CSV.
		return Outcome{Status: proposal.StatusFailed, Rationale: "the model could not infer the bad value with confidence, so no guessed csv is proposed"}
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
