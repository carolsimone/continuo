package fixer

import (
	"context"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// compileFixer fixes a dbt compile failure. It reads the offending file (the
// classifier-extracted FilePath, service resolved from NodeID) and, when that
// file is a .sql, also its co-located schema.yml siblings and the service's
// dbt_project.yml. The model chooses which shown file to change.
type compileFixer struct{}

func (compileFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	return sourceFileFix{gather: compileGather, build: compileBuild, interpret: singleFileInterpret}.Propose(ctx, svc, in)
}

func compileGather(ctx context.Context, svc Services, in Input) (Gathered, string, error) {
	if in.FilePath == "" { // project-level error: no models/ path in the log
		return Gathered{}, "the compile error names no source file, so there is no file to fix", nil
	}
	// For compile the NodeID IS the service discriminator (a synthetic id).
	return gatherSourceFile(ctx, svc, in, in.NodeID)
}

func compileBuild(svc Services, g Gathered, in Input, dbtLog string, precedents []prompt.Precedent) prompt.ProposeRequest {
	files := make([]prompt.NamedFile, 0, len(g.Order))
	for _, p := range g.Order {
		// Sanitize each shown file before it leaves for the external LLM; the raw
		// content is kept in g.Files for the diff and no-op check.
		files = append(files, prompt.NamedFile{Path: p, Content: svc.Sanitizer.Sanitize(g.Files[p])})
	}
	return prompt.AssembleCompileFix(files, dbtLog, in.NodeID, precedents)
}

// singleFileInterpret is the shared interpreter for every single-file Fixer:
// it resolves the model's target_file to exactly one of the shown files, rejects
// a no-op or low-confidence answer, and otherwise reports the proposed rewrite.
// It has nothing compile-specific in it, so compileFixer and duplicateTableFixer
// share it verbatim.
func singleFileInterpret(res ports.ProposeResult, g Gathered, in Input) Outcome {
	target, ok := resolveTarget(res.TargetFile, g)
	if !ok {
		return Outcome{Status: proposal.StatusSkipped} // no shown file to safely apply the fix to
	}
	if res.ProposedContent == "" || res.ProposedContent == g.Files[target] {
		return Outcome{Status: proposal.StatusFailed} // no-op
	}
	if isLowConfidence(res.Confidence) {
		return Outcome{Status: proposal.StatusFailed} // model could not determine a safe fix
	}
	return Outcome{
		Status:           proposal.StatusProposed,
		TargetFile:       target,
		CorrectedContent: res.ProposedContent,
		Confidence:       res.Confidence,
		Rationale:        res.Rationale,
		Model:            res.Model,
		SuspectedRoot:    res.SuspectedRootCauseNode,
	}
}

// resolveTarget maps the model's target_file to exactly one of the files that
// were shown to it, returning ok=false when no safe choice exists. It never
// invents a target the model was not shown (guarding against a PR overwriting an
// arbitrary path), but it tolerates a differently-rooted spelling — the model
// may echo "models/x.sql" for a file shown as "services/svc/models/x.sql" — by
// accepting a path-suffix match when it is unambiguous:
//   - empty target: the offending file, but only when it is the sole shown file
//     (otherwise the intended file is ambiguous → skip);
//   - exact key match: that file;
//   - exactly one shown file ending in "/"+target: that file;
//   - anything else (unknown path, or ambiguous suffix): skip.
func resolveTarget(target string, g Gathered) (string, bool) {
	if target == "" {
		if len(g.Order) == 1 {
			return g.Primary, true
		}
		return "", false
	}
	if _, ok := g.Files[target]; ok {
		return target, true
	}
	var matches []string
	for _, p := range g.Order {
		if strings.HasSuffix(p, "/"+target) {
			matches = append(matches, p)
		}
	}
	if len(matches) == 1 {
		return matches[0], true
	}
	return "", false
}
