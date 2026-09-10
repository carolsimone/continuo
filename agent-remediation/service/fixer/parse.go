package fixer

import (
	"context"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
)

// parseFixer fixes a node whose compiled SQL topology-controller's parser
// rejected: invalid SQL, or a relation referenced without its schema. It reads
// the same files as the compile lane (the offending model, co-located yml,
// dbt_project.yml) but resolves the service from the trigger's Service field
// rather than the node id, and shows the model the parser's error text in
// place of a dbt log, since no Job ran.
type parseFixer struct{}

func (parseFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	return singleShot{gather: parseGather, build: parseBuild, interpret: singleFileInterpret}.Propose(ctx, svc, in)
}

func parseGather(ctx context.Context, svc Services, in Input) (Gathered, bool, error) {
	if in.FilePath == "" {
		svc.Logger.Info("parse fix: trigger carries no file path; skipping", "node", in.NodeID)
		return Gathered{}, true, nil
	}
	if in.Service == "" {
		svc.Logger.Info("parse fix: trigger carries no service; skipping", "node", in.NodeID)
		return Gathered{}, true, nil
	}
	return gatherSourceFile(ctx, svc, in, in.Service, "parse fix")
}

// parseBuild ignores the dbt log argument: a parse failure has none, and the
// error the model needs is the trigger's excerpt.
func parseBuild(svc Services, g Gathered, in Input, _ string, precedents []prompt.Precedent) prompt.ProposeRequest {
	files := make([]prompt.NamedFile, 0, len(g.Order))
	for _, p := range g.Order {
		files = append(files, prompt.NamedFile{Path: p, Content: svc.Sanitizer.Sanitize(g.Files[p])})
	}
	return prompt.AssembleParseFix(files, svc.Sanitizer.Sanitize(in.ErrorExcerpt), in.Service, in.NodeID, precedents)
}
