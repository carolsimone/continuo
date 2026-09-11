package fixer

import (
	"context"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// parseFixer fixes a node whose compiled SQL topology-controller's parser
// rejected: invalid SQL, or a relation referenced without its schema. It reads
// the same files as the compile lane (the offending model, co-located yml,
// dbt_project.yml) but resolves the service from the trigger's Service field
// rather than the node id, and shows the model the parser's error text in
// place of a dbt log, since no Job ran.
//
// A python target is skipped outright, before any read is attempted. A python
// node's SQL is not in the file FilePath names — that path is the contract's
// script entry, a program the parser never reads. The SQL the parser rejected
// is one of the node's `reads` entries in its service's contract.yaml, and
// this system carries no repository path for contract.yaml at all, so editing
// the script would leave the rejected SQL untouched and the release would be
// rejected again on the next parse. Skipping also keeps the driver from
// verifying such a fix as a dbt run: a parse fix packages no
// VerificationContract, so a proposal here would submit a dbt verification
// run for a python service.
type parseFixer struct{}

func (parseFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	return singleShot{gather: parseGather, build: parseBuild, interpret: singleFileInterpret}.Propose(ctx, svc, in)
}

func parseGather(ctx context.Context, svc Services, in Input) (Gathered, bool, error) {
	if pkg_model.NodeType(in.NodeType).IsPython() {
		svc.Logger.Info("parse fix: target is a python node; skipping — "+
			"the SQL the parser rejected lives in the node's reads in its service's "+
			"contract.yaml, whose repository path this system does not carry, so editing "+
			"the script the trigger names cannot repair the contract",
			"node", in.NodeID, "node_type", in.NodeType)
		return Gathered{}, true, nil
	}
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
