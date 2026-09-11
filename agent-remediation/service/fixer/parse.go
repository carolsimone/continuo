package fixer

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// parseFixer fixes a dbt node whose compiled SQL topology-controller's parser
// rejected: invalid SQL, or a relation referenced without its schema. It reads
// the same files as the compile lane (the offending model, co-located yml,
// dbt_project.yml) but resolves the service from the trigger's Service field
// rather than the node id, and shows the model the parser's error text in
// place of a dbt log, since no Job ran.
//
// A python-model node never reaches this type: For routes it to
// pythonParseFixer, because its rejected SQL is a read in its contract yaml.
// A python-csv node does reach it and is refused with a recorded reason: its
// only read is an S3 URI, never SQL, so the parser has nothing to reject in
// it, and a rejection naming one is not something this system can repair.
type parseFixer struct{}

func (parseFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	return sourceFileFix{gather: parseGather, build: parseBuild, interpret: singleFileInterpret}.Propose(ctx, svc, in)
}

func parseGather(ctx context.Context, svc Services, in Input) (Gathered, string, error) {
	if pkg_model.NodeType(in.NodeType).IsPython() {
		return Gathered{}, fmt.Sprintf("%s is a %s node: its reads are declared in its service's contract.yaml, "+
			"not in SQL the parser can reject, so remediation has no fix to offer; correct the contract by hand",
			in.NodeID, in.NodeType), nil
	}
	if in.FilePath == "" {
		return Gathered{}, "the parse rejection names no source file, so there is no file to fix", nil
	}
	if in.Service == "" {
		return Gathered{}, "the parse rejection names no service, so the source cannot be located", nil
	}
	return gatherSourceFile(ctx, svc, in, in.Service)
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
