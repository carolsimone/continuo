package fixer

import (
	"context"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// pythonParseFixer handles a python-model node whose read SQL the release's
// parser rejected before any Job ran. The trigger's file path names the
// node's script, which the parser never reads; the rejected SQL is one of the
// node's `reads` entries in the contract yaml that declares it. So the fix is
// made in that yaml, found from the node id alone the way the validation lane
// finds it, and it is proven the same way: the corrected contract is packaged
// and the driver submits a python verification run, whose own parse leg is
// what decides whether the SQL now parses and resolves.
//
// Everything but the evidence is shared with pythonValidationFixer: the same
// locate step, the same contractFix orchestration, and the same declaration
// guard, so an answer that deletes the rejected read — which would parse while
// the script still performs that read — is refused here exactly as there. The
// evidence differs in that the error is the parser's own text and there is no
// runner log or code bundle, since a parse rejection precedes both.
type pythonParseFixer struct{}

func (pythonParseFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	schema, table, root, located, cleanup, skip, err := locateContractForFix(ctx, svc, in)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return Result{}, err
	}
	if skip != nil {
		return *skip, nil
	}
	return contractFix(ctx, svc, in, schema, table, root, located,
		buildPythonParseRequest, declarationBreach)
}

// buildPythonParseRequest assembles the python evidence — with the parser's
// error as the excerpt and, by construction, no runner log — and turns it into
// the parse-fix request.
func buildPythonParseRequest(ctx context.Context, svc Services, in Input, located ports.Located) (prompt.ProposeRequest, error) {
	ev, err := pythonEvidence(ctx, svc, in, located)
	if err != nil {
		return prompt.ProposeRequest{}, err // transient read: the driver redelivers
	}
	return prompt.AssemblePythonParseFix(ev), nil
}
