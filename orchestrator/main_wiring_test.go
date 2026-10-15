package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"
)

// parseMain parses this service's main.go.
func parseMain(t *testing.T) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	return f
}

// TestMain_ConsumesOnlyTopologyAndCaseBaseStreams pins the streams orchestrator
// reads: the two release.promoted:v2 groups (topology swap, code versions) and
// the three case-base groups. state plans, dispatches, unblocks and finalizes
// runs, so orchestrator consumes no run stream.
func TestMain_ConsumesOnlyTopologyAndCaseBaseStreams(t *testing.T) {
	seen := map[string]bool{}
	ast.Inspect(parseMain(t), func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "streams" {
			seen[sel.Sel.Name] = true
		}
		return true
	})
	got := make([]string, 0, len(seen))
	for name := range seen {
		got = append(got, name)
	}
	slices.Sort(got)
	want := []string{
		"OrchestratorReleasePromoted",
		"OrchestratorReleasePromotedVersions",
		"OrchestratorRemediationPrClosedProvenance",
		"OrchestratorRemediationPrOpenedProposals",
		"OrchestratorRemediationRequestedRejections",
		"ReleasePromotedV2",
		"RemediationPrClosedV1",
		"RemediationPrOpenedV1",
		"RemediationRequestedV2",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("main.go names streams %v, want %v", got, want)
	}
}
