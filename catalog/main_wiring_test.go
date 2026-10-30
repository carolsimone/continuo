package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strings"
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

// TestMain_ConsumesOnlyTopologyAndCaseBaseStreams pins the streams catalog
// reads: the two release.promoted:v2 groups (topology swap, code versions) and
// the three case-base groups. run-controller plans, dispatches, unblocks and finalizes
// runs, so catalog consumes no run stream.
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
		"CatalogReleaseTopology",
		"CatalogReleaseVersions",
		"CatalogRemediationPrClosedProvenance",
		"CatalogRemediationPrOpenedProposals",
		"CatalogRemediationRequestedRejections",
		"ReleasePromotedV2",
		"RemediationPrClosedV1",
		"RemediationPrOpenedV1",
		"RemediationRequestedV2",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("main.go names streams %v, want %v", got, want)
	}
}

// TestMain_RunsNoRunReconciler pins that catalog holds no run status to
// converge: it imports no reconciler.
func TestMain_RunsNoRunReconciler(t *testing.T) {
	for _, imp := range parseMain(t).Imports {
		if path := strings.Trim(imp.Path.Value, `"`); strings.HasSuffix(path, "/internal/reconciler") {
			t.Fatalf("main.go imports %s", path)
		}
	}
}

// TestMain_ProbesRedisAndNeo4j pins the dependencies readiness checks: the two
// stores catalog reads and writes. It keeps no SQL database, so it registers no
// SQL probe.
func TestMain_ProbesRedisAndNeo4j(t *testing.T) {
	var got []string
	ast.Inspect(parseMain(t), func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "AddProbe" {
			return true
		}
		if lit, ok := call.Args[0].(*ast.BasicLit); ok {
			got = append(got, strings.Trim(lit.Value, `"`))
		}
		return true
	})
	slices.Sort(got)
	if want := []string{"neo4j", "redis"}; !slices.Equal(got, want) {
		t.Fatalf("main.go registers readiness probes %v, want %v", got, want)
	}
}

// TestMain_ImportsNoSQLPackage pins that catalog keeps all of its state in
// Neo4j: main.go imports no SQL connection, no message-processing dedup table
// and no outbox.
func TestMain_ImportsNoSQLPackage(t *testing.T) {
	for _, imp := range parseMain(t).Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		for _, banned := range []string{"/pkg/db", "/pkg/messageprocessing", "/pkg/outbox"} {
			if strings.HasSuffix(path, banned) {
				t.Fatalf("main.go imports %s", path)
			}
		}
	}
}
