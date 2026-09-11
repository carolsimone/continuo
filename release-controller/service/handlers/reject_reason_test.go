package handlers

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// TestParseReasonsAreDeclaredRejectReasons pins the parse leg's kind→reason
// table to the reject_reason vocabulary: every contract parse kind maps to a
// reason, and every reason it maps to is one release.rejected:v1 declares.
func TestParseReasonsAreDeclaredRejectReasons(t *testing.T) {
	kinds := pkg_model.ParseFailureKinds()
	if len(parseReasons) != len(kinds) {
		t.Fatalf("parseReasons has %d entries, contract declares %d kinds", len(parseReasons), len(kinds))
	}
	for _, kind := range kinds {
		reason, ok := parseReasons[kind]
		if !ok {
			t.Errorf("kind %q has no reject reason", kind)
			continue
		}
		if !reason.IsValid() {
			t.Errorf("kind %q maps to %q, which is not a declared reject reason", kind, reason)
		}
	}
	if got := ParseReason("a-kind-this-build-does-not-know"); got != pkg_model.RejectReasonInternalError {
		t.Errorf("unknown kind resolved to %q, want %q", got, pkg_model.RejectReasonInternalError)
	}
}

// TestCompileRejectionReasonsAreDeclared pins every reason the compile leg can
// derive from a pod's failed-container attribution.
func TestCompileRejectionReasonsAreDeclared(t *testing.T) {
	cases := map[string]pkg_model.RejectReason{
		"parse-prod":      pkg_model.RejectReasonParseRehearsalFailed,
		"parse-candidate": pkg_model.RejectReasonParseRehearsalFailed,
		"upload":          pkg_model.RejectReasonArtifactUploadFailed,
		"":                pkg_model.RejectReasonCompileFailed,
		"some-other":      pkg_model.RejectReasonCompileFailed,
	}
	for container, want := range cases {
		got, _ := compileRejection([]NodeResult{{NodeID: "core", Status: "failed", FailedContainer: container}})
		if got != want {
			t.Errorf("failed_container %q: reason %q, want %q", container, got, want)
		}
		if !got.IsValid() {
			t.Errorf("failed_container %q: reason %q is not a declared reject reason", container, got)
		}
	}
}

// TestEveryFailCallNamesTheVocabulary walks the production files of this
// package and fails on any Fail(...) call whose reason argument is a bare
// string literal. A reason reaches remediation and the UI, so it must be a
// reject_reason constant from pkg/domain/model, which the vocabulary tests pin
// to contract.yaml.
func TestEveryFailCallNamesTheVocabulary(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	calls := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Fail" || len(call.Args) == 0 {
				return true
			}
			calls++
			if lit, ok := call.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
				t.Errorf("%s: Fail(%s, …) passes a bare literal — use a pkg/domain/model RejectReason constant",
					fset.Position(lit.Pos()), lit.Value)
			}
			return true
		})
	}
	if calls == 0 {
		t.Fatal("guard found no Fail(...) call — the scan is broken")
	}
}

// TestIsHealableReasonMatchesTheContract pins the retry gate to the
// vocabulary's healable flags, over every declared reason.
func TestIsHealableReasonMatchesTheContract(t *testing.T) {
	healable := map[pkg_model.RejectReason]bool{
		pkg_model.RejectReasonCompileFailed:        true,
		pkg_model.RejectReasonSeedBuildFailed:      true,
		pkg_model.RejectReasonValidationFailed:     true,
		pkg_model.RejectReasonDuplicateTable:       true,
		pkg_model.RejectReasonInvalidSQL:           true,
		pkg_model.RejectReasonUnqualifiedReference: true,
	}
	for _, reason := range pkg_model.RejectReasons() {
		if got := IsHealableReason(string(reason)); got != healable[reason] {
			t.Errorf("IsHealableReason(%q) = %v, want %v", reason, got, healable[reason])
		}
	}
	if IsHealableReason("") || IsHealableReason("a_reason_this_build_does_not_know") {
		t.Error("an undeclared reason must not be healable")
	}
}
