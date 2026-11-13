package streams_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const (
	remediationModule   = "agent-remediation"
	remediationRoot     = "github.com/carolsimone/continuo/agent-remediation"
	remediationSteps    = remediationRoot + "/service/steps"
	remediationPorts    = remediationRoot + "/service/ports"
	remediationDomain   = remediationRoot + "/domain/remediation"
	remediationRegistry = "agent-remediation/service/runner/registry.go"
	remediationRunner   = "agent-remediation/service/runner"
	remediationStepsDir = "agent-remediation/service/steps"
)

// remediationModelAndGateDirs are the packages of agent-remediation's service
// layer that may name ports.LLMProvider or ports.VerificationPipeline: the
// runner and its steps, the ports themselves, the model's cache and prompt log,
// and the conformance suite, which supplies scripted stand-ins for both.
// remediationFrozenDirs are today's lane code, which keeps its direct access
// until it is removed; each entry is dropped from the list on its removal.
var remediationModelAndGateDirs = append([]string{
	"agent-remediation/service/runner",
	"agent-remediation/service/steps",
	"agent-remediation/service/ports",
	"agent-remediation/service/llmcache",
	"agent-remediation/service/promptlog",
	"agent-remediation/service/conformance",
}, remediationFrozenDirs...)

var remediationFrozenDirs = []string{
	"agent-remediation/service/handlers",
	"agent-remediation/service/fixer",
	"agent-remediation/service/verification",
}

// walkRemediationGoFiles parses every non-test .go file under dir (relative to
// the repository root), calls fn with the file's slash-separated path relative
// to the root, and returns how many files it inspected.
func walkRemediationGoFiles(t *testing.T, dir string, fn func(rel string, fset *token.FileSet, f *ast.File)) int {
	t.Helper()
	root := repoRootFromTest(t)
	fset := token.NewFileSet()
	checked := 0
	err := filepath.WalkDir(filepath.Join(root, filepath.FromSlash(dir)), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", p, perr)
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			t.Fatalf("relative path of %s: %v", p, rerr)
		}
		checked++
		fn(filepath.ToSlash(rel), fset, f)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return checked
}

// underRemediationDir reports whether the slash-separated file path rel sits in
// dir or in a directory below it.
func underRemediationDir(rel, dir string) bool {
	return strings.HasPrefix(path.Dir(rel)+"/", dir+"/")
}

// remediationStepImportsIn returns the imports of f that name a package under
// agent-remediation/service/steps, unless rel is a file that may import them:
// the runner's registry, or any file of service/steps itself.
func remediationStepImportsIn(rel string, f *ast.File) []string {
	if rel == remediationRegistry || underRemediationDir(rel, remediationStepsDir) {
		return nil
	}
	var out []string
	for _, imp := range f.Imports {
		p := strings.Trim(imp.Path.Value, `"`)
		if p == remediationSteps || strings.HasPrefix(p, remediationSteps+"/") {
			out = append(out, p)
		}
	}
	return out
}

// remediationQualifiedUses returns every selector pkg.Name in f whose pkg is the
// local name of the import pkgPath and whose Name is one of names, as the
// selector's text ("ports.LLMProvider") and its position.
func remediationQualifiedUses(f *ast.File, pkgPath string, names ...string) (uses []string, positions []token.Pos) {
	local := ""
	for _, imp := range f.Imports {
		if strings.Trim(imp.Path.Value, `"`) != pkgPath {
			continue
		}
		switch {
		case imp.Name == nil:
			local = path.Base(pkgPath)
		case imp.Name.Name == "_" || imp.Name.Name == ".":
			return nil, nil
		default:
			local = imp.Name.Name
		}
	}
	if local == "" {
		return nil, nil
	}
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok || id.Name != local {
			return true
		}
		for _, name := range names {
			if sel.Sel.Name == name {
				uses = append(uses, id.Name+"."+name)
				positions = append(positions, sel.Pos())
			}
		}
		return true
	})
	return uses, positions
}

// remediationModelAndGateUsesIn returns the uses of ports.LLMProvider and
// ports.VerificationPipeline in f.
func remediationModelAndGateUsesIn(f *ast.File) ([]string, []token.Pos) {
	return remediationQualifiedUses(f, remediationPorts, "LLMProvider", "VerificationPipeline")
}

// remediationCheckedConstructorUsesIn returns the uses of remediation.NewChecked
// and remediation.NewVerified in f.
func remediationCheckedConstructorUsesIn(f *ast.File) ([]string, []token.Pos) {
	return remediationQualifiedUses(f, remediationDomain, "NewChecked", "NewVerified")
}

// remediationModelAndGateAllowed reports whether the file at rel may name the
// model and the verification gate.
func remediationModelAndGateAllowed(rel string) bool {
	for _, dir := range remediationModelAndGateDirs {
		if underRemediationDir(rel, dir) {
			return true
		}
	}
	return false
}

// TestRemediationStepsAreImportedOnlyByTheRegistry fails when a non-test file of
// agent-remediation, other than the runner's registry and the step packages
// themselves, imports a package under service/steps. The registry is the one
// place that knows the steps, so the runner and everything else reach a step by
// its name.
func TestRemediationStepsAreImportedOnlyByTheRegistry(t *testing.T) {
	checked := walkRemediationGoFiles(t, remediationModule, func(rel string, fset *token.FileSet, f *ast.File) {
		for _, p := range remediationStepImportsIn(rel, f) {
			t.Errorf("%s: imports %q — only %s and the packages under service/steps import a step; every other caller reaches a step by name through the registry",
				rel, p, remediationRegistry)
		}
	})
	if checked == 0 {
		t.Fatal("guard inspected no agent-remediation file — its discovery is broken")
	}
}

// TestOnlyTheRunnerReachesTheModelAndTheGate fails when a non-test file of
// agent-remediation's service layer outside remediationModelAndGateDirs names
// ports.LLMProvider or ports.VerificationPipeline. The runner hands a step the
// model and the gate through the step contract, so a step or a handler never
// holds them on its own. Adapters implement the ports and are not scanned.
func TestOnlyTheRunnerReachesTheModelAndTheGate(t *testing.T) {
	checked := walkRemediationGoFiles(t, remediationModule+"/service", func(rel string, fset *token.FileSet, f *ast.File) {
		if remediationModelAndGateAllowed(rel) {
			return
		}
		uses, positions := remediationModelAndGateUsesIn(f)
		for i, use := range uses {
			t.Errorf("%s:%d: names %s — only the runner, the steps and the packages in remediationModelAndGateDirs reach the model and the verification gate",
				rel, fset.Position(positions[i]).Line, use)
		}
	})
	if checked == 0 {
		t.Fatal("guard inspected no agent-remediation service file — its discovery is broken")
	}
}

// TestRemediationGuardFrozenPackagesExist fails when a folder of
// remediationFrozenDirs is not a directory, so removing a frozen package drops
// it from the list instead of leaving an allowance for a path nothing lives at.
func TestRemediationGuardFrozenPackagesExist(t *testing.T) {
	root := repoRootFromTest(t)
	for _, dir := range remediationFrozenDirs {
		info, err := os.Stat(filepath.Join(root, filepath.FromSlash(dir)))
		if err != nil || !info.IsDir() {
			t.Errorf("remediationFrozenDirs names %q, which is not a directory under the repository root (err: %v)", dir, err)
		}
	}
}

// TestOnlyTheRunnerCreatesCheckedOrVerified fails when a non-test file of
// agent-remediation outside service/runner calls remediation.NewChecked or
// remediation.NewVerified. A Checked or a Verified value is proof that the
// runner's checks or gate passed, so only the runner makes one.
func TestOnlyTheRunnerCreatesCheckedOrVerified(t *testing.T) {
	checked := walkRemediationGoFiles(t, remediationModule, func(rel string, fset *token.FileSet, f *ast.File) {
		if underRemediationDir(rel, remediationRunner) {
			return
		}
		uses, positions := remediationCheckedConstructorUsesIn(f)
		for i, use := range uses {
			t.Errorf("%s:%d: uses %s — only %s/ makes a Checked or a Verified, because each is the proof that a check or the gate passed",
				rel, fset.Position(positions[i]).Line, use, remediationRunner)
		}
	})
	if checked == 0 {
		t.Fatal("guard inspected no agent-remediation file — its discovery is broken")
	}
}

func parseRemediationGuardSource(t *testing.T, src string) *ast.File {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "x.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestRemediationStepImportGuardFlagsViolations pins the step import detector
// so it cannot rot into a guard that never fires.
func TestRemediationStepImportGuardFlagsViolations(t *testing.T) {
	f := parseRemediationGuardSource(t, `package x
import (
	"github.com/carolsimone/continuo/agent-remediation/service/steps/gate"
	shared "github.com/carolsimone/continuo/agent-remediation/service/steps/shared"
	"github.com/carolsimone/continuo/agent-remediation/service/stepsy"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)
`)
	want := []string{
		"github.com/carolsimone/continuo/agent-remediation/service/steps/gate",
		"github.com/carolsimone/continuo/agent-remediation/service/steps/shared",
	}
	for rel, flagged := range map[string]bool{
		"agent-remediation/service/runner/worker.go":           true,
		"agent-remediation/main.go":                            true,
		"agent-remediation/service/conformance/world.go":       true,
		"agent-remediation/service/runner/step/step.go":        true,
		"agent-remediation/service/runner/registry.go":         false,
		"agent-remediation/service/steps/gate/run.go":          false,
		"agent-remediation/service/steps/validation/prompt.go": false,
	} {
		got := remediationStepImportsIn(rel, f)
		if flagged && !reflect.DeepEqual(got, want) {
			t.Errorf("%s: flagged %v, want %v", rel, got, want)
		}
		if !flagged && len(got) != 0 {
			t.Errorf("%s: flagged %v, want none", rel, got)
		}
	}
}

// TestRemediationModelAndGateGuardFlagsViolations pins the detector of the model
// and the gate, and the directories it allows.
func TestRemediationModelAndGateGuardFlagsViolations(t *testing.T) {
	aliased := parseRemediationGuardSource(t, `package x
import rp "github.com/carolsimone/continuo/agent-remediation/service/ports"
var _ rp.LLMProvider
var _ rp.VerificationPipeline
var _ rp.Clock
`)
	if got, want := func() []string { u, _ := remediationModelAndGateUsesIn(aliased); return u }(),
		[]string{"rp.LLMProvider", "rp.VerificationPipeline"}; !reflect.DeepEqual(got, want) {
		t.Errorf("flagged %v under an import alias, want %v", got, want)
	}

	plain := parseRemediationGuardSource(t, `package x
import "github.com/carolsimone/continuo/agent-remediation/service/ports"
var _ ports.LLMProvider
var _ ports.VerificationPipeline
var _ ports.Clock
`)
	if got, want := func() []string { u, _ := remediationModelAndGateUsesIn(plain); return u }(),
		[]string{"ports.LLMProvider", "ports.VerificationPipeline"}; !reflect.DeepEqual(got, want) {
		t.Errorf("flagged %v, want %v", got, want)
	}

	shadow := parseRemediationGuardSource(t, `package x
func f(ports struct{ LLMProvider int }) int { return ports.LLMProvider }
`)
	if u, _ := remediationModelAndGateUsesIn(shadow); len(u) != 0 {
		t.Errorf("flagged %v in a file that does not import the ports package", u)
	}

	for rel, allowed := range map[string]bool{
		"agent-remediation/service/runner/runner.go":               true,
		"agent-remediation/service/runner/step/step.go":            true,
		"agent-remediation/service/steps/gate/verification_run.go": true,
		"agent-remediation/service/ports/llm_provider.go":          true,
		"agent-remediation/service/llmcache/caching_provider.go":   true,
		"agent-remediation/service/promptlog/promptlog.go":         true,
		"agent-remediation/service/conformance/world.go":           true,
		"agent-remediation/service/handlers/propose_fix.go":        true,
		"agent-remediation/service/fixer/fixer.go":                 true,
		"agent-remediation/service/verification/reconciler.go":     true,
		"agent-remediation/service/overlay/overlay.go":             false,
		"agent-remediation/service/proposals/proposals.go":         false,
		"agent-remediation/service/runnerx/x.go":                   false,
		"agent-remediation/service/newpkg/x.go":                    false,
	} {
		if got := remediationModelAndGateAllowed(rel); got != allowed {
			t.Errorf("%s: allowed = %v, want %v", rel, got, allowed)
		}
	}
}

// TestRemediationCheckedGuardFlagsViolations pins the detector of the Checked
// and Verified constructors.
func TestRemediationCheckedGuardFlagsViolations(t *testing.T) {
	f := parseRemediationGuardSource(t, `package x
import rem "github.com/carolsimone/continuo/agent-remediation/domain/remediation"
func f() {
	_, _ = rem.NewChecked(rem.Draft{}, nil)
	_, _ = rem.NewVerified("", nil, nil, nil)
	_ = rem.Draft{}
	_ = rem.KeyDraft
}
`)
	uses, _ := remediationCheckedConstructorUsesIn(f)
	if want := []string{"rem.NewChecked", "rem.NewVerified"}; !reflect.DeepEqual(uses, want) {
		t.Errorf("flagged %v, want %v", uses, want)
	}

	other := parseRemediationGuardSource(t, `package x
import "github.com/carolsimone/continuo/agent-remediation/domain/proposal"
func f() { _ = proposal.NewChecked }
`)
	if u, _ := remediationCheckedConstructorUsesIn(other); len(u) != 0 {
		t.Errorf("flagged %v, which is not the remediation package", u)
	}
}
