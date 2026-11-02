package config_test

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// configImportPath is the package that reads credentials through Secret; the
// guard does not scan it.
const configImportPath = "github.com/carolsimone/continuo/pkg/config"

// credentialReadGuardSkips are the go.work module paths, relative to the
// repository root, the guard does not scan: pkg/config reads each credential
// through Secret itself, and tests/e2e is a test client that hands the
// compose stack's fixed credentials to the services under test.
var credentialReadGuardSkips = []string{"pkg/config", "tests/e2e"}

// TestCredentialsAreReadOnlyThroughSecrets fails on any read of a credential
// variable, or of its <NAME>_FILE form, that bypasses Secret: a call to
// os.Getenv, os.LookupEnv, pkgconfig.EnvOrDefault, Require or RequireInt whose
// first argument names one. A credential read that way never honours the
// <NAME>_FILE form, so a file-mounted credential silently reaches the service
// as an empty string. It scans every non-test Go file of every go.work module.
func TestCredentialsAreReadOnlyThroughSecrets(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()
	scanned := 0
	for _, mod := range goWorkModules(t, root) {
		err := filepath.WalkDir(filepath.Join(root, mod), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			rel = filepath.ToSlash(rel)
			if d.IsDir() {
				if skippedByCredentialGuard(rel, d.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if perr != nil {
				t.Fatalf("parse %s: %v", rel, perr)
			}
			scanned++
			for _, v := range credentialReads(fset, f, pkgconfig.CredentialVars) {
				t.Errorf("%s:%s; read it with (*Validator).RequireSecret or OptionalSecret", rel, v)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", mod, err)
		}
	}
	if scanned == 0 {
		t.Fatal("the guard scanned no Go file; the go.work discovery is broken")
	}
}

// The detector names each kind of bypassing read and leaves reads through
// Secret, reads of other variables and a credential name given as a default
// value alone.
func TestCredentialReadGuardFlagsAViolation(t *testing.T) {
	const src = `package sample

import (
	"os"
	cfg "github.com/carolsimone/continuo/pkg/config"
)

func load(v *cfg.Validator) {
	_ = os.Getenv("REDIS_PASSWORD")
	_, _ = os.LookupEnv("GITHUB_TOKEN")
	_ = cfg.EnvOrDefault("LLM_API_KEY", "")
	_ = v.Require("NEO4J_PASSWORD")
	_ = v.RequireInt("AWS_SECRET_ACCESS_KEY")
	_ = os.Getenv("POSTGRES_PASSWORD_FILE")

	_ = os.Getenv("REDIS_HOST")
	_ = v.Require("REDIS_ADDR")
	_ = v.RequireSecret("REDIS_PASSWORD")
	_ = v.OptionalSecretValue("LLM_API_KEY")
	_ = cfg.EnvOrDefault("JOB_S3_ACCESS_KEY_ID_KEY", "AWS_ACCESS_KEY_ID")
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "sample.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	got := credentialReads(fset, f, pkgconfig.CredentialVars)
	want := []string{
		`9: os.Getenv("REDIS_PASSWORD") reads a credential without Secret`,
		`10: os.LookupEnv("GITHUB_TOKEN") reads a credential without Secret`,
		`11: cfg.EnvOrDefault("LLM_API_KEY") reads a credential without Secret`,
		`12: Require("NEO4J_PASSWORD") reads a credential without Secret`,
		`13: RequireInt("AWS_SECRET_ACCESS_KEY") reads a credential without Secret`,
		`14: os.Getenv("POSTGRES_PASSWORD_FILE") reads a credential without Secret`,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("credentialReads =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// credentialReads returns one "<line>: <call> reads a credential without
// Secret" entry for each call in f that reads one of vars, or its _FILE form,
// through os.Getenv, os.LookupEnv, pkgconfig.EnvOrDefault, Require or
// RequireInt.
func credentialReads(fset *token.FileSet, f *ast.File, vars []string) []string {
	osName, configName := importNames(f)
	var found []string
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		var label string
		pkg, _ := sel.X.(*ast.Ident)
		switch {
		case pkg != nil && osName != "" && pkg.Name == osName && (sel.Sel.Name == "Getenv" || sel.Sel.Name == "LookupEnv"):
			label = pkg.Name + "." + sel.Sel.Name
		case pkg != nil && configName != "" && pkg.Name == configName && sel.Sel.Name == "EnvOrDefault":
			label = pkg.Name + "." + sel.Sel.Name
		case sel.Sel.Name == "Require" || sel.Sel.Name == "RequireInt":
			label = sel.Sel.Name
		default:
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		name, err := strconv.Unquote(lit.Value)
		if err != nil || !slices.Contains(vars, strings.TrimSuffix(name, "_FILE")) {
			return true
		}
		found = append(found, fmt.Sprintf("%d: %s(%q) reads a credential without Secret",
			fset.Position(call.Pos()).Line, label, name))
		return true
	})
	return found
}

// importNames returns the names f refers to the os and pkg/config packages
// by, or "" for a package f does not import.
func importNames(f *ast.File) (osName, configName string) {
	for _, imp := range f.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		local := filepath.Base(path)
		if imp.Name != nil {
			local = imp.Name.Name
		}
		switch path {
		case "os":
			osName = local
		case configImportPath:
			configName = local
		}
	}
	return osName, configName
}

// skippedByCredentialGuard reports whether the guard leaves out the directory
// rel (relative to the repository root, slash-separated) named name.
func skippedByCredentialGuard(rel, name string) bool {
	if name == "testdata" || name == "vendor" || name == "node_modules" || (strings.HasPrefix(name, ".") && name != ".") {
		return true
	}
	for _, skip := range credentialReadGuardSkips {
		if rel == skip || strings.HasPrefix(rel, skip+"/") {
			return true
		}
	}
	return false
}

// goWorkModules returns the module directories go.work uses, relative to root.
func goWorkModules(t *testing.T, root string) []string {
	t.Helper()
	f, err := os.Open(filepath.Clean(filepath.Join(root, "go.work")))
	if err != nil {
		t.Fatalf("open go.work: %v", err)
	}
	defer func() { _ = f.Close() }()
	var mods []string
	inUse := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "use (":
			inUse = true
		case inUse && line == ")":
			inUse = false
		case inUse && line != "" && !strings.HasPrefix(line, "//"):
			mods = append(mods, filepath.Clean(line))
		case strings.HasPrefix(line, "use ") && !strings.HasSuffix(line, "("):
			mods = append(mods, filepath.Clean(strings.TrimSpace(strings.TrimPrefix(line, "use "))))
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read go.work: %v", err)
	}
	if len(mods) == 0 {
		t.Fatal("go.work uses no module")
	}
	return mods
}

// repoRoot walks up from the test's directory to the one holding go.work.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		if filepath.Dir(dir) == dir {
			t.Fatalf("go.work not found above %s", wd)
		}
	}
}
