package sqlparse

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// stubInterpreter installs body as an executable shell script standing in for
// the interpreter, points SQL_PARSES_PYTHON at it and SQL_PARSES_SCRIPT at an
// existing file, and returns the script's path. The stub sees the arguments
// the real interpreter would, and the SQL on stdin.
func stubInterpreter(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	stub := filepath.Join(dir, "python-stub")
	// The stub must carry the executable bit for exec to run it; it lives in
	// a per-test temp directory the test framework removes.
	if err := os.WriteFile(stub, []byte("#!/bin/sh\n"+body), 0o700); err != nil { //nolint:gosec // G306: an executable fixture is required, scoped to a temp dir.
		t.Fatalf("write stub: %v", err)
	}
	scriptPath := filepath.Join(dir, "sql_parses.py")
	if err := os.WriteFile(scriptPath, []byte("# stand-in\n"), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}
	t.Setenv(interpreterEnv, stub)
	t.Setenv(scriptEnv, scriptPath)
	return scriptPath
}

const cleanResult = `printf '{"errors": [], "skipped": ""}\n'`

func TestNew_RefusesWhenTheInterpreterIsMissing(t *testing.T) {
	stubInterpreter(t, cleanResult)
	t.Setenv(interpreterEnv, "no-such-python-here")
	t.Setenv("PATH", t.TempDir())

	p, err := New("some-dialect")
	if err == nil || p != nil {
		t.Fatalf("New = %+v, %v; want a refusal with no parser", p, err)
	}
	if !errors.Is(err, exec.ErrNotFound) {
		t.Errorf("error = %v, want it to wrap exec.ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "no-such-python-here") || !strings.Contains(err.Error(), interpreterEnv) {
		t.Errorf("error = %v, want it to name the interpreter and %s", err, interpreterEnv)
	}
}

func TestNew_RefusesWhenTheScriptIsMissing(t *testing.T) {
	stubInterpreter(t, cleanResult)
	missing := filepath.Join(t.TempDir(), "absent.py")
	t.Setenv(scriptEnv, missing)

	p, err := New("some-dialect")
	if err == nil || p != nil {
		t.Fatalf("New = %+v, %v; want a refusal with no parser", p, err)
	}
	if !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), missing) {
		t.Errorf("error = %v, want it to wrap os.ErrNotExist and name %s", err, missing)
	}
}

func TestNew_RefusesAnEmptyDialect(t *testing.T) {
	stubInterpreter(t, cleanResult)
	if p, err := New(""); err == nil || p != nil {
		t.Fatalf("New(\"\") = %+v, %v; want a refusal", p, err)
	}
}

// An interpreter without sqlglot, or a dialect sqlglot does not know, fails
// the probe parse, and New refuses rather than failing every draft later.
func TestNew_RefusesWhenTheProbeParseFails(t *testing.T) {
	stubInterpreter(t, `echo "ModuleNotFoundError: No module named 'sqlglot'" >&2; exit 1`)
	p, err := New("some-dialect")
	if err == nil || p != nil {
		t.Fatalf("New = %+v, %v; want a refusal", p, err)
	}
	if !strings.Contains(err.Error(), "No module named 'sqlglot'") {
		t.Errorf("error = %v, want the interpreter's stderr in it", err)
	}
}

func TestNew_RefusesWhenTheProbeReportsAnError(t *testing.T) {
	stubInterpreter(t, `printf '{"errors": [{"message": "nope", "line": 1}], "skipped": ""}\n'`)
	if p, err := New("some-dialect"); err == nil || p != nil {
		t.Fatalf("New = %+v, %v; want a refusal when the probe does not parse", p, err)
	}
}

// The interpreter runs isolated, on the configured script, with the dialect New
// was given, and the SQL on stdin.
func TestParse_RunsTheScriptWithTheDialectAndTheSQLOnStdin(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args")
	stdinFile := filepath.Join(dir, "stdin")
	scriptPath := stubInterpreter(t, `echo "$@" > `+argsFile+`; cat > `+stdinFile+`; `+cleanResult)

	p, err := New("dialect-x")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := p.Parse(context.Background(), "select\n  2"); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	args, _ := os.ReadFile(argsFile)   //nolint:gosec // G304: a file inside the test's temp dir.
	stdin, _ := os.ReadFile(stdinFile) //nolint:gosec // G304: a file inside the test's temp dir.
	if got, want := strings.TrimSpace(string(args)), "-I "+scriptPath+" --dialect dialect-x"; got != want {
		t.Errorf("args = %q, want %q", got, want)
	}
	if string(stdin) != "select\n  2" {
		t.Errorf("stdin = %q, want the SQL", stdin)
	}
}

func TestParse_ReadsTheErrorsAndTheSkip(t *testing.T) {
	stubInterpreter(t, `read -r first; case "$(cat)" in
*broken*) printf '{"errors": [{"message": "Expected table name", "line": 2}, {"message": "Invalid expression", "line": 3}], "skipped": ""}\n' ;;
*tag*) printf '{"errors": [], "skipped": "jinja statements: only dbt can render them"}\n' ;;
*) printf '{"errors": [], "skipped": ""}\n' ;;
esac`)
	p, err := New("d")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, err := p.Parse(context.Background(), "x\nbroken")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(got.Errors) != 2 || got.Errors[0].Message != "Expected table name" || got.Errors[0].Line != 2 ||
		got.Errors[1].Message != "Invalid expression" || got.Errors[1].Line != 3 || got.Skipped != "" {
		t.Errorf("Parse = %+v, want both errors with their lines", got)
	}
	got, err = p.Parse(context.Background(), "x\ntag")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(got.Errors) != 0 || got.Skipped != "jinja statements: only dbt can render them" {
		t.Errorf("Parse = %+v, want the skip", got)
	}
}

// parserOver is a Parser whose interpreter is a stub running body, built
// without New's probe, so the stub answers only the parse under test.
func parserOver(t *testing.T, body string) *Parser {
	t.Helper()
	scriptPath := stubInterpreter(t, body)
	return &Parser{python: os.Getenv(interpreterEnv), script: scriptPath, dialect: "d"}
}

func TestParse_RefusesWhatIsNotTheResultLine(t *testing.T) {
	cases := map[string]string{
		"a non-zero exit":  `cat > /dev/null; echo boom >&2; exit 3`,
		"not JSON":         `cat > /dev/null; echo hello`,
		"a missing field":  `cat > /dev/null; printf '{"errors": []}\n'`,
		"an unknown field": `cat > /dev/null; printf '{"errors": [], "skipped": "", "x": 1}\n'`,
		"two result lines": `cat > /dev/null; ` + cleanResult + `; ` + cleanResult,
		"an error no line": `cat > /dev/null; printf '{"errors": [{"message": "m"}], "skipped": ""}\n'`,
		"no output at all": `cat > /dev/null`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			p := parserOver(t, body)
			if got, err := p.Parse(context.Background(), "select 1"); err == nil {
				t.Errorf("Parse = %+v, nil; want an error", got)
			}
		})
	}
}

func TestParse_ANonZeroExitCarriesTheStderr(t *testing.T) {
	p := parserOver(t, `cat > /dev/null; echo "Traceback: boom" >&2; exit 1`)
	_, err := p.Parse(context.Background(), "select 1")
	if err == nil || !strings.Contains(err.Error(), "Traceback: boom") {
		t.Fatalf("Parse error = %v, want the interpreter's stderr in it", err)
	}
}
