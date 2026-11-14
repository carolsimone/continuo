// Package sqlparse implements ports.SQLParser by running the sql_parses.py
// script, which parses SQL with sqlglot, under a Python interpreter of the
// agent-remediation image.
package sqlparse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

const (
	// interpreterEnv names the Python interpreter that runs the script; it
	// must be one that can import sqlglot.
	interpreterEnv     = "SQL_PARSES_PYTHON"
	defaultInterpreter = "python3"
	// scriptEnv names the path of sql_parses.py.
	scriptEnv     = "SQL_PARSES_SCRIPT"
	defaultScript = "/usr/local/lib/continuo/sql_parses.py"

	// probeSQL is parsed once by New: SQL every dialect reads.
	probeSQL = "select 1"
	// probeTimeout bounds the probe parse, which starts the interpreter and
	// imports sqlglot.
	probeTimeout = time.Minute
)

// Parser runs `<interpreter> -I <script> --dialect <dialect>` with the SQL on
// stdin, and reads the one JSON result line the script prints.
type Parser struct {
	python  string
	script  string
	dialect string
}

var _ ports.SQLParser = (*Parser)(nil)

func interpreter() string { return envOr(interpreterEnv, defaultInterpreter) }

func script() string { return envOr(scriptEnv, defaultScript) }

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// New resolves the interpreter (SQL_PARSES_PYTHON, python3 when unset) and the
// script (SQL_PARSES_SCRIPT, /usr/local/lib/continuo/sql_parses.py when unset)
// and parses one probe statement in dialect. It fails when the interpreter or
// the script is missing, when the dialect is empty, and when the probe does not
// parse cleanly, which is how an interpreter without sqlglot or a dialect
// sqlglot does not know shows. A mis-built image so stops at boot, naming the
// cause once, instead of failing every draft's check.
func New(dialect string) (*Parser, error) {
	if dialect == "" {
		return nil, errors.New("sql parser: no dialect given")
	}
	python, err := exec.LookPath(interpreter())
	if err != nil {
		return nil, fmt.Errorf("resolve the sql_parses interpreter %q (%s): %w", interpreter(), interpreterEnv, err)
	}
	scriptPath := script()
	if _, err := os.Stat(scriptPath); err != nil {
		return nil, fmt.Errorf("find the sql_parses script (%s): %w", scriptEnv, err)
	}
	p := &Parser{python: python, script: scriptPath, dialect: dialect}

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	res, err := p.Parse(ctx, probeSQL)
	if err != nil {
		return nil, fmt.Errorf("probe parse in dialect %q: %w", dialect, err)
	}
	if len(res.Errors) > 0 || res.Skipped != "" {
		return nil, fmt.Errorf("probe parse in dialect %q: %q did not parse: %+v", dialect, probeSQL, res)
	}
	return p, nil
}

// resultLine is the script's stdout: {"errors": [{"message", "line"}], "skipped"}.
// Every field is a pointer so a missing one is told apart from an empty one.
type resultLine struct {
	Errors  *[]errorLine `json:"errors"`
	Skipped *string      `json:"skipped"`
}

type errorLine struct {
	Message *string `json:"message"`
	Line    *int    `json:"line"`
}

// Parse runs the script on sql. A non-zero exit, or a stdout that is not
// exactly one result line with every field, is an error carrying the
// interpreter's stderr.
func (p *Parser) Parse(ctx context.Context, sql string) (ports.SQLParseResult, error) {
	// G204: exec.CommandContext runs the resolved interpreter directly (no
	// shell) on a fixed flag set; the dialect is a literal argv entry.
	cmd := exec.CommandContext(ctx, p.python, "-I", p.script, "--dialect", p.dialect) //nolint:gosec // G204: direct execve, no shell, fixed flags.
	cmd.Stdin = strings.NewReader(sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return ports.SQLParseResult{}, fmt.Errorf("run sql_parses: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	res, err := decode(stdout.Bytes())
	if err != nil {
		return ports.SQLParseResult{}, fmt.Errorf("read sql_parses output %q: %w (stderr: %s)",
			stdout.String(), err, strings.TrimSpace(stderr.String()))
	}
	return res, nil
}

func decode(out []byte) (ports.SQLParseResult, error) {
	dec := json.NewDecoder(bytes.NewReader(out))
	dec.DisallowUnknownFields()
	var line resultLine
	if err := dec.Decode(&line); err != nil {
		return ports.SQLParseResult{}, err
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return ports.SQLParseResult{}, errors.New("more than one result line")
	}
	if line.Errors == nil || line.Skipped == nil {
		return ports.SQLParseResult{}, errors.New(`the result line needs both "errors" and "skipped"`)
	}
	res := ports.SQLParseResult{Skipped: *line.Skipped}
	for _, e := range *line.Errors {
		if e.Message == nil || e.Line == nil {
			return ports.SQLParseResult{}, errors.New(`every error needs "message" and "line"`)
		}
		res.Errors = append(res.Errors, ports.SQLParseError{Message: *e.Message, Line: *e.Line})
	}
	return res, nil
}
