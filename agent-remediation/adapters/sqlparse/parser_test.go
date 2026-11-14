//go:build integration

package sqlparse

import (
	"context"
	"os"
	"os/exec"
	"testing"
)

// trinoOnlySQL parses in the trino dialect and not in the postgres one: a JSON
// path query with an array wrapper.
const trinoOnlySQL = "select json_query(payload, 'lax $.items' with array wrapper) as items from raw.events"

// TestSQLParser_PassesTheConfiguredDialect runs the real script under the
// image's interpreter and shows the dialect New was given is the one the parse
// runs in: the same SQL parses under one dialect and fails under the other.
func TestSQLParser_PassesTheConfiguredDialect(t *testing.T) {
	if _, err := exec.LookPath(interpreter()); err != nil {
		t.Skipf("%s not on PATH (%v): the sql_parses interpreter ships only inside the agent-remediation image, not on a bare host or CI runner. It is exercised in CI by the dedicated in-container step that runs this package's integration test inside the running agent-remediation container.", interpreter(), err)
	}
	if _, err := os.Stat(script()); err != nil {
		t.Skipf("%s missing (%v): the sql_parses script ships only inside the agent-remediation image, not on a bare host or CI runner. It is exercised in CI by the dedicated in-container step that runs this package's integration test inside the running agent-remediation container.", script(), err)
	}

	if out, err := exec.Command(interpreter(), "-I", "-c", "import sqlglot").CombinedOutput(); err != nil { //nolint:gosec // G204: the interpreter this package runs, fixed arguments.
		t.Skipf("%s cannot import sqlglot (%v: %s): the interpreter that carries sqlglot ships only inside the agent-remediation image. It is exercised in CI by the dedicated in-container step that runs this package's integration test inside the running agent-remediation container.", interpreter(), err, out)
	}

	ctx := context.Background()
	trino, err := New("trino")
	if err != nil {
		t.Fatalf("New(trino): %v", err)
	}
	postgres, err := New("postgres")
	if err != nil {
		t.Fatalf("New(postgres): %v", err)
	}

	got, err := trino.Parse(ctx, trinoOnlySQL)
	if err != nil {
		t.Fatalf("trino Parse: %v", err)
	}
	if len(got.Errors) != 0 || got.Skipped != "" {
		t.Errorf("trino: %+v, want the SQL to parse", got)
	}

	got, err = postgres.Parse(ctx, trinoOnlySQL)
	if err != nil {
		t.Fatalf("postgres Parse: %v", err)
	}
	if len(got.Errors) == 0 {
		t.Errorf("postgres: %+v, want a parse error for trino-only SQL", got)
	}
	for _, e := range got.Errors {
		if e.Message == "" || e.Line < 1 {
			t.Errorf("error %+v: want a message and a line from 1", e)
		}
	}

	got, err = postgres.Parse(ctx, "{% if x %}select 1{% endif %}")
	if err != nil {
		t.Fatalf("postgres Parse of a statement tag: %v", err)
	}
	if got.Skipped == "" || len(got.Errors) != 0 {
		t.Errorf("statement tag: %+v, want a skip and no error", got)
	}

	if _, err := New("no-such-dialect"); err == nil {
		t.Error("New accepted a dialect sqlglot does not know; it must refuse it at boot")
	}
}
