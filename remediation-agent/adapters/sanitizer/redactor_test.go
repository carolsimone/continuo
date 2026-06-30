package sanitizer

import (
	"strings"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// sampleConstraintLog is a representative Postgres/dbt unique-constraint
// violation as it appears in a failing dbt run log. It carries both the error
// structure the LLM needs (error class, relation, column, constraint, SQLSTATE,
// LINE marker) and warehouse data values that must never reach an external LLM
// (the offending email, the duplicate id).
const sampleConstraintLog = `01:23:45  Running with dbt=1.12.0b1
01:23:46  Database error while running model service_1.dim_customers
01:23:47  ERROR: duplicate key value violates unique constraint "dim_customers_email_key"
  DETAIL:  Key (email)=(john.doe@example.com) already exists.
  SQLSTATE: 23505
LINE 42:   SELECT email, customer_id FROM analytics.stg_customers
                  ^
01:23:48  compiled Code at target/run/service_1/models/dim_customers.sql`

// requireContains fails the test if haystack does not contain needle.
func requireContains(t *testing.T, haystack, needle, why string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Fatalf("expected output to retain %q (%s)\n--- output ---\n%s", needle, why, haystack)
	}
}

// requireAbsent fails the test if haystack still contains needle.
func requireAbsent(t *testing.T, haystack, needle, why string) {
	t.Helper()
	if strings.Contains(haystack, needle) {
		t.Fatalf("expected output to redact %q (%s)\n--- output ---\n%s", needle, why, haystack)
	}
}

func TestRedactor_ImplementsPort(t *testing.T) {
	var _ ports.LogSanitizer = NewRedactor(LevelRedacted)
}

func TestRedactor_ConstraintViolation_RedactsDataKeepsStructure(t *testing.T) {
	out := NewRedactor(LevelRedacted).Sanitize(sampleConstraintLog)

	// Sensitive warehouse data must be gone.
	requireAbsent(t, out, "john.doe@example.com", "email value leaks PII")
	requireAbsent(t, out, "(john.doe@example.com)", "constraint detail value leaks PII")

	// Error structure the LLM needs must survive.
	requireContains(t, out, "duplicate key value violates unique constraint", "error class")
	requireContains(t, out, "dim_customers_email_key", "constraint name is structural")
	requireContains(t, out, "Key (email)=", "constraint column is structural")
	requireContains(t, out, "23505", "SQLSTATE code is structural")
	requireContains(t, out, "LINE 42:", "LINE marker is structural")
	requireContains(t, out, "service_1.dim_customers", "node/relation id is structural")
	requireContains(t, out, "analytics.stg_customers", "relation reference is structural")
	requireContains(t, out, "<email>", "redacted email uses a stable placeholder")
}

func TestRedactor_QuotedLiterals_Redacted(t *testing.T) {
	in := `ERROR: invalid input syntax for type integer: "not-a-number"
  WHERE status = 'cancelled-by-customer-2024' AND note = 'sensitive free text'`
	out := NewRedactor(LevelRedacted).Sanitize(in)

	requireAbsent(t, out, "not-a-number", "double-quoted literal value")
	requireAbsent(t, out, "cancelled-by-customer-2024", "single-quoted literal value")
	requireAbsent(t, out, "sensitive free text", "single-quoted literal value")

	requireContains(t, out, "invalid input syntax for type integer", "error class")
	requireContains(t, out, "WHERE status =", "SQL keyword structure")
	requireContains(t, out, "<str>", "redacted literal uses a stable placeholder")
}

func TestRedactor_UUIDsAndNumbers_Redacted(t *testing.T) {
	in := `DETAIL: Key (order_id)=(550e8400-e29b-41d4-a716-446655440000) is not present in table "orders".
  Failing row contains (1234567890, 42.50, account 9988776655).`
	out := NewRedactor(LevelRedacted).Sanitize(in)

	requireAbsent(t, out, "550e8400-e29b-41d4-a716-446655440000", "uuid value")
	requireAbsent(t, out, "1234567890", "long numeric value")
	requireAbsent(t, out, "9988776655", "long numeric value")

	requireContains(t, out, "Key (order_id)=", "constraint column is structural")
	requireContains(t, out, `is not present in table "orders"`, "relation reference structural")
	requireContains(t, out, "<uuid>", "redacted uuid uses a stable placeholder")
	requireContains(t, out, "<num>", "redacted number uses a stable placeholder")
}

func TestRedactor_FailingRowDump_Redacted(t *testing.T) {
	in := `ERROR: new row for relation "fct_payments" violates check constraint "amount_positive"
  DETAIL: Failing row contains (a1b2, -50.00, john@corp.io, 2024-01-15, secret-token-xyz).`
	out := NewRedactor(LevelRedacted).Sanitize(in)

	requireAbsent(t, out, "john@corp.io", "row-dump email")
	requireAbsent(t, out, "secret-token-xyz", "row-dump token")

	requireContains(t, out, `violates check constraint "amount_positive"`, "error class + constraint")
	requireContains(t, out, `relation "fct_payments"`, "relation reference structural")
}

func TestRedactor_LevelRaw_PassesThrough(t *testing.T) {
	out := NewRedactor(LevelRaw).Sanitize(sampleConstraintLog)
	if out != sampleConstraintLog {
		t.Fatalf("raw level must pass the log through unchanged")
	}
}

func TestRedactor_LevelSummary_DropsBodyKeepsErrorLines(t *testing.T) {
	out := NewRedactor(LevelSummary).Sanitize(sampleConstraintLog)

	// Summary keeps error-bearing lines (redacted) and drops noise.
	requireContains(t, out, "duplicate key value violates unique constraint", "error line retained")
	requireAbsent(t, out, "john.doe@example.com", "data still redacted in summary")
	requireAbsent(t, out, "Running with dbt=", "non-error noise dropped")
}

func TestRedactor_EmptyLog(t *testing.T) {
	if got := NewRedactor(LevelRedacted).Sanitize(""); got != "" {
		t.Fatalf("empty log must stay empty, got %q", got)
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]Level{
		"":             LevelRedacted,
		"redacted":     LevelRedacted,
		"REDACTED":     LevelRedacted,
		"raw":          LevelRaw,
		"summary":      LevelSummary,
		"nonsense-val": LevelRedacted,
	}
	for in, want := range cases {
		if got := ParseLevel(in); got != want {
			t.Fatalf("ParseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}
