package failure

import "testing"

func TestNormalizeSignatureStableAcrossReleases(t *testing.T) {
	// Same underlying error, two different candidate schemas (release ids).
	errA := `Database Error in model orders (models/orders.sql)
  column "custmer_id" does not exist
  compiled Code at target/run/proj/models/orders.sql
  schema "candidate_a1b2c3d4"`
	errB := `Database Error in model orders (models/orders.sql)
  column "custmer_id" does not exist
  compiled Code at target/run/proj/models/orders.sql
  schema "candidate_99887766"`

	sigA := NormalizeSignature(CategoryLogic, errA)
	sigB := NormalizeSignature(CategoryLogic, errB)
	if sigA != sigB {
		t.Fatalf("signatures differ across releases:\n A=%s\n B=%s", sigA, sigB)
	}
	if sigA == "" {
		t.Fatal("signature must not be empty")
	}
}

func TestNormalizeSignatureRowCountsCanonicalized(t *testing.T) {
	a := NormalizeSignature(CategoryTest, "Failure in test not_null_orders (got 14 results, configured to fail if != 0)")
	b := NormalizeSignature(CategoryTest, "Failure in test not_null_orders (got 9 results, configured to fail if != 0)")
	if a != b {
		t.Fatalf("row counts not canonicalized: a=%s b=%s", a, b)
	}
}

func TestNormalizeSignatureCategorySensitive(t *testing.T) {
	line := "something failed"
	if NormalizeSignature(CategoryLogic, line) == NormalizeSignature(CategoryTest, line) {
		t.Fatal("signature must incorporate category")
	}
}

// Two sibling models broken by one upstream change must sign identically, or
// the shared-upstream grouping in agent-remediation can never cluster them and
// the cause is repaired once per victim instead of once at the source.
//
// These are the verbatim messages an e2e release produced for ftable_v and
// ftable_w — two models with byte-identical bodies, both reading a column their
// shared ancestor had dropped. They differ only inside the statement Postgres
// echoes back after "LINE 1:", which names the relation being created.
func TestNormalizeSignatureIgnoresEchoedStatementContext(t *testing.T) {
	lineV := "column u.amount does not exist\n" +
		"LINE 1: ..._e2e_rem_up_b9c41909\".\"ftable_v\" AS (SELECT u.id, u.amount F...\n" +
		"                                                             ^"
	lineW := "column u.amount does not exist\n" +
		"LINE 1: ..._e2e_rem_up_b9c41909\".\"ftable_w\" AS (SELECT u.id, u.amount F...\n" +
		"                                                             ^"

	sigV := NormalizeSignature(CategoryLogic, lineV)
	sigW := NormalizeSignature(CategoryLogic, lineW)
	if sigV != sigW {
		t.Fatalf("two nodes failing on the same dropped column must share one signature:\n ftable_v=%s\n ftable_w=%s", sigV, sigW)
	}
	if sigV == "" {
		t.Fatal("signature must not be empty")
	}
}

// Cutting the echoed statement must not blur genuinely different failures into
// one signature: everything the database says before the line marker is the
// diagnosis, and it is what the signature has to keep distinguishing.
func TestNormalizeSignatureStillSeparatesDifferentErrors(t *testing.T) {
	missingColumn := "column u.amount does not exist\nLINE 1: ...\"ftable_v\" AS (SELECT u.id, u.amount F...\n     ^"
	missingRelation := "relation \"e2e_schema.wrong_name\" does not exist\nLINE 1: ...\"ftable_v\" AS (SELECT u.id, u.amount F...\n     ^"

	if NormalizeSignature(CategoryLogic, missingColumn) == NormalizeSignature(CategoryLogic, missingRelation) {
		t.Fatal("a missing column and a missing relation must not collapse onto one signature")
	}
}

// The diagnosis often names the offending token before the line marker
// ("syntax error at or near \"x\""). That part is the error, not echoed
// context, so it must survive and keep two different offenders apart.
func TestNormalizeSignatureKeepsDiagnosticBeforeTheLineMarker(t *testing.T) {
	nearSelect := "syntax error at or near \"select\"\nLINE 1: ...\"ftable_v\" AS (SELECT u.id F...\n     ^"
	nearFrom := "syntax error at or near \"from\"\nLINE 1: ...\"ftable_v\" AS (SELECT u.id F...\n     ^"

	if NormalizeSignature(CategoryLogic, nearSelect) == NormalizeSignature(CategoryLogic, nearFrom) {
		t.Fatal(`the token named in "at or near" precedes the line marker and must stay in the signature`)
	}
}
