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
