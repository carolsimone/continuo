package proposal

import (
	"strings"
	"testing"
)

func TestComputeUnifiedDiff_ChangedLine(t *testing.T) {
	candidate := "select a, custmer_id from orders\n"
	proposed := "select a, customer_id from orders\n"
	d := ComputeUnifiedDiff(candidate, proposed, "models/orders.sql")
	if !strings.Contains(d, "-select a, custmer_id from orders") {
		t.Errorf("diff missing removed line:\n%s", d)
	}
	if !strings.Contains(d, "+select a, customer_id from orders") {
		t.Errorf("diff missing added line:\n%s", d)
	}
	if !strings.Contains(d, "models/orders.sql") {
		t.Errorf("diff missing path header:\n%s", d)
	}
}

func TestComputeUnifiedDiff_Identical(t *testing.T) {
	if d := ComputeUnifiedDiff("x\n", "x\n", "f.sql"); strings.Contains(d, "\n+") || strings.Contains(d, "\n-") {
		t.Errorf("identical inputs should produce no +/- lines:\n%s", d)
	}
}
