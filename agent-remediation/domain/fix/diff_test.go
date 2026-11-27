package fix

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestComputeRenameDiff(t *testing.T) {
	t.Run("a move with the same content is the rename header alone", func(t *testing.T) {
		got := ComputeRenameDiff("select 1\n", "select 1\n", "models/table_b.sql", "models/table_b_v2.sql")
		assert.Equal(t, "rename from models/table_b.sql\nrename to models/table_b_v2.sql\n", got)
	})
	t.Run("a move that changes a line carries its diff labelled old and new", func(t *testing.T) {
		got := ComputeRenameDiff("select 1\n", "select 2\n", "models/table_b.sql", "models/table_b_v2.sql")
		assert.Equal(t, "rename from models/table_b.sql\nrename to models/table_b_v2.sql\n"+
			"--- a/models/table_b.sql\n+++ b/models/table_b_v2.sql\n-select 1\n+select 2\n", got)
	})
}

func TestEditDiff(t *testing.T) {
	assert.Equal(t, ComputeUnifiedDiff("a\n", "b\n", "m.sql"), EditDiff("a\n", "b\n", "m.sql", ""))
	assert.Equal(t, ComputeRenameDiff("a\n", "a\n", "old.sql", "m.sql"), EditDiff("a\n", "a\n", "m.sql", "old.sql"),
		"a move with unchanged content still has a diff, so an earlier try that moved a file is shown")
}
