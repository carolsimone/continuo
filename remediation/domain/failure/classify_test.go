package failure

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestClassifyBuckets(t *testing.T) {
	cases := []struct {
		name      string
		logText   string
		wantCat   Category
		wantDec   Decision
		reasonHas string
	}{
		// --- hard-drop infra (the only DROP cases) ---
		{"conn refused", "could not connect to database: connection refused", CategoryInfraTransient, DecisionDrop, "infra"},
		{"oomkilled", "Container state: OOMKilled", CategoryInfraTransient, DecisionDrop, "infra"},
		{"imagepull", "Back-off pulling image: ImagePullBackOff", CategoryInfraTransient, DecisionDrop, "infra"},
		{"s3 creds", "AccessDenied: InvalidAccessKeyId when calling PutObject", CategoryInfraTransient, DecisionDrop, "infra"},
		// --- test failures ---
		{"dbt test", "Failure in test not_null_orders_id (got 14 results, configured to fail if != 0)", CategoryTest, DecisionEmit, "test"},
		// --- logic failures ---
		{"missing column", `Database Error in model orders: column "custmer_id" does not exist`, CategoryLogic, DecisionEmit, "logic"},
		{"compile ref", "Compilation Error: depends on a node named 'foo' which was not found", CategoryLogic, DecisionEmit, "logic"},
		{"syntax", `Database Error: syntax error at or near "SELCT"`, CategoryLogic, DecisionEmit, "logic"},
		// --- ambiguous → unknown, EMIT (under-drop) ---
		{"stmt timeout", "Database Error: canceling statement due to statement timeout", CategoryUnknown, DecisionEmit, "unknown"},
		{"perm denied", "Database Error: permission denied for schema analytics", CategoryUnknown, DecisionEmit, "unknown"},
		{"deadlock", "Database Error: deadlock detected", CategoryUnknown, DecisionEmit, "unknown"},
		{"oom warehouse", "Database Error: out of memory", CategoryUnknown, DecisionEmit, "unknown"},
		// --- unmatched → unknown, EMIT ---
		{"gibberish", "some entirely novel failure mode", CategoryUnknown, DecisionEmit, "unknown"},
	}
	ev := FailureEvidence{Source: SourceValidation, ReleaseID: "r1", NodeID: "n1"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(ev, tc.logText)
			if got.Category != tc.wantCat {
				t.Errorf("category = %q, want %q", got.Category, tc.wantCat)
			}
			if got.Decision != tc.wantDec {
				t.Errorf("decision = %q, want %q", got.Decision, tc.wantDec)
			}
			if got.Signature == "" {
				t.Error("signature must not be empty")
			}
			if !contains(got.Reason, tc.reasonHas) {
				t.Errorf("reason = %q, want substring %q", got.Reason, tc.reasonHas)
			}
		})
	}
}

func TestClassifyLogUnavailable(t *testing.T) {
	got := Classify(FailureEvidence{}, "")
	if got.Category != CategoryUnknown || got.Decision != DecisionEmit {
		t.Fatalf("empty log: got %+v", got)
	}
	if got.Reason != "unknown:log_unavailable" {
		t.Errorf("reason = %q, want unknown:log_unavailable", got.Reason)
	}
}

func TestClassifyDuplicateTable_IsLogicAndEmits(t *testing.T) {
	c := ClassifyDuplicateTable(FailureEvidence{
		Source:    SourceDuplicateTable,
		ReleaseID: "rel-1",
		NodeID:    "analytics.orders",
	})

	if c.Category != CategoryLogic {
		t.Errorf("category = %q, want %q", c.Category, CategoryLogic)
	}
	if c.Decision != DecisionEmit {
		t.Errorf("decision = %q, want %q", c.Decision, DecisionEmit)
	}
	if c.Reason != "logic:duplicate_table" {
		t.Errorf("reason = %q, want %q", c.Reason, "logic:duplicate_table")
	}
	if c.Signature == "" {
		t.Error("signature must not be empty")
	}
}

func TestClassifyDuplicateTable_SignatureIsStablePerNode(t *testing.T) {
	a := ClassifyDuplicateTable(FailureEvidence{ReleaseID: "rel-1", NodeID: "analytics.orders"})
	b := ClassifyDuplicateTable(FailureEvidence{ReleaseID: "rel-9", NodeID: "analytics.orders"})
	c := ClassifyDuplicateTable(FailureEvidence{ReleaseID: "rel-1", NodeID: "analytics.customers"})

	if a.Signature != b.Signature {
		t.Errorf("the same collision in two releases must be one signature: %q != %q", a.Signature, b.Signature)
	}
	if a.Signature == c.Signature {
		t.Errorf("different relations must be different signatures, both %q", a.Signature)
	}
}

// The target claimant Target() picks can flip between releases — it prefers
// whichever service the release actually changed — even though the physical
// collision is the same relation both times. The signature must key on
// RelationID, not NodeID, or a flipped target forks one recurring collision
// into two signatures.
func TestClassifyDuplicateTable_SignatureIsStablePerRelationAcrossTargetFlip(t *testing.T) {
	a := ClassifyDuplicateTable(FailureEvidence{
		ReleaseID: "rel-1", NodeID: "analytics.orders_v1", RelationID: "analytics.orders",
	})
	b := ClassifyDuplicateTable(FailureEvidence{
		ReleaseID: "rel-9", NodeID: "analytics.orders_v2", RelationID: "analytics.orders",
	})
	c := ClassifyDuplicateTable(FailureEvidence{
		ReleaseID: "rel-1", NodeID: "analytics.orders_v1", RelationID: "analytics.customers",
	})

	if a.Signature != b.Signature {
		t.Errorf("same relation, different target node id across releases must be one signature: %q != %q",
			a.Signature, b.Signature)
	}
	if a.Signature == c.Signature {
		t.Errorf("different relations must be different signatures, both %q", a.Signature)
	}
}

// A trigger from before RelationID existed carries it empty; the signature
// must still be produced (degrading to the old NodeID-keyed behavior) rather
// than folding every such trigger into one signature via an empty key.
func TestClassifyDuplicateTable_SignatureFallsBackToNodeIDWhenRelationIDEmpty(t *testing.T) {
	a := ClassifyDuplicateTable(FailureEvidence{ReleaseID: "rel-1", NodeID: "analytics.orders"})
	b := ClassifyDuplicateTable(FailureEvidence{ReleaseID: "rel-1", NodeID: "analytics.customers"})

	if a.Signature == b.Signature {
		t.Errorf("different node ids must still be different signatures when RelationID is unset, both %q", a.Signature)
	}
}

func TestClassify_PopulatesExcerptWithKeyErrorLine(t *testing.T) {
	log := "Running dbt\n\nDatabase Error in model revenue\n  column \"gross_amt\" does not exist\n"
	c := Classify(FailureEvidence{}, log)
	assert.Equal(t, "Database Error in model revenue", c.Excerpt)
}

func TestClassify_EmptyLogHasEmptyExcerpt(t *testing.T) {
	c := Classify(FailureEvidence{}, "   ")
	assert.Equal(t, "", c.Excerpt)
}

func TestClassifyWithStructured_ExcerptIsStructuredMessage(t *testing.T) {
	c := ClassifyWithStructured(FailureEvidence{},
		&StructuredResult{Status: "error", Message: "syntax error at or near SELECT"}, "ignored log")
	assert.Equal(t, "syntax error at or near SELECT", c.Excerpt)
}

func TestClassifyDuplicateTable_ExcerptNamesTheRelation(t *testing.T) {
	c := ClassifyDuplicateTable(FailureEvidence{RelationID: "analytics.orders"})
	assert.Equal(t, "multiple nodes produce the relation analytics.orders", c.Excerpt)
}

func TestClassify_ExcerptIsCappedOnARuneBoundary(t *testing.T) {
	long := "error: " + strings.Repeat("é", 4096) // 2 bytes per rune ⇒ over the 4 KiB cap
	c := Classify(FailureEvidence{}, long)
	assert.LessOrEqual(t, len(c.Excerpt), 4096)
	assert.True(t, utf8.ValidString(c.Excerpt))
}

func contains(s, sub string) bool {
	return sub == "" || (len(sub) <= len(s) && (indexOf(s, sub) >= 0))
}
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
