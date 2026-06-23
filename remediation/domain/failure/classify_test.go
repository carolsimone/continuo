package failure

import "testing"

func TestClassifyBuckets(t *testing.T) {
	cases := []struct {
		name     string
		logText  string
		wantCat  Category
		wantDec  Decision
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
