package model

import "testing"

// Every failure reason is filed under a declared failure class, which
// execution-controller records with the reason in each failed attempt's facts.
func TestFailureReason_EveryReasonNamesADeclaredClass(t *testing.T) {
	for _, r := range FailureReasons() {
		if !r.Class().IsValid() {
			t.Errorf("failure reason %q names no declared failure class (got %q)", r, r.Class())
		}
	}
	if got := FailureReasonOOMKilled.Class(); got != FailureClassResource {
		t.Fatalf("oom_killed class = %q, want resource", got)
	}
	if got := FailureReason("unknown").Class(); got != "" {
		t.Fatalf("an undeclared reason has class %q, want none", got)
	}
}
