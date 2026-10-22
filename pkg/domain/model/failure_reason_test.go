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

// The reasons execution-controller reports for an attempt it could not run as
// dispatched: two infra failures (an acknowledged attempt it lost, a Job
// Kubernetes refused on every try) and three the dispatch itself made
// impossible.
func TestFailureReason_DispatchSideReasonsAndTheirClasses(t *testing.T) {
	for reason, class := range map[FailureReason]FailureClass{
		FailureReasonAttemptLost:       FailureClassInfra,
		FailureReasonCreateFailed:      FailureClassInfra,
		FailureReasonInvalidNodeType:   FailureClassInvalid,
		FailureReasonMalformedDispatch: FailureClassInvalid,
		FailureReasonJobNameCollision:  FailureClassInvalid,
	} {
		if !reason.IsValid() {
			t.Errorf("%s is not a declared failure reason", reason)
		}
		if got := reason.Class(); got != class {
			t.Errorf("%s class = %q, want %q", reason, got, class)
		}
	}
}
