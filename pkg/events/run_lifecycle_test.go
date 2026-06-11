package events

import "testing"

func TestDispatchFailedReason_WireValues(t *testing.T) {
	if got := string(DispatchFailedReasonTargetNotFound); got != "target_not_found" {
		t.Fatalf("DispatchFailedReasonTargetNotFound = %q, want %q", got, "target_not_found")
	}
	if got := string(DispatchFailedReasonEmptyProjection); got != "empty_projection" {
		t.Fatalf("DispatchFailedReasonEmptyProjection = %q, want %q", got, "empty_projection")
	}
	if got := string(DispatchFailedReasonInvalidNodeType); got != "invalid_node_type" {
		t.Fatalf("DispatchFailedReasonInvalidNodeType = %q, want %q", got, "invalid_node_type")
	}
}
