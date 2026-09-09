package event

import (
	"testing"
)

func TestRemediationEventID_KeyedOnReleaseAndRound(t *testing.T) {
	if RemediationEventID("r1", 1) != RemediationEventID("r1", 0) {
		t.Fatal("round 0 and round 1 must mint the same id")
	}
	if RemediationEventID("r1", 1) == RemediationEventID("r1", 2) {
		t.Fatal("a later round must mint a distinct id")
	}
	if RemediationEventID("r1", 1) == RemediationEventID("r2", 1) {
		t.Fatal("different releases must mint distinct ids")
	}
}
