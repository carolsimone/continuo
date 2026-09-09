package event

import (
	"testing"
)

func TestRemediationEventIDVariesByAttempt(t *testing.T) {
	a1 := RemediationEventID("r1", 1)
	a1b := RemediationEventID("r1", 1)
	a2 := RemediationEventID("r1", 2)
	if a1 != a1b {
		t.Fatal("same (release,attempt) must be stable")
	}
	if a1 == a2 {
		t.Fatal("different attempt must differ")
	}
}

// TestRemediationEventID_KeyedOnReleaseAndAttempt verifies the id is a
// function of (releaseID, attempt) alone, with no node segment.
func TestRemediationEventID_KeyedOnReleaseAndAttempt(t *testing.T) {
	if RemediationEventID("r", 1) == RemediationEventID("r", 2) {
		t.Fatal("attempts must mint distinct ids")
	}
	first := RemediationEventID("r", 1)
	second := RemediationEventID("r", 1)
	if first != second {
		t.Fatal("must be stable")
	}
}

