package proposal

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

const (
	dirtyReason = "dial postgres://etl:hunter2@db/x refused, contact ops@acme.com" //nolint:gosec // G101: a made-up password the cleaner must remove.
	cleanReason = "dial postgres://etl:[redacted:secret]@db/x refused, contact [redacted:email]"
)

// A coded outcome is a hand-over message: its reason is cleaned. An outcome with
// no code keeps its reason as it is.
func TestNodeOutcome_Cleaned(t *testing.T) {
	coded := NodeOutcome{Status: StatusEscalated, Reason: dirtyReason, Code: model.HandoverReasonTriesSpent}
	uncoded := NodeOutcome{Status: StatusFailed, Reason: dirtyReason}

	assert.Equal(t, NodeOutcome{Status: StatusEscalated, Reason: cleanReason, Code: model.HandoverReasonTriesSpent}, coded.Cleaned())
	assert.Equal(t, uncoded, uncoded.Cleaned())
	assert.Equal(t, coded.Cleaned(), coded.Cleaned().Cleaned(), "cleaning a cleaned outcome changes nothing")
}

// CleanedOutcomes is a copy: the map it is given, which its caller may still
// read, is not changed.
func TestCleanedOutcomes_ReturnsACopyAndLeavesItsArgumentAlone(t *testing.T) {
	given := map[string]NodeOutcome{
		"a": {Status: StatusSkipped, Reason: dirtyReason, Code: model.HandoverReasonClassifierInfra},
		"b": {Status: StatusProposed},
	}

	got := CleanedOutcomes(given)

	assert.Equal(t, map[string]NodeOutcome{
		"a": {Status: StatusSkipped, Reason: cleanReason, Code: model.HandoverReasonClassifierInfra},
		"b": {Status: StatusProposed},
	}, got)
	assert.Equal(t, dirtyReason, given["a"].Reason, "the argument is untouched")
	assert.Nil(t, CleanedOutcomes(nil))
	assert.Empty(t, CleanedOutcomes(map[string]NodeOutcome{}))
}
