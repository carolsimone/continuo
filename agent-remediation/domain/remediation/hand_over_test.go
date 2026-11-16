package remediation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

func TestHandOvers_NamesEveryNodeWithACodeSortedByNodeID(t *testing.T) {
	got := HandOvers(map[string]proposal.NodeOutcome{
		"model.c": {Status: proposal.StatusSkipped, Reason: "no remediation type matches this failure", Code: model.HandoverReasonNoType},
		"model.b": {Status: proposal.StatusProposed},
		"model.a": {Status: proposal.StatusEscalated, Reason: "tried 3 times", Code: model.HandoverReasonTriesSpent},
	})

	assert.Equal(t, []HandOver{
		{NodeID: "model.a", Code: model.HandoverReasonTriesSpent, Message: "tried 3 times"},
		{NodeID: "model.c", Code: model.HandoverReasonNoType, Message: "no remediation type matches this failure"},
	}, got)
}

func TestHandOvers_NoCodeNoHandOver(t *testing.T) {
	assert.Empty(t, HandOvers(map[string]proposal.NodeOutcome{"model.a": {Status: proposal.StatusProposed}}))
	assert.Empty(t, HandOvers(nil))
}
