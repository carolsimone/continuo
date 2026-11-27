package remediation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

func TestHandOvers_NamesEveryNodeWithACodeSortedByNodeID(t *testing.T) {
	got := HandOvers(map[string]fix.NodeOutcome{
		"model.c": {Status: fix.StatusSkipped, Reason: "no remediation type matches this failure", Code: model.HandoverReasonNoType},
		"model.b": {Status: fix.StatusProposed},
		"model.a": {Status: fix.StatusEscalated, Reason: "tried 3 times", Code: model.HandoverReasonTriesSpent},
	})

	assert.Equal(t, []HandOver{
		{NodeID: "model.a", Code: model.HandoverReasonTriesSpent, Message: "tried 3 times"},
		{NodeID: "model.c", Code: model.HandoverReasonNoType, Message: "no remediation type matches this failure"},
	}, got)
}

func TestHandOvers_NoCodeNoHandOver(t *testing.T) {
	assert.Empty(t, HandOvers(map[string]fix.NodeOutcome{"model.a": {Status: fix.StatusProposed}}))
	assert.Empty(t, HandOvers(nil))
}

// A message is what reaches the event and the release page, so it is cleaned:
// a credential and an email address in an outcome's reason are replaced by
// their fixed markers, whatever the code.
func TestHandOvers_MessagesAreCleaned(t *testing.T) {
	const dirty = "dial postgres://etl:hunter2@db/x refused, contact ops@acme.com" //nolint:gosec // G101: a made-up password the cleaner must remove.
	got := HandOvers(map[string]fix.NodeOutcome{
		"model.a": {Status: fix.StatusEscalated, Reason: "tried 3 times; the last try failed: password=hunter2", Code: model.HandoverReasonTriesSpent},
		"model.b": {Status: fix.StatusSkipped, Reason: dirty, Code: model.HandoverReasonClassifierInfra},
	})

	assert.Equal(t, []HandOver{
		{NodeID: "model.a", Code: model.HandoverReasonTriesSpent, Message: "tried 3 times; the last try failed: password=[redacted:secret]"},
		{NodeID: "model.b", Code: model.HandoverReasonClassifierInfra, Message: "dial postgres://etl:[redacted:secret]@db/x refused, contact [redacted:email]"},
	}, got)
}
