package proposal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
)

func TestComposeRationale_IndependentFixNamesFailureUpstreamEditAndNote(t *testing.T) {
	got := proposal.ComposeRationale(proposal.RationaleFacts{
		Members:       []proposal.RationaleMember{{NodeID: "analytics.report", Service: "finance", ErrorLine: `column "amount" does not exist`}},
		Upstream:      []proposal.RationaleUpstream{{NodeID: "analytics.orders", Service: "core", Depth: 1}},
		UpstreamKnown: true,
		Edits:         []string{"services/finance/models/report.sql"},
		TargetNodeID:  "analytics.report",
		ModelNote:     "read amount_eur instead of amount",
	})

	assert.Equal(t, "Failed: `analytics.report` (service finance): column \"amount\" does not exist\n"+
		"This release changed upstream: `analytics.orders` (service core, 1 hop up)\n"+
		"Edited: `services/finance/models/report.sql` (repairs `analytics.report`)\n"+
		"Model's note: read amount_eur instead of amount", got)
}

func TestComposeRationale_SaysWhenNothingUpstreamChanged(t *testing.T) {
	got := proposal.ComposeRationale(proposal.RationaleFacts{
		Members:       []proposal.RationaleMember{{NodeID: "e2e_schema.ftable_e", Service: "service-2", ErrorLine: "relation public.wrong_name does not exist"}},
		UpstreamKnown: true,
		Edits:         []string{"services/service-2/models/ftable_e.sql"},
		TargetNodeID:  "e2e_schema.ftable_e",
		ModelNote:     "upstream renamed amount to amount_eur",
	})

	assert.Contains(t, got, "No upstream of `e2e_schema.ftable_e` changed in this release.\n")
	assert.Less(t, indexOf(got, "Failed:"), indexOf(got, "Model's note:"), "facts come before the model's note")
}

func TestComposeRationale_OmitsTheUpstreamLineWhenUnknown(t *testing.T) {
	got := proposal.ComposeRationale(proposal.RationaleFacts{
		Members:      []proposal.RationaleMember{{NodeID: "svc-a"}},
		Edits:        []string{"services/svc-a/models/x.sql"},
		TargetNodeID: "svc-a",
		ModelNote:    "closed the config() call",
	})

	assert.NotContains(t, got, "changed in this release")
	assert.NotContains(t, got, "changed upstream")
	assert.Equal(t, "Failed: `svc-a`\nEdited: `services/svc-a/models/x.sql` (repairs `svc-a`)\nModel's note: closed the config() call", got)
}

func TestComposeRationale_CrossServiceNamesEveryConsumerAndItsFollowUp(t *testing.T) {
	got := proposal.ComposeRationale(proposal.RationaleFacts{
		Members: []proposal.RationaleMember{
			{NodeID: "analytics.report", Service: "finance", ErrorLine: `column "amount" does not exist`},
			{NodeID: "analytics.spend", Service: "marketing", ErrorLine: `column "amount" does not exist`},
		},
		Upstream:      []proposal.RationaleUpstream{{NodeID: "analytics.orders", Service: "core", Depth: 1}},
		UpstreamKnown: true,
		Edits:         []string{"services/core/models/orders.sql"},
		TargetNodeID:  "analytics.orders",
		CrossService:  true,
		ModelNote:     "kept amount alongside amount_eur",
	})

	assert.Contains(t, got, "Failed: `analytics.report` (service finance): column \"amount\" does not exist\n")
	assert.Contains(t, got, "Failed: `analytics.spend` (service marketing): column \"amount\" does not exist\n")
	assert.Contains(t, got, "`analytics.report` lives in service finance and cannot change in this release; this edit keeps the contract it reads. Moving it to the new shape is a follow-up for service finance.\n")
	assert.Contains(t, got, "`analytics.spend` lives in service marketing and cannot change in this release; this edit keeps the contract it reads. Moving it to the new shape is a follow-up for service marketing.\n")
	assert.Contains(t, got, "Edited: `services/core/models/orders.sql` (repairs `analytics.orders`)\n")
}

func TestComposeRationale_NoNoteEndsOnTheLastFact(t *testing.T) {
	got := proposal.ComposeRationale(proposal.RationaleFacts{
		Members: []proposal.RationaleMember{{NodeID: "a"}}, Edits: []string{"p"}, TargetNodeID: "a",
	})
	assert.Equal(t, "Failed: `a`\nEdited: `p` (repairs `a`)", got)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
