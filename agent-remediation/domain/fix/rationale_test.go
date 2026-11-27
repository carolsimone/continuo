package fix_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
)

func TestComposeRationale_IndependentFixNamesFailureUpstreamEditAndNote(t *testing.T) {
	got := fix.ComposeRationale(fix.RationaleFacts{
		Members:       []fix.RationaleMember{{NodeID: "analytics.report", Project: "finance", ErrorLine: `column "amount" does not exist`}},
		Upstream:      []fix.RationaleUpstream{{NodeID: "analytics.orders", Project: "core", Depth: 1}},
		UpstreamKnown: true,
		Edits:         []string{"services/finance/models/report.sql"},
		TargetNodeID:  "analytics.report",
		ModelNote:     "read amount_eur instead of amount",
	})

	assert.Equal(t, "Failed: `analytics.report` (project finance): column \"amount\" does not exist\n"+
		"This release changed upstream: `analytics.orders` (project core, 1 hop up)\n"+
		"Edited: `services/finance/models/report.sql` (repairs `analytics.report`)\n"+
		"Model's note: read amount_eur instead of amount", got)
}

func TestComposeRationale_SaysWhenNothingUpstreamChanged(t *testing.T) {
	got := fix.ComposeRationale(fix.RationaleFacts{
		Members:       []fix.RationaleMember{{NodeID: "e2e_schema.ftable_e", Project: "service-2", ErrorLine: "relation public.wrong_name does not exist"}},
		UpstreamKnown: true,
		Edits:         []string{"services/service-2/models/ftable_e.sql"},
		TargetNodeID:  "e2e_schema.ftable_e",
		ModelNote:     "upstream renamed amount to amount_eur",
	})

	assert.Contains(t, got, "No upstream of `e2e_schema.ftable_e` changed in this release.\n")
	assert.Less(t, indexOf(got, "Failed:"), indexOf(got, "Model's note:"), "facts come before the model's note")
}

func TestComposeRationale_OmitsTheUpstreamLineWhenUnknown(t *testing.T) {
	got := fix.ComposeRationale(fix.RationaleFacts{
		Members:      []fix.RationaleMember{{NodeID: "svc-a"}},
		Edits:        []string{"services/svc-a/models/x.sql"},
		TargetNodeID: "svc-a",
		ModelNote:    "closed the config() call",
	})

	assert.NotContains(t, got, "changed in this release")
	assert.NotContains(t, got, "changed upstream")
	assert.Equal(t, "Failed: `svc-a`\nEdited: `services/svc-a/models/x.sql` (repairs `svc-a`)\nModel's note: closed the config() call", got)
}

func TestComposeRationale_CrossProjectNamesEveryConsumerAndItsFollowUp(t *testing.T) {
	got := fix.ComposeRationale(fix.RationaleFacts{
		Members: []fix.RationaleMember{
			{NodeID: "analytics.report", Project: "finance", ErrorLine: `column "amount" does not exist`},
			{NodeID: "analytics.spend", Project: "marketing", ErrorLine: `column "amount" does not exist`},
		},
		Upstream:      []fix.RationaleUpstream{{NodeID: "analytics.orders", Project: "core", Depth: 1}},
		UpstreamKnown: true,
		Edits:         []string{"services/core/models/orders.sql"},
		TargetNodeID:  "analytics.orders",
		TargetProject: "core",
		CrossProject:  true,
		ModelNote:     "kept amount alongside amount_eur",
	})

	assert.Contains(t, got, "Failed: `analytics.report` (project finance): column \"amount\" does not exist\n")
	assert.Contains(t, got, "Failed: `analytics.spend` (project marketing): column \"amount\" does not exist\n")
	assert.Contains(t, got, "`analytics.report` lives in project finance and cannot change in this release; this edit keeps the contract it reads. Moving it to the new shape is a follow-up for project finance.\n")
	assert.Contains(t, got, "`analytics.spend` lives in project marketing and cannot change in this release; this edit keeps the contract it reads. Moving it to the new shape is a follow-up for project marketing.\n")
	assert.Contains(t, got, "Edited: `services/core/models/orders.sql` (repairs `analytics.orders`)\n")
}

// TestComposeRationale_CrossProjectSkipsSameProjectMembers verifies P2b: a
// coalesced cross-project cluster can carry a member in the same project as
// the target (a same-signature member merged in by coalesceUpstream); the
// composer must not tell that member it "cannot change in this release" —
// only members whose project differs from the target's get the follow-up line.
func TestComposeRationale_CrossProjectSkipsSameProjectMembers(t *testing.T) {
	got := fix.ComposeRationale(fix.RationaleFacts{
		Members: []fix.RationaleMember{
			{NodeID: "analytics.local", Project: "core"},
			{NodeID: "analytics.report", Project: "finance"},
		},
		TargetNodeID:  "analytics.orders",
		TargetProject: "core",
		CrossProject:  true,
	})

	assert.Contains(t, got, "`analytics.report` lives in project finance and cannot change in this release; this edit keeps the contract it reads. Moving it to the new shape is a follow-up for project finance.")
	assert.NotContains(t, got, "`analytics.local` lives in project core and cannot change")
}

func TestComposeRationale_NoNoteEndsOnTheLastFact(t *testing.T) {
	got := fix.ComposeRationale(fix.RationaleFacts{
		Members: []fix.RationaleMember{{NodeID: "a"}}, Edits: []string{"p"}, TargetNodeID: "a",
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
