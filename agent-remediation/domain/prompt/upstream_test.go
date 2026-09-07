package prompt

import (
	"strings"
	"testing"
)

func TestAssembleUpstreamFix_NamesTargetAndEveryDescendant(t *testing.T) {
	req := AssembleUpstreamFix(UpstreamEvidence{
		TargetNodeID: "s.u", TargetSource: "select id from s.base",
		OwnChangeDiff: "-select id, amount from s.base\n+select id from s.base",
		Members:       []MemberFailure{{NodeID: "s.v", ErrorExcerpt: "column u.amount does not exist"}, {NodeID: "s.w", ErrorExcerpt: "column u.amount does not exist"}},
	})
	if !strings.Contains(req.User, "Upstream node source") || !strings.Contains(req.User, "s.u") {
		t.Fatalf("prompt must show the target: %s", req.User)
	}
	if !strings.Contains(req.User, "s.v") || !strings.Contains(req.User, "s.w") || !strings.Contains(req.User, "column u.amount does not exist") {
		t.Fatalf("prompt must list every failing descendant with its error: %s", req.User)
	}
	if !strings.Contains(req.User, "What this release changed in s.u") {
		t.Fatalf("prompt must show the target's own change: %s", req.User)
	}
	if req.ToolName != "propose_fix" || len(req.ToolParams) != 3 || req.ToolParams[0].Name != "proposed_sql" {
		t.Fatalf("tool schema must be the propose_fix{proposed_sql,rationale,confidence} shape the adapters parse: %+v", req.ToolParams)
	}
}

func TestAssembleUpstreamFix_CrossServiceAddsTheKeepBothClauseAndMemberServices(t *testing.T) {
	req := AssembleUpstreamFix(UpstreamEvidence{
		TargetNodeID: "analytics.orders", TargetSource: "select id, amount_eur from raw", CrossService: true,
		Members: []MemberFailure{{NodeID: "analytics.report", Service: "finance", ErrorExcerpt: "column amount does not exist"}},
	})
	for _, want := range []string{
		"cannot change in this release",
		"keep both",
		"Do not revert the change",
		"- analytics.report (service finance): column amount does not exist",
	} {
		if !strings.Contains(req.System+req.User, want) {
			t.Fatalf("cross-service prompt must contain %q:\n%s\n%s", want, req.System, req.User)
		}
	}
}

func TestAssembleUpstreamFix_SameServiceHasNoCrossServiceClause(t *testing.T) {
	req := AssembleUpstreamFix(UpstreamEvidence{
		TargetNodeID: "s.u", TargetSource: "select id from s.base",
		Members: []MemberFailure{{NodeID: "s.v", ErrorExcerpt: "column u.amount does not exist"}},
	})
	if strings.Contains(req.System, "cannot change in this release") {
		t.Fatalf("same-service prompt must not carry the cross-service clause: %s", req.System)
	}
	if !strings.Contains(req.User, "- s.v: column u.amount does not exist") {
		t.Fatalf("a member without a service renders without one: %s", req.User)
	}
}
