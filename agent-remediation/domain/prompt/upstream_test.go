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
