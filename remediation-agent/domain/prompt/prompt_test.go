package prompt

import (
	"strings"
	"testing"
)

func TestAssemble_IncludesEvidenceAndForcesTool(t *testing.T) {
	req := Assemble(Evidence{
		NodeID:       "e2e_schema.ftable_e",
		CandidateSQL: "select c.id from e2e_schema.ftable_c c left join public.wrong_name w on c.id=w.id",
		DBTLog:       "Database Error: relation \"public.wrong_name\" does not exist",
		Ancestors:    []Ancestor{{NodeID: "e2e_schema.ftable_c", ServiceName: "service-2", Depth: 1}},
	})
	if req.ToolName != "propose_fix" {
		t.Fatalf("tool name = %q, want propose_fix", req.ToolName)
	}
	for _, want := range []string{"proposed_sql", "rationale", "confidence"} {
		found := false
		for _, p := range req.ToolParams {
			if p.Name == want {
				found = true
			}
		}
		if !found {
			t.Errorf("tool params missing %q", want)
		}
	}
	if !strings.Contains(req.User, "ftable_e") || !strings.Contains(req.User, "does not exist") {
		t.Errorf("user content missing node/error context:\n%s", req.User)
	}
	if !strings.Contains(req.System, "without weakening tests") {
		t.Errorf("system prompt must instruct not to weaken tests:\n%s", req.System)
	}
}
