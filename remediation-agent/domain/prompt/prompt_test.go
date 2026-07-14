package prompt

import (
	"strings"
	"testing"
)

func TestAssembleSourceFix_EmbedsSourceAndDiagnosis(t *testing.T) {
	req := AssembleSourceFix(
		"select * from {{ ref('table_b') }} join {{ ref('table_c') }} using (id) join public.silly_error using (id)",
		"analytics.table_e",
		"Remove the invalid join to public.silly_error which does not exist.",
	)
	if req.ToolName != "propose_fix" {
		t.Fatalf("tool name = %q, want propose_fix", req.ToolName)
	}
	if !strings.Contains(req.User, "silly_error") || !strings.Contains(req.User, "ref('table_b')") {
		t.Errorf("user content missing original source:\n%s", req.User)
	}
	if !strings.Contains(req.User, "Remove the invalid join") {
		t.Errorf("user content missing diagnosis:\n%s", req.User)
	}
	if !strings.Contains(req.System, "complete corrected") {
		t.Errorf("system prompt must require the complete corrected source:\n%s", req.System)
	}
	hasProposedSQL := false
	for _, p := range req.ToolParams {
		if p.Name == "proposed_sql" {
			hasProposedSQL = true
		}
	}
	if !hasProposedSQL {
		t.Errorf("tool must keep proposed_sql param so the adapters parse it unchanged")
	}
}

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

func TestAssembleCompileFix_ToolSchemaAndFiles(t *testing.T) {
	req := AssembleCompileFix([]NamedFile{
		{Path: "models/x.sql", Content: "select 1"},
		{Path: "models/schema.yml", Content: "version: 2"},
	}, "Compilation Error ...", "svc")
	names := map[string]bool{}
	for _, p := range req.ToolParams {
		names[p.Name] = true
	}
	for _, want := range []string{"target_file", "proposed_content", "rationale", "confidence"} {
		if !names[want] {
			t.Fatalf("missing tool param %q", want)
		}
	}
	if !strings.Contains(req.User, "models/schema.yml") || !strings.Contains(req.User, "models/x.sql") {
		t.Fatal("both gathered files must appear in the prompt")
	}
}

func TestAssembleSeedFix_CSVVocabularyAndSchema(t *testing.T) {
	req := AssembleSeedFix("seeds/ref.csv", "id,name\n1,\"a,b\"", "Error loading seed", "svc")
	names := map[string]bool{}
	for _, p := range req.ToolParams {
		names[p.Name] = true
	}
	for _, want := range []string{"proposed_content", "confidence", "rationale"} {
		if !names[want] {
			t.Fatalf("missing tool param %q", want)
		}
	}
	if !strings.Contains(strings.ToLower(req.System), "csv") {
		t.Fatal("seed prompt must use CSV vocabulary, not SQL")
	}
}

// TestPrompts_ForbidJinjaRefs locks in the cross-project rule: the independent
// dbt projects cannot resolve {{ ref(...) }} / {{ source(...) }} across each
// other, so both prompts must require physical schema.table references and
// forbid introducing ref()/source().
func TestPrompts_ForbidJinjaRefs(t *testing.T) {
	cases := map[string]string{
		"step 1 (Assemble)":          Assemble(Evidence{NodeID: "analytics.table_g"}).System,
		"step 2 (AssembleSourceFix)": AssembleSourceFix("select 1", "analytics.table_g", "fix it").System,
	}
	for name, sys := range cases {
		if !strings.Contains(sys, "physical schema.table name") {
			t.Errorf("%s: prompt must require physical schema.table names:\n%s", name, sys)
		}
		if !strings.Contains(sys, "{{ ref(...) }}") || !strings.Contains(sys, "{{ source(...) }}") {
			t.Errorf("%s: prompt must explicitly forbid {{ ref(...) }} / {{ source(...) }}:\n%s", name, sys)
		}
	}
}

func TestAssemble_IncludesUpstreamDiffs(t *testing.T) {
	req := Assemble(Evidence{
		NodeID:       "analytics.table_e",
		CandidateSQL: "select 1",
		DBTLog:       "boom",
		Ancestors:    []Ancestor{{NodeID: "analytics.table_c", ServiceName: "service-2", Depth: 1}},
		UpstreamDiffs: []UpstreamDiff{
			{NodeID: "analytics.table_c", ServiceName: "service-2", Diff: "@@ -1 +1 @@\n-old_col\n+new_col"},
		},
	})
	if !strings.Contains(req.User, "analytics.table_c") {
		t.Errorf("diff section must label the upstream node:\n%s", req.User)
	}
	if !strings.Contains(req.User, "new_col") || !strings.Contains(req.User, "```diff") {
		t.Errorf("diff section must render the patch in a diff block:\n%s", req.User)
	}
}

func TestAssemble_OmitsDiffSectionWhenNoDiffs(t *testing.T) {
	req := Assemble(Evidence{
		NodeID:    "analytics.table_e",
		Ancestors: []Ancestor{{NodeID: "analytics.table_c", ServiceName: "service-2", Depth: 1}},
	})
	if strings.Contains(req.User, "```diff") {
		t.Errorf("no diff block should appear when UpstreamDiffs is empty:\n%s", req.User)
	}
}
