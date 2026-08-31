package prompt

import (
	"fmt"
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
	}, "Compilation Error ...", "svc", nil)
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
	req := AssembleSeedFix("seeds/ref.csv", "id,name\n1,\"a,b\"", "Error loading seed", "svc", nil)
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

func TestAssemble_RendersOwnChangeUpstreamAndPrecedents(t *testing.T) {
	req := Assemble(Evidence{
		NodeID: "analytics.orders", CandidateSQL: "select bad", DBTLog: "column x does not exist",
		OwnChangeDiff: "-select good\n+select bad",
		UpstreamChanges: []UpstreamChange{{NodeID: "analytics.payments", Depth: 1,
			CodeDiff: "-a\n+b", ConfigDiff: `-"materialized": "table"` + "\n" + `+"materialized": "incremental"`}},
		Precedents: precedentFixture(),
	})
	for _, want := range []string{
		"What this release changed in the failing model",
		"-select good\n+select bad",
		"Recent upstream changes",
		"analytics.payments",
		`+"materialized": "incremental"`, // config diffs are evidence now
		"How similar failures were fixed before",
	} {
		if !strings.Contains(req.User, want) {
			t.Fatalf("prompt missing %q\n%s", want, req.User)
		}
	}
}

func TestAssemble_OmitsEmptySections(t *testing.T) {
	req := Assemble(Evidence{NodeID: "analytics.orders", CandidateSQL: "select 1", DBTLog: "err"})
	for _, absent := range []string{"What this release changed", "Recent upstream changes", "How similar failures"} {
		if strings.Contains(req.User, absent) {
			t.Fatalf("empty evidence must render no %q section", absent)
		}
	}
}

// AssembleDuplicateTableFix must name the contested RELATION on its "Relation:"
// line, not the target model's own unique_id — the two differ whenever the
// target already carries an alias, and telling the model to stop producing
// its own unique_id (rather than the relation it actually writes) points it
// at the wrong name.
func TestAssembleDuplicateTableFix_NamesTheRelationNotTheTargetNodeID(t *testing.T) {
	req := AssembleDuplicateTableFix(
		NamedFile{Path: "models/orders_v1.sql", Content: "select 1 as id"},
		"analytics.orders",
		"marketing",
		"models/orders_v2.sql",
		nil,
	)
	if !strings.Contains(req.User, "Relation: analytics.orders") {
		t.Errorf("user content must name the contested relation on the Relation: line:\n%s", req.User)
	}
	if !strings.Contains(req.User, "marketing") || !strings.Contains(req.User, "models/orders_v2.sql") {
		t.Errorf("user content must name the competing producer's service and file path:\n%s", req.User)
	}
}

func precedentFixture() []Precedent {
	return []Precedent{
		{NodeID: "analytics.other", Stage: "validation", Category: "sql_error",
			Reason: "missing_column", ErrorExcerpt: "column x does not exist",
			RejectedAt: "2026-08-01T00:00:00Z", Resolved: true,
			ResolutionDiff: "-select a\n+select a, x", PRURL: "https://github.com/acme/r/pull/7"},
		{NodeID: "analytics.third", Stage: "validation", Category: "sql_error",
			Reason: "missing_column", ErrorExcerpt: "column y does not exist",
			RejectedAt: "2026-07-01T00:00:00Z", Resolved: false},
	}
}

func TestAssembleCompileFix_RendersPrecedents(t *testing.T) {
	req := AssembleCompileFix([]NamedFile{{Path: "models/a.sql", Content: "select 1"}},
		"compile err", "service-1", precedentFixture())
	for _, want := range []string{
		"How similar failures were fixed before",
		"-select a\n+select a, x",
		"https://github.com/acme/r/pull/7",
		"column y does not exist", // unresolved precedent appears as a one-line mention
	} {
		if !strings.Contains(req.User, want) {
			t.Fatalf("prompt missing %q\n%s", want, req.User)
		}
	}
}

func TestAssembleCompileFix_NoPrecedents_NoSection(t *testing.T) {
	req := AssembleCompileFix([]NamedFile{{Path: "models/a.sql", Content: "select 1"}},
		"compile err", "service-1", nil)
	if strings.Contains(req.User, "How similar failures were fixed before") {
		t.Fatal("empty precedents must render no section")
	}
}

func TestRenderPrecedents_DiffsCappedAtFive(t *testing.T) {
	ps := make([]Precedent, 8)
	for i := range ps {
		ps[i] = Precedent{NodeID: fmt.Sprintf("analytics.n%d", i), Resolved: true,
			ErrorExcerpt: "e", RejectedAt: "2026-08-01T00:00:00Z",
			ResolutionDiff: fmt.Sprintf("-old%d\n+new%d", i, i)}
	}
	var b strings.Builder
	renderPrecedents(&b, ps)
	out := b.String()
	if !strings.Contains(out, "+new4") || strings.Contains(out, "+new5") {
		t.Fatalf("want diffs for the first 5 only:\n%s", out)
	}
	if !strings.Contains(out, "analytics.n7") {
		t.Fatalf("beyond-cap precedents must still appear as one-line mentions:\n%s", out)
	}
}

// TestRenderPrecedents_UpstreamEdit verifies that a precedent resolved by
// editing a node other than the precedent's own node renders a line naming
// that upstream node and its path before the edited diff.
func TestRenderPrecedents_UpstreamEdit(t *testing.T) {
	ps := []Precedent{{
		NodeID: "analytics.orders", Category: "sql_error", Reason: "missing_column",
		ErrorExcerpt: "column x does not exist", RejectedAt: "2026-08-01T00:00:00Z",
		Resolved: true,
		Edited: []EditedPrecedent{
			{NodeID: "analytics.payments", Path: "models/payments.sql", Diff: "-old\n+new"},
		},
	}}
	var b strings.Builder
	renderPrecedents(&b, ps)
	out := b.String()
	if !strings.Contains(out, "resolved by editing upstream node analytics.payments (models/payments.sql):") {
		t.Fatalf("missing upstream-edit line:\n%s", out)
	}
	if !strings.Contains(out, "-old\n+new") {
		t.Fatalf("missing edited diff:\n%s", out)
	}
}

// TestRenderPrecedents_AmendedCaveat verifies that an Edited entry with
// Amended==true renders the human-amendment caveat before its diff.
func TestRenderPrecedents_AmendedCaveat(t *testing.T) {
	ps := []Precedent{{
		NodeID: "analytics.orders", Category: "sql_error", Reason: "missing_column",
		ErrorExcerpt: "column x does not exist", RejectedAt: "2026-08-01T00:00:00Z",
		Resolved: true,
		Edited: []EditedPrecedent{
			{NodeID: "analytics.payments", Path: "models/payments.sql", Amended: true, Diff: "-old\n+new"},
		},
	}}
	var b strings.Builder
	renderPrecedents(&b, ps)
	out := b.String()
	if !strings.Contains(out, "note: a human amended the proposed fix before merge; the diff below is what shipped.") {
		t.Fatalf("missing amended caveat:\n%s", out)
	}
	if strings.Index(out, "note: a human amended") > strings.Index(out, "-old\n+new") {
		t.Fatalf("caveat must precede the diff it describes:\n%s", out)
	}
}

// TestRenderPrecedents_OwnNodeEdit_NoUpstreamWording verifies that a
// precedent whose only Edited entry targets its own node renders exactly as
// it did before Edited existed: no "resolved by editing upstream node" line.
func TestRenderPrecedents_OwnNodeEdit_NoUpstreamWording(t *testing.T) {
	ps := []Precedent{{
		NodeID: "analytics.orders", Category: "sql_error", Reason: "missing_column",
		ErrorExcerpt: "column x does not exist", RejectedAt: "2026-08-01T00:00:00Z",
		Resolved: true, ResolutionDiff: "-select a\n+select a, x",
		Edited: []EditedPrecedent{
			{NodeID: "analytics.orders", Path: "models/orders.sql", Diff: "-select a\n+select a, x"},
		},
	}}
	var b strings.Builder
	renderPrecedents(&b, ps)
	out := b.String()
	if strings.Contains(out, "resolved by editing upstream node") {
		t.Fatalf("own-node edit must not render upstream wording:\n%s", out)
	}
	if !strings.Contains(out, "-select a\n+select a, x") {
		t.Fatalf("missing resolution diff:\n%s", out)
	}
}

// TestRenderPrecedents_EditedDiffAsResolutionWhenResolutionDiffEmpty verifies
// that a precedent with no ResolutionDiff but with Edited entries still
// renders in full (excerpt + diff), using the edited diff as the resolution,
// rather than falling back to the unresolved/resolved one-liner.
func TestRenderPrecedents_EditedDiffAsResolutionWhenResolutionDiffEmpty(t *testing.T) {
	ps := []Precedent{{
		NodeID: "analytics.orders", Category: "sql_error", Reason: "missing_column",
		ErrorExcerpt: "column x does not exist", RejectedAt: "2026-08-01T00:00:00Z",
		Resolved: true,
		Edited: []EditedPrecedent{
			{NodeID: "analytics.payments", Path: "models/payments.sql", Diff: "-old_upstream\n+new_upstream"},
		},
	}}
	var b strings.Builder
	renderPrecedents(&b, ps)
	out := b.String()
	if !strings.Contains(out, "-old_upstream\n+new_upstream") {
		t.Fatalf("edited diff must be rendered as the resolution when ResolutionDiff is empty:\n%s", out)
	}
	if strings.Contains(out, "analytics.orders (sql_error/missing_column, 2026-08-01T00:00:00Z, resolved): column x does not exist") {
		t.Fatalf("must not fall back to the one-liner when Edited supplies a resolution:\n%s", out)
	}
}

// TestRenderPrecedents_OwnNodeEditedDiffAsResolutionWhenResolutionDiffEmpty
// verifies that an own-node Edited entry (NodeID == the precedent's own
// node), not just an upstream one, stands in as the rendered resolution when
// ResolutionDiff is empty.
func TestRenderPrecedents_OwnNodeEditedDiffAsResolutionWhenResolutionDiffEmpty(t *testing.T) {
	ps := []Precedent{{
		NodeID: "analytics.orders", Category: "sql_error", Reason: "missing_column",
		ErrorExcerpt: "column x does not exist", RejectedAt: "2026-08-01T00:00:00Z",
		Resolved: true,
		Edited: []EditedPrecedent{
			{NodeID: "analytics.orders", Path: "models/orders.sql", Diff: "-select a\n+select a, x"},
		},
	}}
	var b strings.Builder
	renderPrecedents(&b, ps)
	out := b.String()
	if !strings.Contains(out, "-select a\n+select a, x") {
		t.Fatalf("own-node edited diff must be rendered as the resolution when ResolutionDiff is empty:\n%s", out)
	}
}

// TestRenderPrecedents_AmendedCaveat_CoexistsWithResolutionDiff verifies that
// an own-node Edited entry's Amended caveat still renders even when the
// precedent also carries a non-empty ResolutionDiff (an own-timeline
// NodeVersion resolution and a merged-PR edit to the same node are not
// mutually exclusive) — the caveat must not be silently dropped just because
// ResolutionDiff supplied the diff text.
func TestRenderPrecedents_AmendedCaveat_CoexistsWithResolutionDiff(t *testing.T) {
	ps := []Precedent{{
		NodeID: "analytics.orders", Category: "sql_error", Reason: "missing_column",
		ErrorExcerpt: "column x does not exist", RejectedAt: "2026-08-01T00:00:00Z",
		Resolved: true, ResolutionDiff: "-select a\n+select a, x",
		Edited: []EditedPrecedent{
			{NodeID: "analytics.orders", Path: "models/orders.sql", Amended: true, Diff: "-select a\n+select a, x"},
		},
	}}
	var b strings.Builder
	renderPrecedents(&b, ps)
	out := b.String()
	if !strings.Contains(out, "-select a\n+select a, x") {
		t.Fatalf("missing resolution diff:\n%s", out)
	}
	if !strings.Contains(out, "note: a human amended the proposed fix before merge; the diff below is what shipped.") {
		t.Fatalf("amended caveat must render even when ResolutionDiff supplied the diff text:\n%s", out)
	}
}

// TestRenderPrecedents_NoCaveat_OwnNodeNotAmended guards against
// over-rendering: an own-node Edited entry with Amended==false alongside a
// non-empty ResolutionDiff must render no caveat at all.
func TestRenderPrecedents_NoCaveat_OwnNodeNotAmended(t *testing.T) {
	ps := []Precedent{{
		NodeID: "analytics.orders", Category: "sql_error", Reason: "missing_column",
		ErrorExcerpt: "column x does not exist", RejectedAt: "2026-08-01T00:00:00Z",
		Resolved: true, ResolutionDiff: "-select a\n+select a, x",
		Edited: []EditedPrecedent{
			{NodeID: "analytics.orders", Path: "models/orders.sql", Amended: false, Diff: "-select a\n+select a, x"},
		},
	}}
	var b strings.Builder
	renderPrecedents(&b, ps)
	out := b.String()
	if strings.Contains(out, "note: a human amended") {
		t.Fatalf("must not render the amended caveat when the own-node edit is not amended:\n%s", out)
	}
}

func TestAssembleSeedFix_RendersPrecedents(t *testing.T) {
	req := AssembleSeedFix("seeds/ref.csv", "id,name\n1,a", "seed err", "service-1", precedentFixture())
	for _, want := range []string{"How similar failures were fixed before", "-select a\n+select a, x"} {
		if !strings.Contains(req.User, want) {
			t.Fatalf("prompt missing %q\n%s", want, req.User)
		}
	}
}

func TestAssembleDuplicateTableFix_RendersPrecedents(t *testing.T) {
	req := AssembleDuplicateTableFix(
		NamedFile{Path: "models/orders_v1.sql", Content: "select 1 as id"},
		"analytics.orders", "marketing", "models/orders_v2.sql", precedentFixture())
	for _, want := range []string{"How similar failures were fixed before", "-select a\n+select a, x"} {
		if !strings.Contains(req.User, want) {
			t.Fatalf("prompt missing %q\n%s", want, req.User)
		}
	}
}
