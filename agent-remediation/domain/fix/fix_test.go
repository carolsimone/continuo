package fix

import "testing"

// TestNormalizeSingleFileView_NonEmptyEditsOverwritesScalars verifies that
// when Edits is non-empty, NormalizeSingleFileView makes the scalar fields
// agree with Edits[0], even when they started out pointing somewhere else.
func TestNormalizeSingleFileView_NonEmptyEditsOverwritesScalars(t *testing.T) {
	p := Fix{
		FilePath:       "stale/path.sql",
		ProposedSQLURI: "s3://stale/content",
		DiffURI:        "s3://stale/diff",
		Edits: []FileEdit{
			{Path: "services/service-3/models/orders_d.sql", ContentURI: "s3://real/content", DiffURI: "s3://real/diff"},
			{Path: "services/service-3/models/other.sql", ContentURI: "s3://other/content", DiffURI: "s3://other/diff"},
		},
	}

	p.NormalizeSingleFileView()

	if p.FilePath != "services/service-3/models/orders_d.sql" {
		t.Fatalf("FilePath = %q, want edits[0].Path", p.FilePath)
	}
	if p.ProposedSQLURI != "s3://real/content" {
		t.Fatalf("ProposedSQLURI = %q, want edits[0].ContentURI", p.ProposedSQLURI)
	}
	if p.DiffURI != "s3://real/diff" {
		t.Fatalf("DiffURI = %q, want edits[0].DiffURI", p.DiffURI)
	}
}

// TestNormalizeSingleFileView_EmptyEditsLeavesScalarsAlone verifies that when
// Edits is empty, NormalizeSingleFileView leaves the scalar fields untouched
// — the validation candidate-only case, where the scalars point at the
// candidate artifact and are the only record of the fix.
func TestNormalizeSingleFileView_EmptyEditsLeavesScalarsAlone(t *testing.T) {
	p := Fix{
		FilePath:       "",
		ProposedSQLURI: "s3://candidate/content",
		DiffURI:        "s3://candidate/diff",
	}

	p.NormalizeSingleFileView()

	if p.FilePath != "" {
		t.Fatalf("FilePath = %q, want unchanged empty string", p.FilePath)
	}
	if p.ProposedSQLURI != "s3://candidate/content" {
		t.Fatalf("ProposedSQLURI = %q, want unchanged candidate URI", p.ProposedSQLURI)
	}
	if p.DiffURI != "s3://candidate/diff" {
		t.Fatalf("DiffURI = %q, want unchanged candidate URI", p.DiffURI)
	}
}

// TestNormalizeSingleFileView_EmptyFirstEditPathLeavesScalarsAlone verifies
// that a non-empty Edits list whose first entry carries an empty Path does
// not blank an otherwise-correct FilePath: nothing validates a FileEdit
// before it reaches this method, so overwriting from a blank Path would make
// the fix worse, not more consistent.
func TestNormalizeSingleFileView_EmptyFirstEditPathLeavesScalarsAlone(t *testing.T) {
	p := Fix{
		FilePath:       "services/service-3/models/orders_d.sql",
		ProposedSQLURI: "s3://real/content",
		DiffURI:        "s3://real/diff",
		Edits: []FileEdit{
			{Path: "", ContentURI: "s3://real/content", DiffURI: "s3://real/diff"},
		},
	}

	p.NormalizeSingleFileView()

	if p.FilePath != "services/service-3/models/orders_d.sql" {
		t.Fatalf("FilePath = %q, want unchanged (edits[0].Path is empty)", p.FilePath)
	}
	if p.ProposedSQLURI != "s3://real/content" {
		t.Fatalf("ProposedSQLURI = %q, want unchanged", p.ProposedSQLURI)
	}
	if p.DiffURI != "s3://real/diff" {
		t.Fatalf("DiffURI = %q, want unchanged", p.DiffURI)
	}
}

// TestNormalizeRepresentativeViews_FillsNodeIDAndReleaseCheckIDFromBatchFields
// verifies that NormalizeRepresentativeViews sorts ResolvedNodeIDs, derives
// NodeID and ReleaseCheckID from the batch fields when empty, and follows
// edits[0] for the single-file view.
func TestNormalizeRepresentativeViews_FillsNodeIDAndReleaseCheckIDFromBatchFields(t *testing.T) {
	p := Fix{
		ResolvedNodeIDs: []string{"s.b", "s.a"},
		ReleaseChecks:   []ReleaseCheck{{Project: "svc", Kind: "dbt", CheckID: "verify-r-svc-a1"}},
		Edits:           []FileEdit{{Path: "services/svc/models/u.sql", ContentURI: "s3://c", DiffURI: "s3://d", TargetNodeID: "s.u"}},
	}
	p.NormalizeRepresentativeViews()
	if p.NodeID != "s.a" {
		t.Fatalf("representative node must be the smallest resolved id, got %q", p.NodeID)
	}
	if p.ReleaseCheckID != "verify-r-svc-a1" {
		t.Fatalf("release check id view must be the first release check, got %q", p.ReleaseCheckID)
	}
	if p.FilePath != "services/svc/models/u.sql" || p.ProposedSQLURI != "s3://c" {
		t.Fatalf("single-file view must follow edits[0]: %+v", p)
	}
	if p.ResolvedNodeIDs[0] != "s.a" {
		t.Fatal("resolved ids must be sorted")
	}
}

// TestUnionProjects_SortsDedupesAndDropsEmpty verifies that UnionProjects
// merges several project-name sets into one sorted, de-duplicated slice, with
// empty names dropped rather than merged in as a project.
func TestUnionProjects_SortsDedupesAndDropsEmpty(t *testing.T) {
	got := UnionProjects([]string{"ops", ""}, []string{"core"}, nil, []string{"core", "ops"})
	want := []string{"core", "ops"}
	if len(got) != len(want) {
		t.Fatalf("UnionProjects = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("UnionProjects = %v, want %v", got, want)
		}
	}
}
