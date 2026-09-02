package proposal

import "testing"

// TestNormalizeSingleFileView_NonEmptyEditsOverwritesScalars verifies that
// when Edits is non-empty, NormalizeSingleFileView makes the scalar fields
// agree with Edits[0], even when they started out pointing somewhere else.
func TestNormalizeSingleFileView_NonEmptyEditsOverwritesScalars(t *testing.T) {
	p := Proposal{
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
	p := Proposal{
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
// the proposal worse, not more consistent.
func TestNormalizeSingleFileView_EmptyFirstEditPathLeavesScalarsAlone(t *testing.T) {
	p := Proposal{
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

// TestNormalizeRepresentativeViews_FillsNodeIDAndVerificationRunIDFromBatchFields
// verifies that NormalizeRepresentativeViews sorts ResolvedNodeIDs, derives
// NodeID and VerificationRunID from the batch fields when empty, and follows
// edits[0] for the single-file view.
func TestNormalizeRepresentativeViews_FillsNodeIDAndVerificationRunIDFromBatchFields(t *testing.T) {
	p := Proposal{
		ResolvedNodeIDs: []string{"s.b", "s.a"},
		Verifications:   []Verification{{Service: "svc", Kind: "dbt", RunID: "verify-r-svc-a1"}},
		Edits:           []FileEdit{{Path: "services/svc/models/u.sql", ContentURI: "s3://c", DiffURI: "s3://d", TargetNodeID: "s.u"}},
	}
	p.NormalizeRepresentativeViews()
	if p.NodeID != "s.a" {
		t.Fatalf("representative node must be the smallest resolved id, got %q", p.NodeID)
	}
	if p.VerificationRunID != "verify-r-svc-a1" {
		t.Fatalf("verification run id view must be the first verification, got %q", p.VerificationRunID)
	}
	if p.FilePath != "services/svc/models/u.sql" || p.ProposedSQLURI != "s3://c" {
		t.Fatalf("single-file view must follow edits[0]: %+v", p)
	}
	if p.ResolvedNodeIDs[0] != "s.a" {
		t.Fatal("resolved ids must be sorted")
	}
}

// TestUnionServices_SortsDedupesAndDropsEmpty verifies that UnionServices
// merges several service-name sets into one sorted, de-duplicated slice, with
// empty names dropped rather than merged in as a service.
func TestUnionServices_SortsDedupesAndDropsEmpty(t *testing.T) {
	got := UnionServices([]string{"ops", ""}, []string{"core"}, nil, []string{"core", "ops"})
	want := []string{"core", "ops"}
	if len(got) != len(want) {
		t.Fatalf("UnionServices = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("UnionServices = %v, want %v", got, want)
		}
	}
}
