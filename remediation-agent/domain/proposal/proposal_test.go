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
