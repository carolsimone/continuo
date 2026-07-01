package failure

import "testing"

func TestSourceConstValues(t *testing.T) {
	if SourceValidation != "validation" {
		t.Errorf("SourceValidation = %q, want %q", SourceValidation, "validation")
	}
	if SourceCompile != "compile" {
		t.Errorf("SourceCompile = %q, want %q", SourceCompile, "compile")
	}
	if SourceSeed != "seed_build" {
		t.Errorf("SourceSeed = %q, want %q", SourceSeed, "seed_build")
	}
}

func TestFailureEvidenceFilePath(t *testing.T) {
	ev := FailureEvidence{FilePath: "models/x.sql"}
	if ev.FilePath != "models/x.sql" {
		t.Errorf("FilePath = %q, want %q", ev.FilePath, "models/x.sql")
	}
}

func TestCategoryHealable(t *testing.T) {
	cases := map[Category]bool{
		CategoryLogic:          true,
		CategoryTest:           true,
		CategoryUnknown:        true,
		CategoryInfraTransient: false,
	}
	for cat, want := range cases {
		if got := cat.Healable(); got != want {
			t.Errorf("Category(%q).Healable() = %v, want %v", cat, got, want)
		}
	}
}
