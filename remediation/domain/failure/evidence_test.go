package failure

import "testing"

func TestFailureEvidenceFilePath(t *testing.T) {
	ev := FailureEvidence{FilePath: "models/x.sql"}
	if ev.FilePath != "models/x.sql" {
		t.Errorf("FilePath = %q, want %q", ev.FilePath, "models/x.sql")
	}
}
