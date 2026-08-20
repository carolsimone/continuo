package fixer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
)

func TestFor_KnownSources(t *testing.T) {
	for _, src := range []string{"compile", "seed_build", "validation"} {
		if _, err := For(src, ""); err != nil {
			t.Fatalf("For(%q): unexpected error %v", src, err)
		}
	}
}

func TestFor_UnknownSource(t *testing.T) {
	if _, err := For("nonsense", ""); err == nil {
		t.Fatal("For(nonsense): want error, got nil")
	}
}

// TestWriteSourceArtifacts_DiffHeaderNamesTheEditedFile verifies that the
// diff written for the single-shot fixers' source edit is labelled with the
// repository path being edited, not the node id, so the stored diff can
// actually be applied against the file it claims to change.
func TestWriteSourceArtifacts_DiffHeaderNamesTheEditedFile(t *testing.T) {
	w := &fakeArtifacts{}
	svc := Services{Artifacts: w, Logger: testLogger()}
	in := Input{ReleaseID: "r", NodeID: "svc.model.node_a", Attempt: 3}

	_, err := writeSourceArtifacts(context.Background(), svc, in, "services/service-3/models/node_a.sql", "old", "new")
	require.NoError(t, err)

	diff := w.written["proposed-fix/r/svc.model.node_a/attempt-3.source.diff"]
	require.Contains(t, diff, "--- a/services/service-3/models/node_a.sql")
	require.Contains(t, diff, "+++ b/services/service-3/models/node_a.sql")
	require.NotContains(t, diff, "svc.model.node_a", "the diff header must name the edited path, not the node id")
}

func TestNormalizeConfidence(t *testing.T) {
	cases := map[string]proposal.Confidence{
		"low": proposal.ConfidenceLow, "high": proposal.ConfidenceHigh,
		"medium": proposal.ConfidenceMedium, "": proposal.ConfidenceMedium,
	}
	for in, want := range cases {
		if got := normalizeConfidence(in); got != want {
			t.Fatalf("normalizeConfidence(%q)=%v want %v", in, got, want)
		}
	}
}
