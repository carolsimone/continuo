package fixer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
)

func TestFor_KnownSources(t *testing.T) {
	for _, src := range []string{"compile", "seed_build", "validation"} {
		if _, err := For(src); err != nil {
			t.Fatalf("For(%q): unexpected error %v", src, err)
		}
	}
}

func TestFor_UnknownSource(t *testing.T) {
	if _, err := For("nonsense"); err == nil {
		t.Fatal("For(nonsense): want error, got nil")
	}
}

// TestWriteEditArtifacts_KeysAreEditScoped verifies that writeEditArtifacts
// keys each edit's content and diff by attempt AND edit index, so two edits
// written for the same attempt land under distinct keys instead of colliding
// on the single node-scoped key writeSourceArtifacts uses.
func TestWriteEditArtifacts_KeysAreEditScoped(t *testing.T) {
	w := &fakeArtifacts{}
	svc := Services{Artifacts: w, Logger: testLogger()}
	in := Input{ReleaseID: "r", NodeID: "n", Attempt: 2}

	edit, err := writeEditArtifacts(context.Background(), svc, in, 1, "contracts/a.yml", "old", "new")
	require.NoError(t, err)
	require.Contains(t, w.written, "proposed-fix/r/n/attempt-2/edit-1.content")
	require.Contains(t, w.written, "proposed-fix/r/n/attempt-2/edit-1.diff")
	require.Equal(t, "contracts/a.yml", edit.Path)
	require.Equal(t, "s3://bucket/proposed-fix/r/n/attempt-2/edit-1.content", edit.ContentURI)
	require.Equal(t, "s3://bucket/proposed-fix/r/n/attempt-2/edit-1.diff", edit.DiffURI)

	// A second edit in the same attempt must not collide with the first: the
	// old node-scoped key (attempt-2 alone, no edit index) would overwrite
	// edit-1's content here.
	_, err = writeEditArtifacts(context.Background(), svc, in, 2, "contracts/b.yml", "old2", "new2")
	require.NoError(t, err)
	require.Contains(t, w.written, "proposed-fix/r/n/attempt-2/edit-2.content")
	require.Equal(t, "new", w.written["proposed-fix/r/n/attempt-2/edit-1.content"], "edit-1's content must survive writing edit-2 of the same attempt")
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

// TestWriteEditArtifacts_DiffHeaderNamesTheEditedFile verifies that the diff
// written for an edit is labelled with that edit's repository-relative path,
// so a proposal touching several files produces one distinctly-headed diff per
// file rather than N diffs all naming the same thing.
func TestWriteEditArtifacts_DiffHeaderNamesTheEditedFile(t *testing.T) {
	w := &fakeArtifacts{}
	svc := Services{Artifacts: w, Logger: testLogger()}
	in := Input{ReleaseID: "r", NodeID: "svc.model.node_a", Attempt: 3}

	_, err := writeEditArtifacts(context.Background(), svc, in, 1, "contracts/a.yml", "old", "new")
	require.NoError(t, err)

	diff := w.written["proposed-fix/r/svc.model.node_a/attempt-3/edit-1.diff"]
	require.Contains(t, diff, "--- a/contracts/a.yml")
	require.Contains(t, diff, "+++ b/contracts/a.yml")
	require.NotContains(t, diff, "svc.model.node_a", "the diff header must name the edited file, not the node")
}
