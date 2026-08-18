package postgres

import (
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/stretchr/testify/require"
)

// TestEditsOrLegacy_MalformedJSONDegradesToLegacyScalars verifies that a
// corrupt file_edits blob is treated the same as an empty array rather than
// erroring: the read path stays total by falling back to the legacy scalar
// columns for a single-file proposal.
func TestEditsOrLegacy_MalformedJSONDegradesToLegacyScalars(t *testing.T) {
	got := editsOrLegacy([]byte(`{not valid json`), "models/x.sql", "s3://b/x.sql", "s3://b/x.diff")
	require.Equal(t,
		[]proposal.FileEdit{{Path: "models/x.sql", ContentURI: "s3://b/x.sql", DiffURI: "s3://b/x.diff"}},
		got,
		"malformed JSON must degrade to the legacy scalar synthesis, not error",
	)
}

// TestEditsOrLegacy_MalformedJSONNoLegacyFilePathReturnsEmpty verifies that a
// corrupt blob with no legacy file_path to fall back to (a row that never had
// a single-file proposal) yields an empty, non-nil-panicking result rather
// than a synthesized edit built from empty strings.
func TestEditsOrLegacy_MalformedJSONNoLegacyFilePathReturnsEmpty(t *testing.T) {
	got := editsOrLegacy([]byte(`{not valid json`), "", "", "")
	require.Empty(t, got, "malformed JSON with no legacy file_path must not synthesize an empty edit")
}

// TestEditsOrLegacy_EmptyArrayWithLegacyFilePathSynthesizes verifies the
// documented pre-migration row shape: an empty file_edits array (the column
// default) combined with a populated legacy file_path synthesizes one edit
// from the legacy scalar columns.
func TestEditsOrLegacy_EmptyArrayWithLegacyFilePathSynthesizes(t *testing.T) {
	got := editsOrLegacy([]byte(`[]`), "models/x.sql", "s3://b/x.sql", "s3://b/x.diff")
	require.Equal(t,
		[]proposal.FileEdit{{Path: "models/x.sql", ContentURI: "s3://b/x.sql", DiffURI: "s3://b/x.diff"}},
		got,
	)
}

// TestEditsOrLegacy_NonEmptyArrayIsUsedAsIs verifies that a valid, non-empty
// file_edits array decodes and is returned unchanged, ignoring the legacy
// scalar columns entirely.
func TestEditsOrLegacy_NonEmptyArrayIsUsedAsIs(t *testing.T) {
	raw := []byte(`[{"path":"a.sql","content_uri":"s3://b/1","diff_uri":"s3://b/1.diff"}]`)
	got := editsOrLegacy(raw, "legacy/should/be/ignored.sql", "s3://ignored", "s3://ignored.diff")
	require.Equal(t,
		[]proposal.FileEdit{{Path: "a.sql", ContentURI: "s3://b/1", DiffURI: "s3://b/1.diff"}},
		got,
	)
}
