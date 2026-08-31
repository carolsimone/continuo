package proposals

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// amendSources is a fake ports.SourceReader keyed on ref+"\x00"+path. A path
// with no entry (and no injected error) returns ErrSourceNotFound, mirroring the
// GitHub adapter's 404 behaviour.
type amendSources struct {
	files map[string]string
	errs  map[string]error
}

func (s amendSources) ReadFile(_ context.Context, _, ref, path string) (string, error) {
	key := ref + "\x00" + path
	if err, ok := s.errs[key]; ok {
		return "", err
	}
	c, ok := s.files[key]
	if !ok {
		return "", ports.ErrSourceNotFound
	}
	return c, nil
}

func (s amendSources) ListDir(context.Context, string, string, string) ([]string, error) {
	return nil, nil
}

// amendEvidence is a fake ports.EvidenceReader keyed on URI.
type amendEvidence struct {
	objs map[string]string
	errs map[string]error
}

func (e amendEvidence) Fetch(_ context.Context, uri string) (string, error) {
	if err, ok := e.errs[uri]; ok {
		return "", err
	}
	c, ok := e.objs[uri]
	if !ok {
		return "", ports.ErrNotFound
	}
	return c, nil
}

const (
	amendRepo  = "acme/r"
	amendMerge = "mergesha"
)

func edit(path, content, diff, node string) proposal.FileEdit {
	return proposal.FileEdit{Path: path, ContentURI: content, DiffURI: diff, TargetNodeID: node}
}

// TestNormalizeContent verifies the byte-compare canon: CRLF folds to LF and
// trailing newlines are stripped, but interior content is untouched.
func TestNormalizeContent(t *testing.T) {
	require.Equal(t, "a\nb", normalizeContent("a\r\nb\r\n\n"))
	require.Equal(t, "a\nb", normalizeContent("a\nb"))
	require.Equal(t, "", normalizeContent("\n\n"))
	require.Equal(t, "a b", normalizeContent("a b"))
}

// TestResolveClosedEdits_IdenticalNotAmended: merged content equal to the
// proposal is not an amendment.
func TestResolveClosedEdits_IdenticalNotAmended(t *testing.T) {
	e := edit("m.sql", "s3://b/content", "s3://b/diff", "model.a")
	sources := amendSources{files: map[string]string{amendMerge + "\x00m.sql": "SELECT 1\n"}}
	evidence := amendEvidence{objs: map[string]string{
		"s3://b/content": "SELECT 1\n",
		"s3://b/diff":    "diff-text",
	}}

	out, err := resolveClosedEdits(context.Background(), sources, evidence, amendRepo, amendMerge, []proposal.FileEdit{e})

	require.NoError(t, err)
	require.Len(t, out, 1)
	require.False(t, out[0].Amended)
	require.Equal(t, "m.sql", out[0].Path)
	require.Equal(t, "model.a", out[0].TargetNodeID)
	require.Equal(t, "diff-text", out[0].Diff)
}

// TestResolveClosedEdits_SemanticDiffAmended: a real content change is an
// amendment.
func TestResolveClosedEdits_SemanticDiffAmended(t *testing.T) {
	e := edit("m.sql", "s3://b/content", "s3://b/diff", "model.a")
	sources := amendSources{files: map[string]string{amendMerge + "\x00m.sql": "SELECT 2\n"}}
	evidence := amendEvidence{objs: map[string]string{
		"s3://b/content": "SELECT 1\n",
		"s3://b/diff":    "diff-text",
	}}

	out, err := resolveClosedEdits(context.Background(), sources, evidence, amendRepo, amendMerge, []proposal.FileEdit{e})

	require.NoError(t, err)
	require.Len(t, out, 1)
	require.True(t, out[0].Amended)
}

// TestResolveClosedEdits_LineEndingAndTrailingNewlineNotAmended: a merge that
// only rewrote line endings and added trailing newlines is not an amendment.
func TestResolveClosedEdits_LineEndingAndTrailingNewlineNotAmended(t *testing.T) {
	e := edit("m.sql", "s3://b/content", "s3://b/diff", "model.a")
	sources := amendSources{files: map[string]string{amendMerge + "\x00m.sql": "SELECT 1\r\nSELECT 2\r\n\n\n"}}
	evidence := amendEvidence{objs: map[string]string{
		"s3://b/content": "SELECT 1\nSELECT 2",
		"s3://b/diff":    "diff-text",
	}}

	out, err := resolveClosedEdits(context.Background(), sources, evidence, amendRepo, amendMerge, []proposal.FileEdit{e})

	require.NoError(t, err)
	require.Len(t, out, 1)
	require.False(t, out[0].Amended, "CRLF and trailing-newline-only differences must not read as an amendment")
}

// TestResolveClosedEdits_MergedFileGoneIsAmended: a file absent at the merge
// commit is an amendment, not an error — the merged tree does not carry what
// the agent proposed. The proposed content is never fetched in this case.
func TestResolveClosedEdits_MergedFileGoneIsAmended(t *testing.T) {
	e := edit("gone.sql", "s3://b/content", "s3://b/diff", "model.a")
	sources := amendSources{} // no file -> ErrSourceNotFound
	evidence := amendEvidence{objs: map[string]string{"s3://b/diff": "diff-text"}}

	out, err := resolveClosedEdits(context.Background(), sources, evidence, amendRepo, amendMerge, []proposal.FileEdit{e})

	require.NoError(t, err)
	require.Len(t, out, 1)
	require.True(t, out[0].Amended)
	require.Equal(t, "diff-text", out[0].Diff)
}

// TestResolveClosedEdits_TransientGitHubErrorAborts: a non-404 read error
// aborts the whole resolution so the caller records nothing this tick.
func TestResolveClosedEdits_TransientGitHubErrorAborts(t *testing.T) {
	e := edit("m.sql", "s3://b/content", "s3://b/diff", "model.a")
	sources := amendSources{errs: map[string]error{amendMerge + "\x00m.sql": errors.New("502 bad gateway")}}
	evidence := amendEvidence{objs: map[string]string{
		"s3://b/content": "SELECT 1\n",
		"s3://b/diff":    "diff-text",
	}}

	out, err := resolveClosedEdits(context.Background(), sources, evidence, amendRepo, amendMerge, []proposal.FileEdit{e})

	require.Error(t, err)
	require.Nil(t, out)
}

// TestResolveClosedEdits_TransientS3ContentErrorAborts: an S3 error fetching the
// proposed content aborts the whole resolution.
func TestResolveClosedEdits_TransientS3ContentErrorAborts(t *testing.T) {
	e := edit("m.sql", "s3://b/content", "s3://b/diff", "model.a")
	sources := amendSources{files: map[string]string{amendMerge + "\x00m.sql": "SELECT 1\n"}}
	evidence := amendEvidence{
		objs: map[string]string{"s3://b/diff": "diff-text"},
		errs: map[string]error{"s3://b/content": errors.New("s3 timeout")},
	}

	out, err := resolveClosedEdits(context.Background(), sources, evidence, amendRepo, amendMerge, []proposal.FileEdit{e})

	require.Error(t, err)
	require.Nil(t, out)
}

// TestResolveClosedEdits_TransientS3DiffErrorAborts: an S3 error fetching the
// diff aborts the whole resolution too.
func TestResolveClosedEdits_TransientS3DiffErrorAborts(t *testing.T) {
	e := edit("m.sql", "s3://b/content", "s3://b/diff", "model.a")
	sources := amendSources{files: map[string]string{amendMerge + "\x00m.sql": "SELECT 1\n"}}
	evidence := amendEvidence{
		objs: map[string]string{"s3://b/content": "SELECT 1\n"},
		errs: map[string]error{"s3://b/diff": errors.New("s3 timeout")},
	}

	out, err := resolveClosedEdits(context.Background(), sources, evidence, amendRepo, amendMerge, []proposal.FileEdit{e})

	require.Error(t, err)
	require.Nil(t, out)
}

// TestResolveClosedEdits_DiffTruncatedToCap: a diff longer than diffByteCap is
// carried truncated to the cap.
func TestResolveClosedEdits_DiffTruncatedToCap(t *testing.T) {
	e := edit("m.sql", "s3://b/content", "s3://b/diff", "model.a")
	bigDiff := strings.Repeat("a", diffByteCap+2048)
	sources := amendSources{files: map[string]string{amendMerge + "\x00m.sql": "SELECT 1\n"}}
	evidence := amendEvidence{objs: map[string]string{
		"s3://b/content": "SELECT 1\n",
		"s3://b/diff":    bigDiff,
	}}

	out, err := resolveClosedEdits(context.Background(), sources, evidence, amendRepo, amendMerge, []proposal.FileEdit{e})

	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Len(t, out[0].Diff, diffByteCap, "the diff must be truncated to the byte cap")
	require.Equal(t, bigDiff[:diffByteCap], out[0].Diff)
}
