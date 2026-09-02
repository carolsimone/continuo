package overlay

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuild_DeterministicSortedTarWithReadableModes(t *testing.T) {
	a, err := Build([]File{{Path: "models/b.sql", Content: []byte("b")}, {Path: "models/a.sql", Content: []byte("a")}})
	require.NoError(t, err)
	b, err := Build([]File{{Path: "models/a.sql", Content: []byte("a")}, {Path: "models/b.sql", Content: []byte("b")}})
	require.NoError(t, err)
	assert.Equal(t, a, b, "same files in any order must build byte-identical tarballs")

	gz, err := gzip.NewReader(bytes.NewReader(a))
	require.NoError(t, err)
	tr := tar.NewReader(gz)
	var names []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		names = append(names, h.Name)
		assert.Equal(t, int64(0o644), h.Mode)
	}
	assert.Equal(t, []string{"models/a.sql", "models/b.sql"}, names)
}

func TestBuild_RejectsUnsafePaths(t *testing.T) {
	for _, p := range []string{"/abs.sql", "../up.sql", "models/../../up.sql", ""} {
		_, err := Build([]File{{Path: p, Content: []byte("x")}})
		assert.Error(t, err, p)
	}
}

// TestBuild_CarriesEachFileContent proves the tarball is the files themselves,
// not just their names: the member read back out must be byte-identical to what
// went in, since the verification run's compile leg runs whatever this lays down.
func TestBuild_CarriesEachFileContent(t *testing.T) {
	tarball, err := Build([]File{
		{Path: "models/a.sql", Content: []byte("select 1")},
		{Path: "seeds/nested/dir/c.csv", Content: []byte("id,name\n1,a\n")},
	})
	require.NoError(t, err)

	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	require.NoError(t, err)
	tr := tar.NewReader(gz)
	got := map[string]string{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		body, err := io.ReadAll(tr)
		require.NoError(t, err)
		require.Equal(t, int64(len(body)), h.Size)
		got[h.Name] = string(body)
	}
	assert.Equal(t, map[string]string{
		"models/a.sql":           "select 1",
		"seeds/nested/dir/c.csv": "id,name\n1,a\n",
	}, got)
}

// TestBuild_EmptyInputIsAnEmptyArchive keeps the caller from having to special
// case "nothing to overlay": an empty set builds a valid, readable archive with
// no members rather than an error or a corrupt stream.
func TestBuild_EmptyInputIsAnEmptyArchive(t *testing.T) {
	tarball, err := Build(nil)
	require.NoError(t, err)

	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	require.NoError(t, err)
	_, err = tar.NewReader(gz).Next()
	assert.ErrorIs(t, err, io.EOF)
}

// TestBuild_DoesNotReorderTheCallersSlice pins that sorting happens on a copy:
// the driver keeps the edit order the clusters produced, and a builder that
// sorted the caller's own slice would silently reorder the proposal's edits.
func TestBuild_DoesNotReorderTheCallersSlice(t *testing.T) {
	files := []File{{Path: "models/b.sql", Content: []byte("b")}, {Path: "models/a.sql", Content: []byte("a")}}
	_, err := Build(files)
	require.NoError(t, err)
	assert.Equal(t, "models/b.sql", files[0].Path)
}
