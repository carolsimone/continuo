package overlay

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuild_DeterministicSortedTarWithReadableModes(t *testing.T) {
	a, err := Build([]File{{Path: "models/b.sql", Content: []byte("b")}, {Path: "models/a.sql", Content: []byte("a")}}, nil)
	require.NoError(t, err)
	b, err := Build([]File{{Path: "models/a.sql", Content: []byte("a")}, {Path: "models/b.sql", Content: []byte("b")}}, nil)
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
		_, err := Build([]File{{Path: p, Content: []byte("x")}}, nil)
		assert.Error(t, err, p)
	}
}

// TestBuild_CarriesEachFileContent proves the tarball is the files themselves,
// not just their names: the member read back out must be byte-identical to what
// went in, since the release check's compile leg runs whatever this lays down.
func TestBuild_CarriesEachFileContent(t *testing.T) {
	tarball, err := Build([]File{
		{Path: "models/a.sql", Content: []byte("select 1")},
		{Path: "seeds/nested/dir/c.csv", Content: []byte("id,name\n1,a\n")},
	}, nil)
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
	tarball, err := Build(nil, nil)
	require.NoError(t, err)

	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	require.NoError(t, err)
	_, err = tar.NewReader(gz).Next()
	assert.ErrorIs(t, err, io.EOF)
}

// TestBuild_DoesNotReorderTheCallersSlice pins that sorting happens on a copy:
// the caller keeps the edit order the clusters produced, and a builder that
// sorted the caller's own slice would silently reorder the fix's edits.
func TestBuild_DoesNotReorderTheCallersSlice(t *testing.T) {
	files := []File{{Path: "models/b.sql", Content: []byte("b")}, {Path: "models/a.sql", Content: []byte("a")}}
	_, err := Build(files, nil)
	require.NoError(t, err)
	assert.Equal(t, "models/b.sql", files[0].Path)
}

// members reads a built archive back as member name -> content.
func members(t *testing.T, tarball []byte) map[string]string {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(tarball))
	require.NoError(t, err)
	tr := tar.NewReader(gz)
	got := map[string]string{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return got
		}
		require.NoError(t, err)
		b, err := io.ReadAll(tr)
		require.NoError(t, err)
		got[h.Name] = string(b)
	}
}

// TestBuild_ListsTheDeletesInTheReservedMember proves a moved file's old path// reaches the release check: cleaned, sorted and without repeats, one per line.
func TestBuild_ListsTheDeletesInTheReservedMember(t *testing.T) {
	tarball, err := Build(
		[]File{{Path: "models/table_b_v2.sql", Content: []byte("select 1")}},
		[]string{"models/table_b.sql", "./models/old.sql", "models/table_b.sql"},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"models/table_b_v2.sql": "select 1",
		DeleteListName:          "models/old.sql\nmodels/table_b.sql\n",
	}, members(t, tarball))
}

// TestBuild_WithNoDeletesWritesNoDeleteList keeps every overlay that moves no
// file exactly the files it carries.
func TestBuild_WithNoDeletesWritesNoDeleteList(t *testing.T) {
	tarball, err := Build([]File{{Path: "models/a.sql", Content: []byte("a")}}, []string{})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"models/a.sql": "a"}, members(t, tarball))
}

// TestBuild_RefusesAContradictoryOrUnsafeDelete pins the refusals: a path both
// written and deleted, an unsafe path, and a file at the reserved name.
func TestBuild_RefusesAContradictoryOrUnsafeDelete(t *testing.T) {
	_, err := Build([]File{{Path: "models/a.sql", Content: []byte("a")}}, []string{"models/./a.sql"})
	assert.ErrorContains(t, err, "both written and deleted")
	_, err = Build(nil, []string{"../outside.sql"})
	assert.ErrorContains(t, err, "escapes the project")
	_, err = Build(nil, []string{"/abs.sql"})
	assert.ErrorContains(t, err, "relative to the project")
	_, err = Build([]File{{Path: DeleteListName, Content: []byte("x")}}, nil)
	assert.ErrorContains(t, err, "reserved")
}

// TestDeleteListName_MatchesTheSharedContract pins the reserved member's name
// to the shared fixture s3-sidecar's overlay fetcher is tested against, so the
// writer and the reader of the delete list agree on it.
func TestDeleteListName_MatchesTheSharedContract(t *testing.T) {
	raw, err := os.ReadFile("testdata/overlay_contract_v1.json")
	require.NoError(t, err)
	var contract struct {
		DeleteListMember string `json:"delete_list_member"`
	}
	require.NoError(t, json.Unmarshal(raw, &contract))
	assert.Equal(t, contract.DeleteListMember, DeleteListName)
}
