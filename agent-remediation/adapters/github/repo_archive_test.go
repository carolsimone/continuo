package github

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// tarEntry describes one entry to write into a test tarball. A zero
// typeflag defaults to a regular file; content is ignored for directories
// and symlinks.
type tarEntry struct {
	name       string
	content    []byte
	typeflag   byte
	linkname   string
	paxRecords map[string]string
}

// buildTarball gzip-compresses a tar archive built from entries, for serving
// from an httptest.Server as a stand-in for GitHub's tarball endpoint.
func buildTarball(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		typeflag := e.typeflag
		if typeflag == 0 {
			typeflag = tar.TypeReg
		}
		hdr := &tar.Header{
			Name:     e.name,
			Mode:     0o644,
			Typeflag: typeflag,
			Linkname: e.linkname,
		}
		if typeflag == tar.TypeXGlobalHeader {
			// archive/tar encodes a global header from its PAX records alone
			// and rejects any other field; the entry's on-disk name is chosen
			// by the writer, as git archive chooses "pax_global_header".
			hdr = &tar.Header{Typeflag: tar.TypeXGlobalHeader, PAXRecords: e.paxRecords}
		}
		if typeflag == tar.TypeReg {
			hdr.Size = int64(len(e.content))
		}
		if typeflag == tar.TypeDir {
			hdr.Mode = 0o755
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("write header %q: %v", e.name, err)
		}
		if typeflag == tar.TypeReg {
			if _, err := tw.Write(e.content); err != nil {
				t.Fatalf("write content %q: %v", e.name, err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}
	return buf.Bytes()
}

func serveTarball(t *testing.T, tb []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(tb)
	}))
}

func TestFetch_ExtractsAndStripsTopLevelDir(t *testing.T) {
	tb := buildTarball(t, []tarEntry{
		{name: "repo-abc123/", typeflag: tar.TypeDir},
		{name: "repo-abc123/contracts/", typeflag: tar.TypeDir},
		{name: "repo-abc123/contracts/a.yml", content: []byte("nodes:\n  - schema: analytics\n    table: t\n")},
	})
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(tb)
	}))
	defer srv.Close()

	gh := NewSourceReader(srv.URL, "tok", srv.Client())
	root, cleanup, err := gh.Fetch(context.Background(), "owner/repo", "abc123")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer cleanup()

	if gotPath != "/repos/owner/repo/tarball/abc123" {
		t.Errorf("path = %q", gotPath)
	}

	got, err := os.ReadFile(filepath.Join(root, "contracts", "a.yml")) //nolint:gosec // G304: root is the temp dir Fetch just created and returned in this test, not external input.
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	want := "nodes:\n  - schema: analytics\n    table: t\n"
	if string(got) != want {
		t.Fatalf("content = %q, want %q", got, want)
	}

	// cleanup must be idempotent and must actually remove the tree.
	cleanup()
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("root still exists after cleanup: err=%v", err)
	}
}

func TestFetch_RejectsPathTraversal(t *testing.T) {
	tb := buildTarball(t, []tarEntry{
		{name: "repo-abc123/contracts/a.yml", content: []byte("ok")},
		{name: "../evil", content: []byte("pwned")},
	})
	srv := serveTarball(t, tb)
	defer srv.Close()

	before, _ := filepath.Glob(filepath.Join(os.TempDir(), "repo-archive-*"))

	gh := NewSourceReader(srv.URL, "tok", srv.Client())
	_, cleanup, err := gh.Fetch(context.Background(), "owner/repo", "abc123")
	if err == nil {
		if cleanup != nil {
			cleanup()
		}
		t.Fatal("expected error for path-traversal entry, got nil")
	}
	if cleanup != nil {
		t.Fatal("expected nil cleanup on the error path")
	}

	after, _ := filepath.Glob(filepath.Join(os.TempDir(), "repo-archive-*"))
	if len(after) > len(before) {
		t.Fatalf("temp dir leaked on error path: before=%v after=%v", before, after)
	}
}

func TestFetch_404IsErrSourceNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	gh := NewSourceReader(srv.URL, "tok", srv.Client())
	_, cleanup, err := gh.Fetch(context.Background(), "owner/repo", "missing")
	if !errors.Is(err, ports.ErrSourceNotFound) {
		t.Fatalf("err = %v, want ErrSourceNotFound", err)
	}
	if cleanup != nil {
		t.Fatal("expected nil cleanup on the error path")
	}
}

// TestFetch_SkipsSymlinks verifies that a symlink entry in the tarball is
// neither followed nor recreated on disk, so an archive cannot use a
// symlink to make a later regular-file entry write outside the extraction
// root.
func TestFetch_SkipsSymlinks(t *testing.T) {
	tb := buildTarball(t, []tarEntry{
		{name: "repo-abc123/contracts/a.yml", content: []byte("ok")},
		{name: "repo-abc123/evil-link", typeflag: tar.TypeSymlink, linkname: "/etc/passwd"},
	})
	srv := serveTarball(t, tb)
	defer srv.Close()

	gh := NewSourceReader(srv.URL, "tok", srv.Client())
	root, cleanup, err := gh.Fetch(context.Background(), "owner/repo", "abc123")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer cleanup()

	if _, err := os.Lstat(filepath.Join(root, "evil-link")); !os.IsNotExist(err) {
		t.Fatalf("expected symlink entry to be skipped, got err = %v", err)
	}
}

// TestFetch_SizeCapExceeded verifies that decompressed archive content over
// the size cap fails extraction rather than silently writing a truncated
// file. maxArchiveBytes is lowered for the duration of the test so this does
// not require a genuinely huge fixture.
func TestFetch_SizeCapExceeded(t *testing.T) {
	orig := maxArchiveBytes
	maxArchiveBytes = 16
	defer func() { maxArchiveBytes = orig }()

	tb := buildTarball(t, []tarEntry{
		{name: "repo-abc123/big.txt", content: bytes.Repeat([]byte("x"), 64)},
	})
	srv := serveTarball(t, tb)
	defer srv.Close()

	gh := NewSourceReader(srv.URL, "tok", srv.Client())
	_, cleanup, err := gh.Fetch(context.Background(), "owner/repo", "abc123")
	if err == nil {
		if cleanup != nil {
			cleanup()
		}
		t.Fatal("expected error for archive exceeding size cap, got nil")
	}
}

// GitHub builds its tarballs with git archive, whose first entry is a PAX
// global header named "pax_global_header" (typeflag 'g', carrying the commit
// sha as a comment record) that precedes the top-level directory. It is
// metadata, not a file: extraction must skip it rather than read it as the
// archive's top-level directory and then reject every real entry.
func TestFetch_SkipsPaxGlobalHeaderBeforeTopLevelDir(t *testing.T) {
	tb := buildTarball(t, []tarEntry{
		{typeflag: tar.TypeXGlobalHeader, paxRecords: map[string]string{"comment": "0123456789abcdef0123456789abcdef01234567"}},
		{name: "owner-repo-0123456/", typeflag: tar.TypeDir},
		{name: "owner-repo-0123456/services/service-py/contracts/", typeflag: tar.TypeDir},
		{name: "owner-repo-0123456/services/service-py/contracts/kpis.yml", content: []byte("nodes: []\n")},
	})
	srv := serveTarball(t, tb)
	defer srv.Close()

	g := NewSourceReader(srv.URL, "tok", srv.Client())
	root, cleanup, err := g.Fetch(context.Background(), "owner/repo", "0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer cleanup()

	got, err := os.ReadFile(filepath.Join(root, "services", "service-py", "contracts", "kpis.yml")) //nolint:gosec // G304: root is the extraction directory Fetch just created for this test.
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(got) != "nodes: []\n" {
		t.Fatalf("extracted content = %q", got)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read root: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "services" {
		t.Fatalf("root must hold only the stripped tree, got %v", entries)
	}
}
