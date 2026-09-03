// Package overlay builds the source-overlay tarball a dbt fix-verification
// run lays over the team's project before it compiles: the proposed content
// of every file the fix edits, keyed by its path within that project. It is
// pure — it takes the files and returns the bytes, performing no I/O — so the
// archive's shape is testable without object storage or a release.
//
// The archive is deterministic: the same set of files always produces the same
// bytes, whatever order they arrive in. Two attempts that propose identical
// content therefore write an identical object, so a redelivery overwrites the
// object it wrote before rather than leaving two archives a reader must choose
// between.
package overlay

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
)

// File is one member of the overlay: the project-relative path the content is
// laid down at, using forward slashes, and the content itself.
type File struct {
	Path    string
	Content []byte
}

// Build returns the gzip-compressed tar archive of files. Members are written
// sorted by path with a fixed mode, timestamp, and ownership, so the bytes
// depend only on the set of files and not on the order they were collected in
// or the machine that built them. An empty set builds a valid, empty archive.
//
// Every path must stay inside the project the archive is unpacked over, so an
// empty, absolute, or upward-traversing path is refused rather than written.
func Build(files []File) ([]byte, error) {
	sorted := make([]File, len(files))
	copy(sorted, files)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, f := range sorted {
		name, err := memberName(f.Path)
		if err != nil {
			return nil, err
		}
		if err := tw.WriteHeader(&tar.Header{
			Typeflag: tar.TypeReg,
			Name:     name,
			Mode:     0o644,
			Size:     int64(len(f.Content)),
			ModTime:  time.Unix(0, 0).UTC(),
			Uid:      0,
			Gid:      0,
		}); err != nil {
			return nil, fmt.Errorf("write overlay header for %s: %w", name, err)
		}
		if _, err := tw.Write(f.Content); err != nil {
			return nil, fmt.Errorf("write overlay content for %s: %w", name, err)
		}
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("close overlay archive: %w", err)
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("compress overlay archive: %w", err)
	}
	return buf.Bytes(), nil
}

// memberName validates a path as a project-relative archive member and returns
// its cleaned form. A path that is empty, absolute, or resolves outside the
// project would write over files the fix never proposed changing once the
// archive is unpacked, so it is refused here rather than carried.
func memberName(p string) (string, error) {
	if p == "" {
		return "", fmt.Errorf("overlay: a file needs a path")
	}
	if strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("overlay: path %q must be relative to the project", p)
	}
	clean := path.Clean(p)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("overlay: path %q escapes the project", p)
	}
	return clean, nil
}
