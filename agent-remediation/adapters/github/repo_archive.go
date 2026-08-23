package github

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

var _ ports.RepoArchive = (*GitHub)(nil)

// maxArchiveBytes caps the total decompressed size accepted from a repo
// tarball, so a maliciously or accidentally huge archive cannot exhaust disk
// during extraction. A package var, not a const, so tests can lower it
// without needing a multi-hundred-megabyte fixture.
var maxArchiveBytes int64 = 200 << 20 // 200 MiB

// Fetch downloads the tarball of repo at ref via
// GET {base}/repos/{repo}/tarball/{ref} — the same base URL and PAT as
// ReadFile — and extracts it into a fresh temporary directory.
func (g *GitHub) Fetch(ctx context.Context, repo, ref string) (rootDir string, cleanup func(), err error) {
	u := fmt.Sprintf("%s/repos/%s/tarball/%s", g.baseURL, repo, ref)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", nil, fmt.Errorf("build github request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if g.token != "" {
		req.Header.Set("Authorization", "Bearer "+g.token)
	}
	resp, err := g.hc.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("github tarball %s@%s: %w", repo, ref, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return "", nil, ports.ErrSourceNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", nil, fmt.Errorf("github tarball %s@%s: status %d: %s",
			repo, ref, resp.StatusCode, truncate(errBody, 512))
	}

	dir, err := os.MkdirTemp("", "repo-archive-*")
	if err != nil {
		return "", nil, fmt.Errorf("create temp dir: %w", err)
	}
	if err := extractTarball(resp.Body, dir); err != nil {
		// Never leave a partially extracted tree behind on a failed fetch: a
		// temp dir that leaks on every failed remediation attempt is a slow
		// disk-fill in production.
		_ = os.RemoveAll(dir)
		return "", nil, fmt.Errorf("github tarball %s@%s: %w", repo, ref, err)
	}
	return dir, func() { _ = os.RemoveAll(dir) }, nil
}

// extractTarball extracts a gzip-compressed tar stream into dir, stripping
// the single top-level directory GitHub wraps every entry in (e.g.
// "owner-repo-<short-sha>/"). The top-level directory name is taken from the
// first entry; every subsequent entry must share it, and any entry whose
// relative path (after stripping) contains a ".." segment or is absolute is
// rejected as path traversal. Symlink entries are skipped — never followed,
// never recreated — so an untrusted archive cannot use one to redirect a
// later entry's write outside dir.
func extractTarball(r io.Reader, dir string) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("open gzip stream: %w", err)
	}
	defer func() { _ = gz.Close() }()

	// Bounds total decompressed bytes pulled out of the gzip stream across the
	// whole archive, independent of what any individual tar header claims —
	// the defense against a small compressed input that decompresses to a
	// huge one.
	limited := io.LimitReader(gz, maxArchiveBytes+1)
	tr := tar.NewReader(limited)

	var topPrefix string
	var totalBytes int64
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar entry: %w", err)
		}

		name := filepath.ToSlash(hdr.Name)
		if strings.HasPrefix(name, "/") {
			return fmt.Errorf("tar entry %q: absolute path not allowed", name)
		}
		if topPrefix == "" {
			if i := strings.Index(name, "/"); i >= 0 {
				topPrefix = name[:i+1]
			} else {
				topPrefix = name + "/"
			}
		}
		if !strings.HasPrefix(name, topPrefix) {
			return fmt.Errorf("tar entry %q: outside top-level directory %q", name, topPrefix)
		}
		rel := strings.TrimPrefix(name, topPrefix)
		if rel == "" {
			continue // the top-level directory entry itself; nothing to extract
		}
		for _, seg := range strings.Split(rel, "/") {
			if seg == ".." {
				return fmt.Errorf("tar entry %q: path traversal", name)
			}
		}

		target := filepath.Join(dir, filepath.FromSlash(rel))
		if target != dir && !strings.HasPrefix(target, dir+string(os.PathSeparator)) {
			return fmt.Errorf("tar entry %q: escapes extraction root", name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o750); err != nil {
				return fmt.Errorf("mkdir %q: %w", target, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
				return fmt.Errorf("mkdir %q: %w", filepath.Dir(target), err)
			}
			n, err := writeTarFile(target, tr)
			if err != nil {
				return err
			}
			totalBytes += n
			if totalBytes > maxArchiveBytes {
				return fmt.Errorf("archive exceeds %d byte size cap", maxArchiveBytes)
			}
		case tar.TypeSymlink:
			continue // never follow or recreate a symlink from an untrusted archive
		default:
			continue
		}
	}
	return nil
}

// writeTarFile copies one regular-file tar entry's content to target and
// returns the number of bytes written.
func writeTarFile(target string, r io.Reader) (int64, error) {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // G304: target is built by extractTarball, which already rejects any tar entry outside the top-level directory, containing "..", or escaping dir before target is ever computed.
	if err != nil {
		return 0, fmt.Errorf("create %q: %w", target, err)
	}
	defer func() { _ = f.Close() }()
	n, err := io.Copy(f, r)
	if err != nil {
		return n, fmt.Errorf("write %q: %w", target, err)
	}
	return n, nil
}
