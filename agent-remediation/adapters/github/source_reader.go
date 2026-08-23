// Package github implements the SourceReader port over the GitHub Contents API.
// It is read-only: it issues a single authenticated GET per file and never
// performs a write request.
package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// maxSourceBytes is the maximum file size accepted from the GitHub Contents
// API. Files larger than this limit are rejected to prevent unbounded memory
// use and LLM token overruns.
const maxSourceBytes = 1 << 20 // 1 MiB

type GitHub struct {
	baseURL string
	token   string
	hc      *http.Client
}

var _ ports.SourceReader = (*GitHub)(nil)

// NewSourceReader builds a read-only GitHub source reader. baseURL is the API
// root (https://api.github.com in prod; a stub in dev/e2e).
func NewSourceReader(baseURL, token string, hc *http.Client) *GitHub {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &GitHub{baseURL: strings.TrimRight(baseURL, "/"), token: token, hc: hc}
}

// ReadFile returns the raw text of repo/path at ref. 404 → ErrSourceNotFound.
// Returns an error if the response body exceeds maxSourceBytes or cannot be
// read fully, so the caller never receives silently truncated content.
func (g *GitHub) ReadFile(ctx context.Context, repo, ref, path string) (string, error) {
	u := fmt.Sprintf("%s/repos/%s/contents/%s?ref=%s", g.baseURL, repo, path, url.QueryEscape(ref))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", fmt.Errorf("build github request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github.raw+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if g.token != "" {
		req.Header.Set("Authorization", "Bearer "+g.token)
	}
	resp, err := g.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("github get %s@%s: %w", path, ref, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return "", ports.ErrSourceNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// For error responses, read a short excerpt for the message body only.
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("github get %s@%s: status %d: %s", path, ref, resp.StatusCode, truncate(errBody, 512))
	}
	// Read one byte beyond the limit to detect oversized responses without
	// buffering the entire body.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSourceBytes+1))
	if err != nil {
		return "", fmt.Errorf("github get %s@%s: read body: %w", path, ref, err)
	}
	if len(body) > maxSourceBytes {
		return "", fmt.Errorf("github get %s@%s: source file exceeds %d bytes", path, ref, maxSourceBytes)
	}
	return string(body), nil
}

// ListDir returns the repo-relative paths of the files directly under dir at
// ref, using the GitHub Contents API directory listing. Sub-directories are
// omitted. A 404 (directory absent) → ErrSourceNotFound.
func (g *GitHub) ListDir(ctx context.Context, repo, ref, dir string) ([]string, error) {
	u := fmt.Sprintf("%s/repos/%s/contents/%s?ref=%s", g.baseURL, repo, dir, url.QueryEscape(ref))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("build github request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if g.token != "" {
		req.Header.Set("Authorization", "Bearer "+g.token)
	}
	resp, err := g.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("github list %s@%s: %w", dir, ref, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ports.ErrSourceNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("github list %s@%s: status %d: %s", dir, ref, resp.StatusCode, truncate(errBody, 512))
	}
	var entries []struct {
		Path string `json:"path"`
		Type string `json:"type"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxSourceBytes+1)).Decode(&entries); err != nil {
		return nil, fmt.Errorf("github list %s@%s: decode: %w", dir, ref, err)
	}
	paths := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Type == "file" {
			paths = append(paths, e.Path)
		}
	}
	return paths, nil
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n])
	}
	return string(b)
}
