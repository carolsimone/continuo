// Package github implements the SourceReader port over the GitHub Contents API.
// It is read-only: it issues a single authenticated GET per file and never
// performs a write request.
package github

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

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
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("github get %s@%s: status %d: %s", path, ref, resp.StatusCode, truncate(body, 512))
	}
	return string(body), nil
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n])
	}
	return string(b)
}
