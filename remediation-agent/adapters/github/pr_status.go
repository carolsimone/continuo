package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

var _ ports.PullRequestStatusChecker = (*GitHub)(nil)
var _ ports.PullRequestBranchFinder = (*GitHub)(nil)

// PRStatus returns the current state of repo's pull request #number via a
// single read-only GET. Every non-2xx response (including 404) is an error so
// the caller leaves the proposal untouched and retries on the next pass.
func (g *GitHub) PRStatus(ctx context.Context, repo string, number int) (ports.PRStatus, error) {
	u := fmt.Sprintf("%s/repos/%s/pulls/%d", g.baseURL, repo, number)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return ports.PRStatus{}, fmt.Errorf("build github request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if g.token != "" {
		req.Header.Set("Authorization", "Bearer "+g.token)
	}
	resp, err := g.hc.Do(req)
	if err != nil {
		return ports.PRStatus{}, fmt.Errorf("github get pull %s#%d: %w", repo, number, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		// A 401 (bad/missing credentials) or a 403 that is NOT rate limiting
		// means the token cannot read PR state (missing Pull requests: Read).
		// Tag it so the reconciler surfaces a standing, human-actionable
		// degraded signal. GitHub also returns 403 (and 429) for primary and
		// secondary rate limits — those are transient, so they fall through to
		// a generic error the reconciler simply retries.
		if resp.StatusCode == http.StatusUnauthorized ||
			(resp.StatusCode == http.StatusForbidden && !isRateLimited(resp.Header)) {
			return ports.PRStatus{}, fmt.Errorf("github get pull %s#%d: status %d: %s: %w",
				repo, number, resp.StatusCode, truncate(errBody, 512), ports.ErrPermissionDenied)
		}
		return ports.PRStatus{}, fmt.Errorf("github get pull %s#%d: status %d: %s",
			repo, number, resp.StatusCode, truncate(errBody, 512))
	}
	var body struct {
		State    string     `json:"state"`
		Merged   bool       `json:"merged"`
		MergedAt *time.Time `json:"merged_at"`
		ClosedAt *time.Time `json:"closed_at"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxSourceBytes)).Decode(&body); err != nil {
		return ports.PRStatus{}, fmt.Errorf("github get pull %s#%d: decode: %w", repo, number, err)
	}
	st := ports.PRStatus{Closed: body.State == "closed", Merged: body.Merged}
	switch {
	case body.Merged && body.MergedAt != nil:
		st.ClosedAt = *body.MergedAt
	case body.ClosedAt != nil:
		st.ClosedAt = *body.ClosedAt
	}
	return st, nil
}

// FindByBranch looks up repo's pull request(s) with the given head branch via
// a single read-only GET, and returns the first match. GitHub's list endpoint
// returns at most one open PR per branch (it forbids opening a second), so a
// single result covers the normal case; state=all also surfaces one already
// closed since creation, which the reconciler records and lets the ordinary
// open-PR sweep carry to its terminal outcome next pass.
func (g *GitHub) FindByBranch(ctx context.Context, repo, branch string) (ports.PullRequestRef, bool, error) {
	owner, _, ok := strings.Cut(repo, "/")
	if !ok {
		return ports.PullRequestRef{}, false, fmt.Errorf("find pr by branch: invalid repo %q", repo)
	}
	q := url.Values{
		"head":     {owner + ":" + branch},
		"state":    {"all"},
		"per_page": {"1"},
	}
	u := fmt.Sprintf("%s/repos/%s/pulls?%s", g.baseURL, repo, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return ports.PullRequestRef{}, false, fmt.Errorf("build github request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if g.token != "" {
		req.Header.Set("Authorization", "Bearer "+g.token)
	}
	resp, err := g.hc.Do(req)
	if err != nil {
		return ports.PullRequestRef{}, false, fmt.Errorf("github list pulls %s head=%s: %w", repo, branch, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if resp.StatusCode == http.StatusUnauthorized ||
			(resp.StatusCode == http.StatusForbidden && !isRateLimited(resp.Header)) {
			return ports.PullRequestRef{}, false, fmt.Errorf("github list pulls %s head=%s: status %d: %s: %w",
				repo, branch, resp.StatusCode, truncate(errBody, 512), ports.ErrPermissionDenied)
		}
		return ports.PullRequestRef{}, false, fmt.Errorf("github list pulls %s head=%s: status %d: %s",
			repo, branch, resp.StatusCode, truncate(errBody, 512))
	}
	var body []struct {
		Number    int        `json:"number"`
		HTMLURL   string     `json:"html_url"`
		CreatedAt *time.Time `json:"created_at"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxSourceBytes)).Decode(&body); err != nil {
		return ports.PullRequestRef{}, false, fmt.Errorf("github list pulls %s head=%s: decode: %w", repo, branch, err)
	}
	if len(body) == 0 {
		return ports.PullRequestRef{}, false, nil
	}
	ref := ports.PullRequestRef{Number: body[0].Number, URL: body[0].HTMLURL}
	if body[0].CreatedAt != nil {
		ref.CreatedAt = *body[0].CreatedAt
	}
	return ref, true, nil
}

// isRateLimited reports whether a 4xx response's headers indicate GitHub
// throttling rather than a permission problem. Primary rate limits exhaust the
// budget (X-RateLimit-Remaining: 0); secondary/abuse limits set Retry-After.
// A genuine permission 403 has neither.
func isRateLimited(h http.Header) bool {
	return h.Get("Retry-After") != "" || h.Get("X-RateLimit-Remaining") == "0"
}
