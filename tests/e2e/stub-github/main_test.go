package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandleContents_RawAccept verifies that the remediation-agent read path
// (Accept containing "raw") still returns the canned ftable_e source.
func TestHandleContents_RawAccept(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/repos/org/repo/contents/models/ftable_e.sql", nil)
	req.Header.Set("Accept", "application/vnd.github.raw+json")
	rec := httptest.NewRecorder()

	handleContents(rec, req)

	res := rec.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "silly_error") {
		t.Errorf("expected canned source in body, got: %q", body)
	}
	if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "vnd.github.raw") {
		t.Errorf("unexpected Content-Type: %q", ct)
	}
}

// TestHandleContents_JSONAccept verifies that a JSON-Accept GET on the contents
// endpoint returns 404, signalling "file does not exist" to the PR creator so
// it can issue a create-without-sha.
func TestHandleContents_JSONAccept(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/repos/org/repo/contents/models/ftable_e.sql", nil)
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()

	handleContents(rec, req)

	if rec.Result().StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Result().StatusCode)
	}
}

// TestHandleGitRefs_CreateBranch verifies that POST /repos/.../git/refs returns
// 201 with the stub base SHA.
func TestHandleGitRefs_CreateBranch(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/repos/org/repo/git/refs", strings.NewReader(`{"ref":"refs/heads/stub","sha":"abc"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handleGitRefs(rec, req)

	res := rec.Result()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.StatusCode)
	}
	body := rec.Body.String()
	if !strings.Contains(body, stubBaseSHA) {
		t.Errorf("expected stub base SHA in body, got: %q", body)
	}
}

// TestHandlePulls_Create verifies that POST /repos/.../pulls returns 201 with
// the stub PR number and html_url.
func TestHandlePulls_Create(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/repos/org/repo/pulls", strings.NewReader(`{"title":"fix","head":"stub","base":"main"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handlePulls(rec, req)

	res := rec.Result()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.StatusCode)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"number"`) {
		t.Errorf("expected number in body, got: %q", body)
	}
	if !strings.Contains(body, "html_url") {
		t.Errorf("expected html_url in body, got: %q", body)
	}
}

// TestHandleApp_AccessTokens verifies that POST /app/installations/{id}/access_tokens
// returns 201 with the stub token (no JWT verification).
func TestHandleApp_AccessTokens(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/app/installations/12345/access_tokens", nil)
	req.Header.Set("Authorization", "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.stub")
	rec := httptest.NewRecorder()

	handleApp(rec, req)

	res := rec.Result()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.StatusCode)
	}
	body := rec.Body.String()
	if !strings.Contains(body, stubToken) {
		t.Errorf("expected token %q in body, got: %q", stubToken, body)
	}
	if !strings.Contains(body, "expires_at") {
		t.Errorf("expected expires_at in body, got: %q", body)
	}
}

// TestPullLifecycle_MergePath drives POST -> GET(open) -> PUT merge -> GET(merged).
func TestPullLifecycle_MergePath(t *testing.T) {
	resetPRs()
	// Open the PR.
	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodPost, "/repos/o/r/pulls", nil))
	if rec.Code != http.StatusCreated {
		t.Fatalf("open PR: got %d", rec.Code)
	}
	// It reads back open.
	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/o/r/pulls/1", nil))
	var pr struct {
		State  string `json:"state"`
		Merged bool   `json:"merged"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &pr); err != nil {
		t.Fatal(err)
	}
	if pr.State != "open" || pr.Merged {
		t.Fatalf("want open/unmerged, got %+v", pr)
	}
	// Merge it.
	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodPut, "/repos/o/r/pulls/1/merge", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("merge: got %d", rec.Code)
	}
	// It reads back merged.
	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/o/r/pulls/1", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &pr); err != nil {
		t.Fatal(err)
	}
	if pr.State != "closed" || !pr.Merged {
		t.Fatalf("want closed/merged, got %+v", pr)
	}
}

// TestPullLifecycle_ClosePath drives POST -> PATCH state=closed -> GET(closed, unmerged).
func TestPullLifecycle_ClosePath(t *testing.T) {
	resetPRs()
	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodPost, "/repos/o/r/pulls", nil))
	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodPatch, "/repos/o/r/pulls/1",
		strings.NewReader(`{"state":"closed"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("patch close: got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/o/r/pulls/1", nil))
	var pr struct {
		State  string `json:"state"`
		Merged bool   `json:"merged"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &pr); err != nil {
		t.Fatal(err)
	}
	if pr.State != "closed" || pr.Merged {
		t.Fatalf("want closed/unmerged, got %+v", pr)
	}
}

// TestListPulls_FindsPRByHeadBranch verifies GET pulls?head=owner:branch
// finds a PR a prior POST created for that branch — the read path the
// remediation-agent opening sweep's FindByBranch depends on to recover a
// stranded claim (a PR created on GitHub but never recorded).
func TestListPulls_FindsPRByHeadBranch(t *testing.T) {
	resetPRs()

	// Open a PR for branch "remediation/rel-1/model-attempt1".
	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodPost, "/repos/o/r/pulls",
		strings.NewReader(`{"title":"fix","head":"remediation/rel-1/model-attempt1","base":"main"}`)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create PR: got %d", rec.Code)
	}
	var created struct {
		Number int `json:"number"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	// A branch lookup in "owner:branch" form (as FindByBranch sends it) finds it.
	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet,
		"/repos/o/r/pulls?head=o%3Aremediation%2Frel-1%2Fmodel-attempt1&state=all&per_page=1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("list pulls: got %d", rec.Code)
	}
	var found []struct {
		Number    int    `json:"number"`
		CreatedAt string `json:"created_at"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &found); err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].Number != created.Number {
		t.Fatalf("expected exactly one match with number %d, got %+v", created.Number, found)
	}
	if found[0].CreatedAt == "" {
		t.Error("expected a non-empty created_at on the matched PR")
	}

	// An unrelated branch finds nothing.
	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/o/r/pulls?head=o%3Aother-branch&state=all", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &found); err != nil {
		t.Fatal(err)
	}
	if len(found) != 0 {
		t.Fatalf("expected no match for an unrelated branch, got %+v", found)
	}
}

// TestListPulls_FiltersByState verifies the state query parameter: "open"
// (the default) excludes a closed PR, and "all" includes it.
func TestListPulls_FiltersByState(t *testing.T) {
	resetPRs()

	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodPost, "/repos/o/r/pulls",
		strings.NewReader(`{"title":"fix","head":"stub-state","base":"main"}`)))
	var created struct {
		Number int `json:"number"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	// Still open: the default "open" state finds it.
	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/o/r/pulls?head=stub-state", nil))
	var openMatches []struct {
		Number int `json:"number"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &openMatches); err != nil {
		t.Fatal(err)
	}
	if len(openMatches) != 1 {
		t.Fatalf("expected one open match, got %+v", openMatches)
	}

	// Close it, then the default "open" filter must exclude it.
	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodPatch, fmt.Sprintf("/repos/o/r/pulls/%d", created.Number),
		strings.NewReader(`{"state":"closed"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("close PR: got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/o/r/pulls?head=stub-state", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &openMatches); err != nil {
		t.Fatal(err)
	}
	if len(openMatches) != 0 {
		t.Fatalf("expected no open match after closing, got %+v", openMatches)
	}

	// "state=all" still finds the now-closed PR.
	rec = httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/o/r/pulls?head=stub-state&state=all", nil))
	var allMatches []struct {
		Number int `json:"number"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &allMatches); err != nil {
		t.Fatal(err)
	}
	if len(allMatches) != 1 {
		t.Fatalf("expected one match with state=all, got %+v", allMatches)
	}
}

// TestGetUnknownPullIs404 verifies an unregistered PR number returns 404.
func TestGetUnknownPullIs404(t *testing.T) {
	resetPRs()
	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/o/r/pulls/99", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", rec.Code)
	}
}

// TestHandleCommits_ReturnsCannedFilePatch verifies GET /repos/.../commits/{sha}
// returns a commit whose files[] carries the canned upstream patch, exercising
// the remediation-agent CommitFileDiff read path.
func TestHandleCommits_ReturnsCannedFilePatch(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/repos/owner/repo/commits/anysha", nil)
	req.Header.Set("Accept", "application/vnd.github+json")

	handleRepos(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var commit struct {
		Files []struct {
			Filename string `json:"filename"`
			Patch    string `json:"patch"`
		} `json:"files"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &commit); err != nil {
		t.Fatal(err)
	}
	if len(commit.Files) == 0 || commit.Files[0].Patch == "" {
		t.Fatalf("expected a file with a patch, got %+v", commit.Files)
	}
	if commit.Files[0].Filename != "services/service-2/models/ftable_upstream_diff.sql" {
		t.Errorf("filename = %q, want services/service-2/models/ftable_upstream_diff.sql", commit.Files[0].Filename)
	}
}
