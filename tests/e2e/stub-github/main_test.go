package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestHandleContents_RawAccept verifies that the agent-remediation read path
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
// agent-remediation opening sweep's FindByBranch depends on to recover a
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

// TestHandleGitBlobs_Create verifies POST /repos/.../git/blobs returns 201
// with a sha, and that the sha is a deterministic function of the posted
// content: the same content always yields the same sha, and different
// content yields a different one.
func TestHandleGitBlobs_Create(t *testing.T) {
	post := func(content string) (int, string) {
		rec := httptest.NewRecorder()
		body := fmt.Sprintf(`{"content":%q,"encoding":"base64"}`, content)
		handleRepos(rec, httptest.NewRequest(http.MethodPost, "/repos/o/r/git/blobs", strings.NewReader(body)))
		var resp struct {
			SHA string `json:"sha"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		return rec.Code, resp.SHA
	}

	code1, sha1 := post("aGVsbG8=")
	if code1 != http.StatusCreated {
		t.Fatalf("expected 201, got %d", code1)
	}
	if sha1 == "" {
		t.Fatal("expected a non-empty sha")
	}

	code2, sha2 := post("aGVsbG8=")
	if code2 != http.StatusCreated || sha2 != sha1 {
		t.Fatalf("expected same content to yield the same sha: %q vs %q", sha1, sha2)
	}

	_, sha3 := post("d29ybGQ=")
	if sha3 == sha1 {
		t.Fatalf("expected different content to yield a different sha, both were %q", sha1)
	}
}

// TestHandleGitCommits_Get verifies GET /repos/.../git/commits/{sha} returns
// the requested commit's sha plus a tree.sha, since the PR creator reads
// baseCommit.data.tree.sha to use as the new tree's base_tree.
func TestHandleGitCommits_Get(t *testing.T) {
	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/o/r/git/commits/"+stubBaseSHA, nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp struct {
		SHA  string `json:"sha"`
		Tree struct {
			SHA string `json:"sha"`
		} `json:"tree"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v, body: %q", err, rec.Body.String())
	}
	if resp.SHA != stubBaseSHA {
		t.Errorf("expected sha %q, got %q", stubBaseSHA, resp.SHA)
	}
	if resp.Tree.SHA == "" {
		t.Error("expected a non-empty tree.sha")
	}
}

// TestHandleGitTrees_Create verifies POST /repos/.../git/trees returns 201
// with a sha, and that the posted entries are recorded under that sha —
// retrievable via recordedTreeFor — so a test can assert which paths were
// committed, in which order, and onto which base tree.
func TestHandleGitTrees_Create(t *testing.T) {
	resetGitWrites()

	reqBody := `{"base_tree":"basetree0","tree":[` +
		`{"path":"models/a.sql","mode":"100644","type":"blob","sha":"shaA"},` +
		`{"path":"models/b.sql","mode":"100644","type":"blob","sha":"shaB"}` +
		`]}`
	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodPost, "/repos/o/r/git/trees", strings.NewReader(reqBody)))

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	var resp struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v, body: %q", err, rec.Body.String())
	}
	if resp.SHA == "" {
		t.Fatal("expected a non-empty tree sha")
	}

	recorded, ok := recordedTreeFor(resp.SHA)
	if !ok {
		t.Fatalf("expected tree %q to be recorded", resp.SHA)
	}
	if recorded.BaseTree != "basetree0" {
		t.Errorf("expected base_tree %q, got %q", "basetree0", recorded.BaseTree)
	}
	if len(recorded.Entries) != 2 || recorded.Entries[0].Path != "models/a.sql" || recorded.Entries[1].Path != "models/b.sql" {
		t.Fatalf("expected both paths recorded in order, got %+v", recorded.Entries)
	}
}

// TestHandleGitCommits_Create verifies POST /repos/.../git/commits returns
// 201 with a sha, and that the posted message/tree/parents are recorded
// under that sha so a test can assert what a given commit carries. The same
// git/commits path serves both GET and POST.
func TestHandleGitCommits_Create(t *testing.T) {
	resetGitWrites()

	reqBody := `{"message":"fix ftable_e","tree":"treesha0","parents":["basesha0"]}`
	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodPost, "/repos/o/r/git/commits", strings.NewReader(reqBody)))

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
	var resp struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v, body: %q", err, rec.Body.String())
	}
	if resp.SHA == "" {
		t.Fatal("expected a non-empty commit sha")
	}

	recorded, ok := recordedCommitFor(resp.SHA)
	if !ok {
		t.Fatalf("expected commit %q to be recorded", resp.SHA)
	}
	if recorded.Message != "fix ftable_e" || recorded.Tree != "treesha0" || len(recorded.Parents) != 1 || recorded.Parents[0] != "basesha0" {
		t.Fatalf("unexpected recorded commit: %+v", recorded)
	}
}

// TestHandleGitRefs_UpdatePatch verifies PATCH
// /repos/.../git/refs/heads/{branch} returns 200 with the updated ref
// object, moving the head branch onto a new commit sha. The same git/refs
// path serves both POST (create) and PATCH (update).
func TestHandleGitRefs_UpdatePatch(t *testing.T) {
	reqBody := `{"sha":"newcommitsha0","force":true}`
	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodPatch, "/repos/o/r/git/refs/heads/stub", strings.NewReader(reqBody)))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp struct {
		Ref    string `json:"ref"`
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v, body: %q", err, rec.Body.String())
	}
	if resp.Ref != "refs/heads/stub" {
		t.Errorf("expected ref %q, got %q", "refs/heads/stub", resp.Ref)
	}
	if resp.Object.SHA != "newcommitsha0" {
		t.Errorf("expected updated sha %q, got %q", "newcommitsha0", resp.Object.SHA)
	}
}

// TestHandleTarball_ServesFixtureUnderSingleTopDirectory verifies the shape the
// agent-remediation's archive reader depends on: a gzipped tar whose every
// entry sits under exactly one top-level directory named "{repo}-{ref}". The
// reader strips that one leading segment, so an archive without it would
// extract a directory level too high and no contract file would be found.
func TestHandleTarball_ServesFixtureUnderSingleTopDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "services", "svc", "contracts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "services", "svc", "contracts", "n.yml"),
		[]byte("nodes: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	prev := repoFixtureDir
	repoFixtureDir = root
	defer func() { repoFixtureDir = prev }()

	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/owner/my-repo/tarball/deadbeef", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/gzip" {
		t.Errorf("unexpected Content-Type: %q", ct)
	}

	gz, err := gzip.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatalf("open gzip stream: %v", err)
	}
	tr := tar.NewReader(gz)
	var names []string
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		names = append(names, hdr.Name)
	}
	if len(names) == 0 {
		t.Fatal("archive is empty")
	}
	const top = "my-repo-deadbeef/"
	for _, n := range names {
		if !strings.HasPrefix(n, top) {
			t.Errorf("entry %q is not under the single top-level directory %q", n, top)
		}
	}
	want := top + "services/svc/contracts/n.yml"
	found := false
	for _, n := range names {
		if n == want {
			found = true
		}
	}
	if !found {
		t.Errorf("expected entry %q, got %v", want, names)
	}
}

// TestHandleTarball_NoFixtureIs404 verifies that a stub with no repository
// fixture reports the repository as absent, which the archive reader maps to a
// permanent "not available" skip rather than a retry loop.
func TestHandleTarball_NoFixtureIs404(t *testing.T) {
	prev := repoFixtureDir
	repoFixtureDir = ""
	defer func() { repoFixtureDir = prev }()

	rec := httptest.NewRecorder()
	handleRepos(rec, httptest.NewRequest(http.MethodGet, "/repos/owner/my-repo/tarball/deadbeef", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}
