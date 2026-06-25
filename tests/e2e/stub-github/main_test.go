package main

import (
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
