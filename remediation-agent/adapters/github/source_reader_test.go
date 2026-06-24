package github

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

func TestReadFile_BuildsRawContentsRequest(t *testing.T) {
	var gotPath, gotQuery, gotAuth, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		gotAuth, gotAccept = r.Header.Get("Authorization"), r.Header.Get("Accept")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("select 1"))
	}))
	defer srv.Close()

	gh := NewSourceReader(srv.URL, "tok", srv.Client())
	body, err := gh.ReadFile(context.Background(), "owner/repo", "abc123", "models/x.sql")
	if err != nil {
		t.Fatal(err)
	}
	if body != "select 1" {
		t.Fatalf("body = %q", body)
	}
	if gotPath != "/repos/owner/repo/contents/models/x.sql" {
		t.Errorf("path = %q", gotPath)
	}
	if gotQuery != "ref=abc123" {
		t.Errorf("query = %q", gotQuery)
	}
	if gotAuth != "Bearer tok" {
		t.Errorf("auth = %q", gotAuth)
	}
	if gotAccept != "application/vnd.github.raw+json" {
		t.Errorf("accept = %q", gotAccept)
	}
}

func TestReadFile_404IsErrSourceNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer srv.Close()
	gh := NewSourceReader(srv.URL, "tok", srv.Client())
	if _, err := gh.ReadFile(context.Background(), "o/r", "ref", "p"); err != ports.ErrSourceNotFound {
		t.Fatalf("err = %v, want ErrSourceNotFound", err)
	}
}

// TestReadFile_OversizeBodyReturnsError verifies that a response body larger
// than maxSourceBytes causes ReadFile to return an error rather than silently
// returning truncated content.
func TestReadFile_OversizeBodyReturnsError(t *testing.T) {
	oversized := make([]byte, maxSourceBytes+1)
	for i := range oversized {
		oversized[i] = 'x'
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(oversized)
	}))
	defer srv.Close()

	gh := NewSourceReader(srv.URL, "tok", srv.Client())
	_, err := gh.ReadFile(context.Background(), "o/r", "ref", "p")
	if err == nil {
		t.Fatal("expected error for oversize body, got nil")
	}
}

// TestReadFile_SmallBodySucceeds verifies that a response body within the
// size limit is returned successfully.
func TestReadFile_SmallBodySucceeds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("select 42"))
	}))
	defer srv.Close()

	gh := NewSourceReader(srv.URL, "tok", srv.Client())
	body, err := gh.ReadFile(context.Background(), "o/r", "ref", "p")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if body != "select 42" {
		t.Fatalf("body = %q, want %q", body, "select 42")
	}
}
