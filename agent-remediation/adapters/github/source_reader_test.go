package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
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

// TestSourceReader_AFileOverTheCapIsTooLarge verifies that a response body one
// byte over the cap makes ReadFile return ports.ErrSourceTooLarge, naming the
// file, the ref and the cap, rather than truncated content or a plain failure.
func TestSourceReader_AFileOverTheCapIsTooLarge(t *testing.T) {
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
	_, err := gh.ReadFile(context.Background(), "o/r", "ref", "models/big.sql")

	if !errors.Is(err, ports.ErrSourceTooLarge) {
		t.Fatalf("err = %v, want ErrSourceTooLarge", err)
	}
	for _, want := range []string{"models/big.sql", "ref", fmt.Sprint(ports.MaxSourceBytes)} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q, want it to name %q", err, want)
		}
	}
}

// TestSourceReader_AFileAtTheCapIsRead verifies that a body of exactly the cap
// is returned whole.
func TestSourceReader_AFileAtTheCapIsRead(t *testing.T) {
	atCap := strings.Repeat("x", maxSourceBytes)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(atCap))
	}))
	defer srv.Close()

	gh := NewSourceReader(srv.URL, "tok", srv.Client())
	body, err := gh.ReadFile(context.Background(), "o/r", "ref", "p")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(body) != maxSourceBytes {
		t.Fatalf("read %d bytes, want %d", len(body), maxSourceBytes)
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

func TestListDir_ReturnsFilePaths(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/contents/services/svc/models") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"path":"services/svc/models/a.sql","type":"file"},
			{"path":"services/svc/models/schema.yml","type":"file"},
			{"path":"services/svc/models/nested","type":"dir"}
		]`))
	}))
	defer srv.Close()

	g := NewSourceReader(srv.URL, "", srv.Client())
	got, err := g.ListDir(context.Background(), "owner/repo", "abc", "services/svc/models")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	want := []string{"services/svc/models/a.sql", "services/svc/models/schema.yml"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestListDir_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	g := NewSourceReader(srv.URL, "", srv.Client())
	_, err := g.ListDir(context.Background(), "owner/repo", "abc", "services/svc/models")
	if !errors.Is(err, ports.ErrSourceNotFound) {
		t.Fatalf("want ErrSourceNotFound, got %v", err)
	}
}
