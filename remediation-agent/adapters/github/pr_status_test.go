package github

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

func prStatusServer(t *testing.T, wantPath string, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, wantPath, r.URL.Path)
		require.Equal(t, "Bearer tkn", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func TestPRStatus_Open(t *testing.T) {
	srv := prStatusServer(t, "/repos/acme/dbt-repo/pulls/7", http.StatusOK,
		`{"state":"open","merged":false,"merged_at":null,"closed_at":null}`)
	defer srv.Close()
	g := NewSourceReader(srv.URL, "tkn", srv.Client())
	st, err := g.PRStatus(context.Background(), "acme/dbt-repo", 7)
	require.NoError(t, err)
	require.False(t, st.Closed)
	require.False(t, st.Merged)
	require.True(t, st.ClosedAt.IsZero())
}

func TestPRStatus_Merged(t *testing.T) {
	srv := prStatusServer(t, "/repos/acme/dbt-repo/pulls/7", http.StatusOK,
		`{"state":"closed","merged":true,"merged_at":"2026-07-01T10:00:00Z","closed_at":"2026-07-01T10:00:00Z"}`)
	defer srv.Close()
	g := NewSourceReader(srv.URL, "tkn", srv.Client())
	st, err := g.PRStatus(context.Background(), "acme/dbt-repo", 7)
	require.NoError(t, err)
	require.True(t, st.Closed)
	require.True(t, st.Merged)
	require.Equal(t, time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC), st.ClosedAt.UTC())
}

func TestPRStatus_ClosedWithoutMerge(t *testing.T) {
	srv := prStatusServer(t, "/repos/acme/dbt-repo/pulls/7", http.StatusOK,
		`{"state":"closed","merged":false,"merged_at":null,"closed_at":"2026-07-02T09:30:00Z"}`)
	defer srv.Close()
	g := NewSourceReader(srv.URL, "tkn", srv.Client())
	st, err := g.PRStatus(context.Background(), "acme/dbt-repo", 7)
	require.NoError(t, err)
	require.True(t, st.Closed)
	require.False(t, st.Merged)
	require.Equal(t, time.Date(2026, 7, 2, 9, 30, 0, 0, time.UTC), st.ClosedAt.UTC())
}

func TestPRStatus_NotFoundIsError(t *testing.T) {
	srv := prStatusServer(t, "/repos/acme/dbt-repo/pulls/7", http.StatusNotFound, `{"message":"Not Found"}`)
	defer srv.Close()
	g := NewSourceReader(srv.URL, "tkn", srv.Client())
	_, err := g.PRStatus(context.Background(), "acme/dbt-repo", 7)
	require.Error(t, err, "a vanished PR must surface as an error so the row stays open and is retried")
	require.NotErrorIs(t, err, ports.ErrPermissionDenied,
		"a 404 is a missing PR, not a permission problem, and must not be classified as one")
}

func TestPRStatus_ForbiddenIsPermissionDenied(t *testing.T) {
	srv := prStatusServer(t, "/repos/acme/dbt-repo/pulls/7", http.StatusForbidden,
		`{"message":"Resource not accessible by personal access token"}`)
	defer srv.Close()
	g := NewSourceReader(srv.URL, "tkn", srv.Client())
	_, err := g.PRStatus(context.Background(), "acme/dbt-repo", 7)
	require.ErrorIs(t, err, ports.ErrPermissionDenied,
		"a 403 means the token lacks Pull requests: Read and must be classified as a permission error")
}

func TestPRStatus_UnauthorizedIsPermissionDenied(t *testing.T) {
	srv := prStatusServer(t, "/repos/acme/dbt-repo/pulls/7", http.StatusUnauthorized,
		`{"message":"Bad credentials"}`)
	defer srv.Close()
	g := NewSourceReader(srv.URL, "tkn", srv.Client())
	_, err := g.PRStatus(context.Background(), "acme/dbt-repo", 7)
	require.ErrorIs(t, err, ports.ErrPermissionDenied,
		"a 401 means the token is missing or invalid and must be classified as a permission error")
}
