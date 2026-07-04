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
	return prStatusServerH(t, wantPath, status, nil, body)
}

// prStatusServerH is prStatusServer with response headers, used to simulate
// GitHub's rate-limit signals on a 403/429.
func prStatusServerH(t *testing.T, wantPath string, status int, headers map[string]string, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, wantPath, r.URL.Path)
		require.Equal(t, "Bearer tkn", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		for k, v := range headers {
			w.Header().Set(k, v)
		}
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
	// A genuine permission 403 still has rate-limit budget remaining.
	srv := prStatusServerH(t, "/repos/acme/dbt-repo/pulls/7", http.StatusForbidden,
		map[string]string{"X-RateLimit-Remaining": "4999"},
		`{"message":"Resource not accessible by personal access token"}`)
	defer srv.Close()
	g := NewSourceReader(srv.URL, "tkn", srv.Client())
	_, err := g.PRStatus(context.Background(), "acme/dbt-repo", 7)
	require.ErrorIs(t, err, ports.ErrPermissionDenied,
		"a 403 with rate-limit budget remaining means the token lacks Pull requests: Read")
}

func TestPRStatus_ForbiddenPrimaryRateLimitIsTransient(t *testing.T) {
	// GitHub signals primary rate-limit exhaustion with a 403 and
	// X-RateLimit-Remaining: 0 — a transient throttle, not a permission gap.
	srv := prStatusServerH(t, "/repos/acme/dbt-repo/pulls/7", http.StatusForbidden,
		map[string]string{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1700000000"},
		`{"message":"API rate limit exceeded"}`)
	defer srv.Close()
	g := NewSourceReader(srv.URL, "tkn", srv.Client())
	_, err := g.PRStatus(context.Background(), "acme/dbt-repo", 7)
	require.Error(t, err)
	require.NotErrorIs(t, err, ports.ErrPermissionDenied,
		"a rate-limited 403 is transient and must not surface as a standing permission failure")
}

func TestPRStatus_ForbiddenSecondaryRateLimitIsTransient(t *testing.T) {
	// GitHub signals secondary/abuse rate limits with a Retry-After header.
	srv := prStatusServerH(t, "/repos/acme/dbt-repo/pulls/7", http.StatusForbidden,
		map[string]string{"Retry-After": "60"},
		`{"message":"You have exceeded a secondary rate limit"}`)
	defer srv.Close()
	g := NewSourceReader(srv.URL, "tkn", srv.Client())
	_, err := g.PRStatus(context.Background(), "acme/dbt-repo", 7)
	require.Error(t, err)
	require.NotErrorIs(t, err, ports.ErrPermissionDenied,
		"a secondary-rate-limited 403 is transient and must not surface as a permission failure")
}

func TestPRStatus_TooManyRequestsIsTransient(t *testing.T) {
	srv := prStatusServerH(t, "/repos/acme/dbt-repo/pulls/7", http.StatusTooManyRequests,
		map[string]string{"Retry-After": "60"}, `{"message":"rate limited"}`)
	defer srv.Close()
	g := NewSourceReader(srv.URL, "tkn", srv.Client())
	_, err := g.PRStatus(context.Background(), "acme/dbt-repo", 7)
	require.Error(t, err)
	require.NotErrorIs(t, err, ports.ErrPermissionDenied,
		"a 429 is a rate limit, not a permission failure")
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
