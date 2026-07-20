package liveness

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandler_OKWhenNoFailures: an empty failure set yields 200 and the body
// "<kind> OK", with nothing logged.
func TestHandler_OKWhenNoFailures(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	h := Handler("readiness", func(context.Context) []Failure { return nil }, logger)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if got := rec.Body.String(); got != "readiness OK" {
		t.Fatalf("expected body %q, got %q", "readiness OK", got)
	}
	if logs.Len() != 0 {
		t.Fatalf("expected no logs on the healthy path, got %q", logs.String())
	}
}

// TestHandler_ServiceUnavailableWhenFailures: a non-empty failure set yields
// 503, the body "NOT <kind>: N component(s) unhealthy", and one Warn log per
// failing component.
func TestHandler_ServiceUnavailableWhenFailures(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	check := func(context.Context) []Failure {
		return []Failure{
			{Name: "redis", Err: errors.New("connection refused")},
			{Name: "consumer_a_heartbeat", Err: errors.New("read loop stalled")},
		}
	}
	h := Handler("liveness", check, logger)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
	if got, want := rec.Body.String(), "NOT liveness: 2 component(s) unhealthy"; got != want {
		t.Fatalf("expected body %q, got %q", want, got)
	}
	out := logs.String()
	if strings.Count(out, "Health check failed") != 2 {
		t.Fatalf("expected 2 per-failure Warn logs, got: %q", out)
	}
	for _, name := range []string{"redis", "consumer_a_heartbeat"} {
		if !strings.Contains(out, name) {
			t.Fatalf("expected failing component %q to be logged, got: %q", name, out)
		}
	}
}
