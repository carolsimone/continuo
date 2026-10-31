package retention

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestPruneTarget_HonoursMinRetention(t *testing.T) {
	var got []time.Duration
	s := NewSweeper([]Target{
		{Name: "outbox", Prune: func(_ context.Context, r time.Duration, _ int) (int64, error) { got = append(got, r); return 0, nil }},
		{Name: "dedup", MinRetention: 30 * 24 * time.Hour, Prune: func(_ context.Context, r time.Duration, _ int) (int64, error) { got = append(got, r); return 0, nil }},
	}, Config{Retention: 7 * 24 * time.Hour}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.sweep(context.Background())
	if len(got) != 2 || got[0] != 7*24*time.Hour || got[1] != 30*24*time.Hour {
		t.Fatalf("retentions = %v", got)
	}
}

func TestPruneTarget_LongerConfiguredRetentionWinsOverMinRetention(t *testing.T) {
	var got time.Duration
	s := NewSweeper([]Target{
		{Name: "dedup", MinRetention: 30 * 24 * time.Hour, Prune: func(_ context.Context, r time.Duration, _ int) (int64, error) { got = r; return 0, nil }},
	}, Config{Retention: 90 * 24 * time.Hour}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.sweep(context.Background())
	if got != 90*24*time.Hour {
		t.Fatalf("retention = %v, want 90 days", got)
	}
}

// A pass that deletes rows and then fails keeps its count: the sweep logs the
// failure with what was deleted, and goes on to the next target.
func TestSweep_AFailedPassKeepsItsCountAndTheSweepGoesOn(t *testing.T) {
	var logs bytes.Buffer
	nextRan := false
	s := NewSweeper([]Target{
		{Name: "failing", Prune: func(context.Context, time.Duration, int) (int64, error) {
			return 4, errors.New("one partition is unreachable")
		}},
		{Name: "next", Prune: func(context.Context, time.Duration, int) (int64, error) { nextRan = true; return 0, nil }},
	}, Config{}, slog.New(slog.NewTextHandler(&logs, nil)))

	n, err := s.pruneTarget(context.Background(), s.targets[0])
	if n != 4 || err == nil {
		t.Fatalf("pruneTarget = %d, %v, want 4 and the failure", n, err)
	}
	s.sweep(context.Background())
	if !nextRan {
		t.Error("the sweep stopped at the failing target")
	}
	if out := logs.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "count=4") {
		t.Errorf("the failure is not logged with its count:\n%s", out)
	}
}

// A sweep a shutdown cancels logs no error and prunes no further target.
func TestSweep_ACancelledSweepLogsNoError(t *testing.T) {
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	nextRan := false
	s := NewSweeper([]Target{
		{Name: "cancelled", Prune: func(ctx context.Context, _ time.Duration, _ int) (int64, error) {
			cancel()
			return 1, ctx.Err()
		}},
		{Name: "next", Prune: func(context.Context, time.Duration, int) (int64, error) { nextRan = true; return 0, nil }},
	}, Config{}, slog.New(slog.NewTextHandler(&logs, nil)))

	s.sweep(ctx)
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a cancelled sweep logged an error:\n%s", logs.String())
	}
	if nextRan {
		t.Error("the sweep went on after its context was cancelled")
	}
}
