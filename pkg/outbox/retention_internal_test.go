package outbox

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestPruneTarget_HonoursMinRetention(t *testing.T) {
	var got []time.Duration
	s := NewRetentionSweeper([]RetentionTarget{
		{Name: "outbox", Prune: func(_ context.Context, r time.Duration, _ int) (int64, error) { got = append(got, r); return 0, nil }},
		{Name: "dedup", MinRetention: 30 * 24 * time.Hour, Prune: func(_ context.Context, r time.Duration, _ int) (int64, error) { got = append(got, r); return 0, nil }},
	}, RetentionConfig{Retention: 7 * 24 * time.Hour}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.sweep(context.Background())
	if len(got) != 2 || got[0] != 7*24*time.Hour || got[1] != 30*24*time.Hour {
		t.Fatalf("retentions = %v", got)
	}
}

func TestPruneTarget_LongerConfiguredRetentionWinsOverMinRetention(t *testing.T) {
	var got time.Duration
	s := NewRetentionSweeper([]RetentionTarget{
		{Name: "dedup", MinRetention: 30 * 24 * time.Hour, Prune: func(_ context.Context, r time.Duration, _ int) (int64, error) { got = r; return 0, nil }},
	}, RetentionConfig{Retention: 90 * 24 * time.Hour}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.sweep(context.Background())
	if got != 90*24*time.Hour {
		t.Fatalf("retention = %v, want 90 days", got)
	}
}
