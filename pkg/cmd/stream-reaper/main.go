// Command stream-reaper trims continuo event streams by age.
//
// On each run it computes a cutoff of now-retention and, for every stream in
// streams.All, runs XTRIM <stream> MINID <cutoff> — dropping entries older than
// the retention window. It operates only on the explicit continuo stream list,
// so unrelated keys on the same Redis instance — notably the ui-service auth
// session keys, which are plain strings, not streams — are never touched.
//
// Intended to run as a periodic (daily) Kubernetes CronJob. Config via env:
//   REDIS_URL          required; e.g. redis://:pass@host:6379
//   STREAM_RETENTION   optional Go duration (default 72h); entries older than
//                      this are removed.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
)

const defaultRetention = 72 * time.Hour

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("stream-reaper failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		return fmt.Errorf("REDIS_URL is required")
	}

	retention := defaultRetention
	if v := os.Getenv("STREAM_RETENTION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid STREAM_RETENTION %q: %w", v, err)
		}
		if d <= 0 {
			return fmt.Errorf("STREAM_RETENTION must be positive, got %q", v)
		}
		retention = d
	}

	opt, err := goredis.ParseURL(url)
	if err != nil {
		return fmt.Errorf("parse REDIS_URL: %w", err)
	}
	rdb := goredis.NewClient(opt)
	defer func() {
		if closeErr := rdb.Close(); closeErr != nil {
			logger.Error("close redis client", "error", closeErr)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cutoff := time.Now().Add(-retention)
	// Redis stream entry IDs are "<unix-ms>-<seq>"; MINID drops every entry with
	// a smaller id, i.e. everything created before the cutoff.
	minID := fmt.Sprintf("%d-0", cutoff.UnixMilli())

	logger.Info("stream-reaper start",
		"retention", retention.String(),
		"cutoff", cutoff.UTC().Format(time.RFC3339),
		"streams", len(streams.All))

	var totalRemoved int64
	var failures int
	for _, s := range streams.All {
		// Approximate trim (~) is cheaper than exact and is fine for age-based
		// cleanup; a few extra retained entries per stream are harmless.
		removed, err := rdb.XTrimMinIDApprox(ctx, s, minID, 0).Result()
		if err != nil {
			logger.Error("xtrim failed", "stream", s, "error", err)
			failures++
			continue
		}
		if removed > 0 {
			logger.Info("trimmed", "stream", s, "removed", removed)
		}
		totalRemoved += removed
	}

	logger.Info("stream-reaper done",
		"total_removed", totalRemoved,
		"failures", failures,
		"streams", len(streams.All))

	if failures > 0 {
		return fmt.Errorf("%d stream(s) failed to trim", failures)
	}
	return nil
}
