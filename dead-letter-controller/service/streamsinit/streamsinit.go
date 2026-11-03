// Package streamsinit creates every Redis stream and consumer group the
// release declares, so that no consumer or producer ever creates one.
package streamsinit

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
)

// Plan is what Run creates.
type Plan struct {
	// Groups maps each stream to the consumer groups declared on it. A stream
	// with no group is still created, because a producer adds to it without
	// creating it.
	Groups map[string][]string
	// CreateStreams turns creation on. When false, Run creates nothing: the
	// streams and groups come from outside the release.
	CreateStreams bool
}

// Run creates each stream of plan, and each of its groups, naming the stream by
// its contract name; the admin maps the name to the key it stores it under. It
// writes one log line per stream with the number of groups it created (or 1 for
// a stream with no group that it created), and returns the first error with the
// stream, and the group, named.
func Run(ctx context.Context, admin ports.StreamAdmin, plan Plan, logger *slog.Logger) error {
	if !plan.CreateStreams {
		logger.Info("creating streams and groups is turned off (REDIS_INIT_CREATE_STREAMS=false), nothing created")
		return nil
	}

	names := make([]string, 0, len(plan.Groups))
	for stream := range plan.Groups {
		names = append(names, stream)
	}
	sort.Strings(names)

	for _, stream := range names {
		groups := plan.Groups[stream]
		created := 0
		if len(groups) == 0 {
			ok, err := admin.EnsureStream(ctx, stream)
			if err != nil {
				return fmt.Errorf("create stream %s: %w", stream, err)
			}
			if ok {
				created++
			}
		}
		for _, group := range groups {
			ok, err := admin.EnsureGroup(ctx, stream, group)
			if err != nil {
				return fmt.Errorf("create group %s of stream %s: %w", group, stream, err)
			}
			if ok {
				created++
			}
		}
		logger.Info("stream ready", "stream", stream, "groups", len(groups), "created", created)
	}
	return nil
}
