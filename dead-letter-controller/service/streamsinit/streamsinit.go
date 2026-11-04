// Package streamsinit creates every Redis stream and consumer group the
// release declares, so that no consumer or producer ever creates one.
package streamsinit

import (
	"context"
	"errors"
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
	// Logins are the Redis logins Run signs in as before it creates anything;
	// nil checks no login.
	Logins ports.RedisLogins
}

// Run first signs in as every login of plan.Logins and fails, before it
// creates anything, when any login is refused, listing each refused login.
// The logins are checked whether or not creation is turned on. It then creates
// each stream of plan, and each of its groups, naming the stream by its
// contract name; the admin maps the name to the key it stores it under. It
// writes one log line per stream with the number of groups it created (or 1 for
// a stream with no group that it created), and returns the first error with the
// stream, and the group, named.
func Run(ctx context.Context, admin ports.StreamAdmin, plan Plan, logger *slog.Logger) error {
	if err := checkLogins(ctx, admin, plan.Logins, logger); err != nil {
		return err
	}
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

// checkLogins signs in as each login of source, in username order, and
// returns one error listing every login that could not sign in: a password it
// could not read, or a sign-in Redis refused. A nil source checks nothing.
func checkLogins(ctx context.Context, admin ports.StreamAdmin, source ports.RedisLogins, logger *slog.Logger) error {
	if source == nil {
		return nil
	}
	logins, err := source.Logins(ctx)
	if err != nil {
		return err
	}
	var failures []error
	for _, login := range logins {
		err := login.ReadErr
		if err == nil {
			err = admin.CheckLogin(ctx, login.Username, login.Password)
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("redis login %s: %w; create it as described in deploy/CREDENTIALS.md", login.Username, err))
			continue
		}
		logger.Info("redis login signs in", "login", login.Username)
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	logger.Info("every redis login signs in", "logins", len(logins))
	return nil
}
