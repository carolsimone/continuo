// Package streamsinit creates every Redis stream and consumer group the
// release declares, so that no consumer or producer ever creates one, and
// deletes the unprefixed keys of the streams that no consumer reads.
package streamsinit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/unprefixed"
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
	// DeleteUnprefixedKeys turns on deleting the unprefixed keys of the
	// streams once creation is done: the key of each stream of Groups once its
	// groups have finished with it, and the key of each stream of Retired.
	DeleteUnprefixedKeys bool
	// Retired lists the streams removed from the contract. Nothing reads or
	// writes them.
	Retired []string
}

// Run first signs in as every login of plan.Logins and fails, before it
// creates anything, when any login is refused, listing each refused login.
// The logins are checked whether or not creation is turned on. It then creates
// each stream of plan, and each of its groups, naming the stream by its
// contract name; the admin maps the name to the key it stores it under. It
// writes one log line per stream with the number of groups it created (or 1 for
// a stream with no group that it created), and returns the first error with the
// stream, and the group, named.
//
// When plan.DeleteUnprefixedKeys is set, it first refuses a Redis a release
// before the key prefix last used (see refuseUnprefixedOnly), and once every
// stream exists it deletes the unprefixed keys (see deleteUnprefixedKeys).
// With creation turned off it does neither.
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

	if plan.DeleteUnprefixedKeys {
		if err := refuseUnprefixedOnly(ctx, admin, names, plan, logger); err != nil {
			return err
		}
	}

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
	if plan.DeleteUnprefixedKeys {
		return deleteUnprefixedKeys(ctx, admin, names, plan, logger)
	}
	return nil
}

// refuseUnprefixedOnly fails, before anything is created, when a stream of
// names has no key under the prefix while its unprefixed key holds one of the
// stream's contract groups. Redis then holds the streams of a release before
// the key prefix, which this release never reads: the release that added the
// prefix reads both keys and must run first. An unprefixed key it cannot read
// (a login whose rule predates this release answers NOPERM) is logged and not
// counted.
func refuseUnprefixedOnly(ctx context.Context, admin ports.StreamAdmin, names []string, plan Plan, logger *slog.Logger) error {
	var found []string
	for _, stream := range names {
		exists, err := admin.StreamExists(ctx, stream)
		if err != nil {
			return fmt.Errorf("check stream %s: %w", stream, err)
		}
		if exists {
			continue
		}
		k, present, err := admin.ReadUnprefixed(ctx, stream)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Warn("unprefixed key not read", "stream", stream, "error", err)
			continue
		}
		if present && unprefixed.HoldsGroupOf(k, plan.Groups[stream]) {
			found = append(found, stream)
		}
	}
	if len(found) > 0 {
		return fmt.Errorf("redis holds streams under their names from before the continuo: key prefix (%s) and not under the prefix: "+
			"upgrade through the release that added the continuo: key prefix first and let it run until its services have read them, since this release does not read them",
			strings.Join(found, ", "))
	}
	return nil
}

// deleteUnprefixedKeys deletes the unprefixed key of each stream of names
// whose contract groups have all finished with it (see unprefixed.Held), and
// the unprefixed key of each retired stream whatever it holds: nothing reads
// it. A key a contract group has not finished with is kept and logged, and
// once every other key is dealt with the run fails, naming each such key and
// its groups: no consumer of this release reads those entries. A key it cannot
// read or delete (a login whose rule predates this release answers NOPERM) is
// logged and left, and does not fail the run. A cancelled ctx ends it with
// ctx's error.
func deleteUnprefixedKeys(ctx context.Context, admin ports.StreamAdmin, names []string, plan Plan, logger *slog.Logger) error {
	const hint = "apply this release's rule of continuo_redis_migrate from deploy/redis-acl-rules.acl, or delete the key yourself"
	var kept []string
	for _, stream := range names {
		k, exists, err := admin.ReadUnprefixed(ctx, stream)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Warn("unprefixed key not read, kept", "stream", stream, "error", err, "hint", hint)
			continue
		}
		if !exists {
			continue
		}
		if held := unprefixed.Held(k, plan.Groups[stream]); len(held) > 0 {
			logger.Warn("unprefixed key kept", "stream", stream, "held_by", held)
			kept = append(kept, fmt.Sprintf("%s (%s)", stream, strings.Join(held, "; ")))
			continue
		}
		if err := deleteUnprefixed(ctx, admin, stream, hint, logger); err != nil {
			return err
		}
	}
	for _, stream := range plan.Retired {
		if err := deleteUnprefixed(ctx, admin, stream, hint, logger); err != nil {
			return err
		}
	}
	if len(kept) > 0 {
		return fmt.Errorf("unprefixed stream keys hold entries no consumer of this release reads: %s; "+
			"let the release that reads them finish and deploy again, or discard the entries with DEL <key>",
			strings.Join(kept, ", "))
	}
	return nil
}

// deleteUnprefixed deletes the unprefixed key of stream and logs what it did.
// It returns only ctx's error: a key it cannot delete is logged and left.
func deleteUnprefixed(ctx context.Context, admin ports.StreamAdmin, stream, hint string, logger *slog.Logger) error {
	deleted, err := admin.DeleteUnprefixed(ctx, stream)
	switch {
	case err != nil && ctx.Err() != nil:
		return ctx.Err()
	case err != nil:
		logger.Warn("unprefixed key not deleted", "stream", stream, "error", err, "hint", hint)
	case deleted:
		logger.Info("unprefixed key deleted", "stream", stream)
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
