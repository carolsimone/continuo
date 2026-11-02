package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
)

type sourceKeyCtx struct{}

// withSourceKey returns ctx carrying the dedup suffix of the key a message was
// read from, for DedupMessageID.
func withSourceKey(ctx context.Context, suffix string) context.Context {
	return context.WithValue(ctx, sourceKeyCtx{}, suffix)
}

// DedupMessageID returns the identity a handler dedups msg under: the entry ID
// for a message read from the unprefixed (legacy) key, and the entry ID with
// "@" and the physical key appended for any other key.
//
// Two keys of one logical stream (the legacy key and the prefixed key, which a
// consumer reads side by side while a rolling upgrade lasts) number their
// entries independently, so entries written in the same millisecond can share
// an ID. Qualifying the ID by key keeps their dedup rows and markers apart,
// while a legacy entry keeps the identity an earlier release recorded for it.
// Outbox-entry-ID deduplication is unaffected: it does not use this value.
// Without a consumer in ctx, the entry ID is returned unchanged.
func DedupMessageID(ctx context.Context, msg goredis.XMessage) string {
	if suffix, ok := ctx.Value(sourceKeyCtx{}).(string); ok {
		return msg.ID + suffix
	}
	return msg.ID
}

// dedupSuffix is the DedupMessageID suffix of entries read from key.
func (c *StreamConsumer) dedupSuffix() string {
	if c.key == LegacyKey(c.streamName) {
		return ""
	}
	return "@" + c.key
}
