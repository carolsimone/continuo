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
// with "@" and the key it was read from appended, which keeps the dedup rows
// and markers of entries of different keys apart even when their IDs are
// equal. Outbox-entry-ID deduplication is unaffected: it does not use this
// value. Without a consumer in ctx, the entry ID is returned unchanged.
func DedupMessageID(ctx context.Context, msg goredis.XMessage) string {
	if suffix, ok := ctx.Value(sourceKeyCtx{}).(string); ok {
		return msg.ID + suffix
	}
	return msg.ID
}

// dedupSuffix is the DedupMessageID suffix of the entries the consumer reads.
func (c *StreamConsumer) dedupSuffix() string { return "@" + c.key }
