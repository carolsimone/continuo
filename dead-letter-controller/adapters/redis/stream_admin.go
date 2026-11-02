package redis

import (
	"context"
	"strings"

	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// placeholderGroup is the group EnsureStream creates to make Redis create a
// stream, and destroys at once.
const placeholderGroup = "streams-init"

// StreamAdmin creates streams and consumer groups in Redis. It stores each
// stream under pkgredis.Key of its contract name.
type StreamAdmin struct{ rdb *goredis.Client }

var _ ports.StreamAdmin = (*StreamAdmin)(nil)

// NewStreamAdmin returns an admin over rdb.
func NewStreamAdmin(rdb *goredis.Client) *StreamAdmin { return &StreamAdmin{rdb: rdb} }

// EnsureGroup runs XGROUP CREATE <key> group 0 MKSTREAM on the key of the
// contract stream name, so a new group reads the stream from its first entry. A
// group that exists (BUSYGROUP) is left as it is.
func (a *StreamAdmin) EnsureGroup(ctx context.Context, stream, group string) (bool, error) {
	err := a.rdb.XGroupCreateMkStream(ctx, pkgredis.Key(stream), group, "0").Err()
	if err != nil {
		if isBusyGroup(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// EnsureStream creates the stream at the key of the contract stream name by
// creating the placeholder group with MKSTREAM and destroying it, which leaves
// an empty stream with no group. A stream that exists is not touched.
func (a *StreamAdmin) EnsureStream(ctx context.Context, stream string) (bool, error) {
	key := pkgredis.Key(stream)
	exists, err := a.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if exists > 0 {
		return false, nil
	}
	created, err := a.EnsureGroup(ctx, stream, placeholderGroup)
	if err != nil {
		return false, err
	}
	if !created {
		// A concurrent run created the stream and owns removing the placeholder.
		return false, nil
	}
	if err := a.rdb.XGroupDestroy(ctx, key, placeholderGroup).Err(); err != nil {
		return false, err
	}
	return true, nil
}

func isBusyGroup(err error) bool { return strings.HasPrefix(err.Error(), "BUSYGROUP") }
