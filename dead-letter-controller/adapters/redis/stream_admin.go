package redis

import (
	"context"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/trim"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/unprefixed"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// placeholderGroup is the group EnsureStream creates to make Redis create a
// stream, and destroys at once.
const placeholderGroup = "streams-init"

// StreamAdmin creates streams and consumer groups in Redis, and deletes the
// unprefixed keys of streams. It stores each stream under pkgredis.Key of its
// contract name; the unprefixed key of a stream is the contract name itself.
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

// CheckLogin opens a client of its own to the admin's Redis that signs in as
// username with password, sends PING and closes it. A NOPERM answer to the
// PING still means the sign-in succeeded: the login's rule only refuses the
// command.
func (a *StreamAdmin) CheckLogin(ctx context.Context, username, password string) error {
	base := a.rdb.Options()
	client := goredis.NewClient(&goredis.Options{
		Network:      base.Network,
		Addr:         base.Addr,
		Dialer:       base.Dialer,
		TLSConfig:    base.TLSConfig,
		Protocol:     base.Protocol,
		DialTimeout:  base.DialTimeout,
		ReadTimeout:  base.ReadTimeout,
		WriteTimeout: base.WriteTimeout,
		Username:     username,
		Password:     password,
		PoolSize:     1,
		MaxRetries:   -1,
	})
	defer func() { _ = client.Close() }()
	err := client.Ping(ctx).Err()
	if err != nil && strings.HasPrefix(err.Error(), "NOPERM") {
		return nil
	}
	return err
}

// StreamExists runs EXISTS on the key of the contract stream name.
func (a *StreamAdmin) StreamExists(ctx context.Context, stream string) (bool, error) {
	n, err := a.rdb.Exists(ctx, pkgredis.Key(stream)).Result()
	if err != nil {
		return false, fmt.Errorf("exists %s: %w", stream, err)
	}
	return n > 0, nil
}

// ReadUnprefixed reads the unprefixed key of stream with XINFO GROUPS and
// XINFO STREAM.
func (a *StreamAdmin) ReadUnprefixed(ctx context.Context, stream string) (unprefixed.Key, bool, error) {
	infos, err := a.rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return unprefixed.Key{}, false, nil
		}
		return unprefixed.Key{}, false, fmt.Errorf("xinfo groups %s: %w", stream, err)
	}
	var k unprefixed.Key
	for _, info := range infos {
		last, err := trim.ParseID(info.LastDeliveredID)
		if err != nil {
			return unprefixed.Key{}, false, fmt.Errorf("group %s of %s: %w", info.Name, stream, err)
		}
		k.Groups = append(k.Groups, unprefixed.Group{Name: info.Name, Pending: info.Pending, LastDelivered: last})
	}
	info, err := a.rdb.XInfoStream(ctx, stream).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return unprefixed.Key{}, false, nil
		}
		return unprefixed.Key{}, false, fmt.Errorf("xinfo stream %s: %w", stream, err)
	}
	if info.Length > 0 {
		last, err := trim.ParseID(info.LastEntry.ID)
		if err != nil {
			return unprefixed.Key{}, false, fmt.Errorf("last entry of %s: %w", stream, err)
		}
		k.Last = &last
	}
	return k, true, nil
}

// DeleteUnprefixed runs DEL on the unprefixed key of stream.
func (a *StreamAdmin) DeleteUnprefixed(ctx context.Context, stream string) (bool, error) {
	n, err := a.rdb.Del(ctx, stream).Result()
	if err != nil {
		return false, fmt.Errorf("del %s: %w", stream, err)
	}
	return n == 1, nil
}

func isBusyGroup(err error) bool { return strings.HasPrefix(err.Error(), "BUSYGROUP") }
