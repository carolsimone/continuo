package redis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// legacyStream creates the consumer group on both keys of a contract name
// unique to the test, so a consumer starts with one loop per key.
func legacyStream(t *testing.T, rc *goredis.Client, label, group string) string {
	t.Helper()
	stream := keysTestStream(t, rc, label)
	for _, key := range []string{Key(stream), LegacyKey(stream)} {
		require.NoError(t, rc.XGroupCreateMkStream(context.Background(), key, group, "0").Err())
	}
	return stream
}

// aclUser creates a Redis login that may run every command on keys and returns
// a client signed in as it; the login is deleted when the test ends.
func aclUser(t *testing.T, rc *goredis.Client, keys ...string) (*goredis.Client, string) {
	t.Helper()
	user := "pkg-redis-legacy-test-" + uuid.NewString()
	const password = "legacy-test-password"
	args := []any{"ACL", "SETUSER", user, "reset", "on", ">" + password, "+@all"}
	for _, k := range keys {
		args = append(args, "~"+k)
	}
	require.NoError(t, rc.Do(context.Background(), args...).Err())
	t.Cleanup(func() { rc.Do(context.Background(), "ACL", "DELUSER", user) })
	c := goredis.NewClient(&goredis.Options{Addr: os.Getenv("REDIS_ADDR"), Username: user, Password: password})
	t.Cleanup(func() { _ = c.Close() })
	return c, user
}

// readsPrefixedEntry adds an entry to the prefixed key and waits for the
// handler to see it while Start keeps running.
func readsPrefixedEntry(t *testing.T, rc *goredis.Client, stream string, seen <-chan string, done <-chan error, value string) {
	t.Helper()
	require.NoError(t, rc.XAdd(context.Background(), &goredis.XAddArgs{Stream: Key(stream), Values: map[string]any{"k": value}}).Err())
	for {
		select {
		case v := <-seen:
			if v == value {
				return
			}
		case err := <-done:
			t.Fatalf("Start returned instead of reading the prefixed key: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatalf("the consumer never read %q from the prefixed key", value)
		}
	}
}

// legacyLoopEnds waits for the loop of the unprefixed key to end.
func legacyLoopEnds(t *testing.T, c *StreamConsumer) {
	t.Helper()
	require.Eventually(t, func() bool {
		loops := c.keyLoops.Load()
		return loops != nil && len(*loops) == 1 && (*loops)[0].ended.Load()
	}, 10*time.Second, 50*time.Millisecond, "the loop of the unprefixed key did not end")
}

func seenConsumer(rc *goredis.Client, stream, group string) (*StreamConsumer, chan string) {
	seen := make(chan string, 16)
	c := NewStreamConsumer(rc, stream, group, func(_ context.Context, m goredis.XMessage) error {
		seen <- fmt.Sprint(m.Values["k"])
		return nil
	}, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	return c, seen
}

// Deleting the unprefixed key, as the release that stops reading it does once
// it is drained, ends only that key's loop: the consumer keeps reading the
// prefixed key and stays healthy.
func TestConsumerKeepsReadingThePrefixedKeyWhenTheLegacyKeyIsDeleted(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group := "test-group"
	stream := legacyStream(t, rc, "legacy-deleted", group)
	c, seen := seenConsumer(rc, stream, group)
	done := startConsumer(ctx, c)
	readsPrefixedEntry(t, rc, stream, seen, done, "before")

	require.NoError(t, rc.Del(ctx, LegacyKey(stream)).Err())
	legacyLoopEnds(t, c)
	readsPrefixedEntry(t, rc, stream, seen, done, "after")

	assert.NoError(t, c.Healthy(time.Minute))
	assert.Equal(t, int64(0), rc.Exists(ctx, LegacyKey(stream)).Val(), "the consumer never re-creates the legacy key")
	cancel()
	assert.NoError(t, <-done)
}

// A login whose rule stops reaching the unprefixed key while the consumer runs
// (the next release's rules applied early) ends only that key's loop.
func TestConsumerKeepsReadingThePrefixedKeyWhenTheLegacyKeyIsRefused(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group := "test-group"
	stream := legacyStream(t, rc, "legacy-refused", group)
	client, user := aclUser(t, rc, Key(stream), LegacyKey(stream))
	c, seen := seenConsumer(client, stream, group)
	done := startConsumer(ctx, c)
	readsPrefixedEntry(t, rc, stream, seen, done, "before")

	require.NoError(t, rc.Do(ctx, "ACL", "SETUSER", user, "resetkeys", "~"+Key(stream)).Err())
	legacyLoopEnds(t, c)
	readsPrefixedEntry(t, rc, stream, seen, done, "after")

	assert.NoError(t, c.Healthy(time.Minute))
	cancel()
	assert.NoError(t, <-done)
}

// A consumer whose login may not read the unprefixed key starts on the
// prefixed key alone instead of waiting for a group it can never see.
func TestConsumerStartsWithoutALegacyKeyItsLoginMayNotRead(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group := "test-group"
	stream := legacyStream(t, rc, "legacy-never-readable", group)
	client, _ := aclUser(t, rc, Key(stream))
	c, seen := seenConsumer(client, stream, group)
	done := startConsumer(ctx, c)
	readsPrefixedEntry(t, rc, stream, seen, done, "only")
	cancel()
	assert.NoError(t, <-done)
}

// A loop that ended because its unprefixed key went away no longer counts
// towards the consumer's health.
func TestHealthy_SkipsAKeyLoopThatEnded(t *testing.T) {
	c := NewStreamConsumer(nil, "orders:v1", "svc-orders", nil, discardLog())
	l := c.onKey(LegacyKey("orders:v1"))
	c.keyLoops.Store(&[]*StreamConsumer{l})
	l.lastActivity.Store(time.Now().Add(-time.Hour).UnixNano())
	l.ended.Store(true)
	assert.NoError(t, c.Healthy(time.Minute))
}
