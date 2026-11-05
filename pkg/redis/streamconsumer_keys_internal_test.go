package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shortStartup is a startup policy that gives up on a missing group within a
// second, so a test of the wait does not take the default five minutes.
var shortStartup = StartupPolicy{Timeout: time.Second, InitialDelay: 100 * time.Millisecond, MaxDelay: 200 * time.Millisecond}

// keysTestStream returns a contract name unique to the test and deletes its
// key, and the unprefixed key named by the bare contract name, when the test
// ends.
func keysTestStream(t *testing.T, rc *goredis.Client, label string) string {
	t.Helper()
	stream := fmt.Sprintf("test-%s-%d", label, time.Now().UnixNano())
	t.Cleanup(func() { rc.Del(context.Background(), Key(stream), stream) })
	return stream
}

// startConsumer runs c.Start in the background and returns the channel its
// result arrives on.
func startConsumer(ctx context.Context, c *StreamConsumer) <-chan error {
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	return done
}

func TestConsumerNeverCreatesItsGroup(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	stream := keysTestStream(t, rc, "never-creates")
	group := "test-group"

	c := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error { return nil }, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup

	select {
	case err := <-startConsumer(ctx, c):
		require.Error(t, err)
		assert.Contains(t, err.Error(),
			fmt.Sprintf(`consumer group %q on %q does not exist; the streams-init step creates it`, group, Key(stream)))
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not give up on a missing group within its startup policy")
	}
	assert.Equal(t, int64(0), rc.Exists(ctx, Key(stream)).Val(), "Start must not create the prefixed key")
	assert.Equal(t, int64(0), rc.Exists(ctx, stream).Val(), "Start must not create the unprefixed key")
}

func TestConsumerWaitsForItsGroup(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := keysTestStream(t, rc, "waits")
	group := "test-group"

	seen := make(chan string, 1)
	c := NewStreamConsumer(rc, stream, group, func(_ context.Context, m goredis.XMessage) error {
		seen <- m.Values["k"].(string)
		return nil
	}, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = StartupPolicy{Timeout: 10 * time.Second, InitialDelay: 50 * time.Millisecond, MaxDelay: 100 * time.Millisecond}
	done := startConsumer(ctx, c)

	time.Sleep(300 * time.Millisecond)
	require.NoError(t, rc.XGroupCreateMkStream(ctx, Key(stream), group, "0").Err())
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: Key(stream), Values: map[string]any{"k": "late"}}).Err())

	select {
	case v := <-seen:
		assert.Equal(t, "late", v)
	case err := <-done:
		t.Fatalf("Start returned before the group appeared: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer never read the entry once its group appeared")
	}
	cancel()
	assert.NoError(t, <-done)
}

// The unprefixed key of a stream is never read, even when it holds the
// consumer's group: its entry stays undelivered and no consumer joins the group
// there.
func TestConsumerReadsOnlyThePrefixedKey(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := keysTestStream(t, rc, "prefixed-only")
	group := "test-group"
	for _, key := range []string{Key(stream), stream} {
		require.NoError(t, rc.XGroupCreateMkStream(ctx, key, group, "0").Err())
		require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: key, Values: map[string]any{"from": key}}).Err())
	}

	seen := make(chan string, 2)
	c := NewStreamConsumer(rc, stream, group, func(_ context.Context, m goredis.XMessage) error {
		seen <- m.Values["from"].(string)
		return nil
	}, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	done := startConsumer(ctx, c)

	select {
	case v := <-seen:
		assert.Equal(t, Key(stream), v)
	case err := <-done:
		t.Fatalf("Start returned instead of reading the prefixed key: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer never read the prefixed key")
	}
	select {
	case v := <-seen:
		t.Fatalf("the consumer read the entry of %q", v)
	case <-time.After(1500 * time.Millisecond):
	}
	groups, err := rc.XInfoGroups(ctx, stream).Result()
	require.NoError(t, err)
	require.Len(t, groups, 1)
	assert.Equal(t, "0-0", groups[0].LastDeliveredID, "nothing was delivered from the unprefixed key")
	assert.Zero(t, groups[0].Consumers, "no consumer joined the group on the unprefixed key")
	assert.NoError(t, c.Healthy(time.Minute))
	cancel()
	assert.NoError(t, <-done)
}

func TestConsumerStopsWhenItsGroupDisappears(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream := keysTestStream(t, rc, "group-gone")
	group := "test-group"
	require.NoError(t, rc.XGroupCreateMkStream(ctx, Key(stream), group, "0").Err())

	c := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error { return nil }, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	done := startConsumer(ctx, c)
	time.Sleep(300 * time.Millisecond)
	require.NoError(t, rc.XGroupDestroy(ctx, Key(stream), group).Err())

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), Key(stream))
		assert.Contains(t, err.Error(), group)
		assert.Contains(t, err.Error(), "NOGROUP")
	case <-time.After(10 * time.Second):
		t.Fatal("Start kept running after its group disappeared")
	}
	groups, err := rc.XInfoGroups(ctx, Key(stream)).Result()
	require.NoError(t, err)
	assert.Empty(t, groups, "the consumer does not re-create its group")
}

func TestConsumerDeadLettersToThePrefixedStream(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ensureDeadLetterStream(t, rc)
	stream := keysTestStream(t, rc, "dead-letter")
	group := "test-group"
	require.NoError(t, rc.XGroupCreateMkStream(ctx, Key(stream), group, "0").Err())
	msgID, err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: Key(stream), Values: map[string]any{"k": "v"}}).Result()
	require.NoError(t, err)

	c := NewStreamConsumer(rc, stream, group, func(context.Context, goredis.XMessage) error { return events.ErrPermanent }, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	done := startConsumer(ctx, c)

	require.Eventually(t, func() bool {
		dl, err := deadLettersIn(ctx, rc, stream)
		return err == nil && len(dl) == 1
	}, 5*time.Second, 20*time.Millisecond, "the dead letter lands on the prefixed dead-letter stream")
	dl := deadLettersFor(t, rc, stream)
	var p events.ConsumerDeadLetter
	require.NoError(t, json.Unmarshal([]byte(dl[0].Values["payload"].(string)), &p))
	assert.Equal(t, stream, p.OriginalStream, "the dead letter names the contract stream, not its key")
	assert.Equal(t, msgID, p.OriginalMessageID)

	legacy, err := rc.XRange(ctx, streams.ConsumerDeadLetterV1, "-", "+").Result()
	require.NoError(t, err)
	for _, e := range legacy {
		var lp events.ConsumerDeadLetter
		payload, _ := e.Values["payload"].(string)
		if json.Unmarshal([]byte(payload), &lp) == nil {
			assert.NotEqual(t, stream, lp.OriginalStream, "no dead letter lands on the unprefixed stream")
		}
	}
	cancel()
	assert.NoError(t, <-done)
}

func TestXaddDeadLetter_NeverCreatesTheDeadLetterStream(t *testing.T) {
	rc := internalRedisClient(t)
	ctx := context.Background()
	dlq := Key(streams.ConsumerDeadLetterV1)
	// The dead-letter stream is shared by the suite: move it aside for the
	// test and put it back afterwards.
	if rc.Exists(ctx, dlq).Val() != 0 {
		aside := fmt.Sprintf("test-dead-letter-aside-%d", time.Now().UnixNano())
		require.NoError(t, rc.Rename(ctx, dlq, aside).Err())
		t.Cleanup(func() { rc.Rename(context.Background(), aside, dlq) })
	}
	c := NewStreamConsumer(rc, "s", "g", nil, discardLog())
	err := c.xaddDeadLetter(ctx, map[string]any{"k": "v"})
	require.Error(t, err, "a dead letter to a missing stream fails rather than creating it")
	assert.Contains(t, err.Error(), dlq)
	assert.Equal(t, int64(0), rc.Exists(ctx, dlq).Val())
}

func TestDedupMessageIDKeepsTheEntryIDWithoutAConsumer(t *testing.T) {
	assert.Equal(t, "5-0", DedupMessageID(context.Background(), goredis.XMessage{ID: "5-0"}))
}

// A handler dedups an entry under its ID qualified by the key it was read
// from, so the identity an entry was recorded under stays the same across
// releases.
func TestConsumerQualifiesTheDedupIdentityByItsKey(t *testing.T) {
	rc := internalRedisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := keysTestStream(t, rc, "dedup-id")
	group := "test-group"
	const entryID = "1700000000000-0"
	require.NoError(t, rc.XGroupCreateMkStream(ctx, Key(stream), group, "0").Err())
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: Key(stream), ID: entryID, Values: map[string]any{"k": "v"}}).Err())

	identity := make(chan string, 1)
	c := NewStreamConsumer(rc, stream, group, func(ctx context.Context, m goredis.XMessage) error {
		assert.Equal(t, entryID, m.ID, "the entry keeps its own ID, which acknowledgement uses")
		identity <- DedupMessageID(ctx, m)
		return nil
	}, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	done := startConsumer(ctx, c)

	select {
	case id := <-identity:
		assert.Equal(t, entryID+"@"+Key(stream), id)
	case err := <-done:
		t.Fatalf("Start returned before the handler ran: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never ran")
	}
	cancel()
	assert.NoError(t, <-done)
}

// A dead letter names the key its entry was read from and carries the identity
// the handler deduplicated the entry under.
func TestConsumerDeadLetterCarriesTheHandlersDedupIdentity(t *testing.T) {
	rc := internalRedisClient(t)
	ensureDeadLetterStream(t, rc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := keysTestStream(t, rc, "dead-letter-identity")
	group := "test-group"
	const entryID = "1700000000000-0"
	require.NoError(t, rc.XGroupCreateMkStream(ctx, Key(stream), group, "0").Err())
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: Key(stream), ID: entryID, Values: map[string]any{"k": "v"}}).Err())

	identity := make(chan string, 1)
	c := NewStreamConsumer(rc, stream, group, func(ctx context.Context, m goredis.XMessage) error {
		identity <- DedupMessageID(ctx, m)
		return fmt.Errorf("%w: the entry fails", events.ErrPermanent)
	}, discardLog())
	c.SetService("pkg-redis-test")
	c.startup = shortStartup
	done := startConsumer(ctx, c)

	require.Eventually(t, func() bool {
		dl, err := deadLettersIn(ctx, rc, stream)
		return err == nil && len(dl) == 1
	}, 5*time.Second, 20*time.Millisecond, "the entry is dead-lettered")
	var p events.ConsumerDeadLetter
	require.NoError(t, json.Unmarshal([]byte(deadLettersFor(t, rc, stream)[0].Values["payload"].(string)), &p))
	assert.Equal(t, Key(stream), p.OriginalKey, "the dead letter names the key its entry was read from")
	assert.Equal(t, entryID, p.OriginalMessageID, "the dead letter names the entry by its own ID")
	assert.Equal(t, <-identity, p.DedupMessageID(), "the dead letter's identity is the one its handler deduplicated under")
	cancel()
	assert.NoError(t, <-done)
}
