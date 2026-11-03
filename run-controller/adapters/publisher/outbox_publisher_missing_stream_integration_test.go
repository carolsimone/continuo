//go:build integration

package publisher

import (
	"context"
	"os"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestPublisherRefusesAMissingStream publishes against the compose Redis: a
// stream that does not exist under its prefixed key is not created, Publish and
// PublishBatch return the "does not exist" error so the outbox keeps the row,
// and once the streams-init step has created the stream the entry lands on the
// prefixed key.
func TestPublisherRefusesAMissingStream(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		testdeps.Unavailable(t, "REDIS_ADDR not set — skipping Redis integration test")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	t.Cleanup(func() { _ = rc.Close() })
	ctx := context.Background()

	name := "test.publisher.missing:" + uuid.NewString()
	key := pkgredis.Key(name)
	t.Cleanup(func() { rc.Del(ctx, key, name) })
	entry := &outbox.Entry{ID: uuid.New(), EventType: "test.event", Payload: []byte(`{"k":"v"}`), StreamName: name}
	p := NewOutboxPublisher(rc, newTestLogger())

	err := p.Publish(ctx, entry)
	require.ErrorContains(t, err, "stream "+key+" does not exist; the streams-init step creates it")
	require.NotErrorIs(t, err, pkgevents.ErrPermanent, "a missing stream is retried, never dead-lettered")
	errs := p.PublishBatch(ctx, []*outbox.Entry{entry})
	require.Len(t, errs, 1)
	require.ErrorContains(t, errs[0], "stream "+key+" does not exist; the streams-init step creates it")
	for _, k := range []string{key, name} {
		n, err := rc.Exists(ctx, k).Result()
		require.NoError(t, err)
		require.Zero(t, n, "publishing must not create %s", k)
	}

	require.NoError(t, rc.XGroupCreateMkStream(ctx, key, "g", "$").Err())
	require.NoError(t, p.Publish(ctx, entry))
	msgs, err := rc.XRange(ctx, key, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, entry.ID.String(), msgs[0].Values["outbox_entry_id"])
	n, err := rc.Exists(ctx, name).Result()
	require.NoError(t, err)
	require.Zero(t, n, "the unprefixed key is never written")
}
