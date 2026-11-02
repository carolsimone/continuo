//go:build integration

package redis

import (
	"context"
	"os"
	"testing"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// adminClient connects to the compose Redis and returns a unique stream name
// the test owns, with the key the admin stores it under; the key is deleted when
// the test ends, and the test fails if the unprefixed name exists as a key.
func adminClient(t *testing.T) (rc *goredis.Client, stream, key string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		testdeps.Unavailable(t, "REDIS_ADDR not set — skipping Redis integration test")
	}
	rc = goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	t.Cleanup(func() { _ = rc.Close() })
	stream = "test.stream-admin:" + uuid.NewString()
	key = pkgredis.Key(stream)
	t.Cleanup(func() { rc.Del(context.Background(), key, stream) })
	t.Cleanup(func() {
		if n, err := rc.Exists(context.Background(), stream).Result(); err != nil || n != 0 {
			t.Errorf("the unprefixed key %q exists (%d, %v), want only %q", stream, n, err, key)
		}
	})
	return rc, stream, key
}

func TestEnsureGroupCreatesStreamAndGroupFromZero(t *testing.T) {
	rc, stream, key := adminClient(t)
	ctx := context.Background()

	created, err := NewStreamAdmin(rc).EnsureGroup(ctx, stream, "g")
	if err != nil || !created {
		t.Fatalf("EnsureGroup = %v, %v; want true, nil", created, err)
	}
	infos, err := rc.XInfoGroups(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Name != "g" || infos[0].LastDeliveredID != "0-0" {
		t.Fatalf("groups = %+v, want one group g at 0-0", infos)
	}
}

func TestEnsureGroupIgnoresBusyGroup(t *testing.T) {
	rc, stream, key := adminClient(t)
	ctx := context.Background()
	admin := NewStreamAdmin(rc)
	if _, err := admin.EnsureGroup(ctx, stream, "g"); err != nil {
		t.Fatal(err)
	}
	created, err := admin.EnsureGroup(ctx, stream, "g")
	if err != nil || created {
		t.Fatalf("second EnsureGroup = %v, %v; want false, nil", created, err)
	}
	if n, err := rc.Exists(ctx, key).Result(); err != nil || n != 1 {
		t.Fatalf("the prefixed key %q exists = %d, %v; want 1, nil", key, n, err)
	}
}

func TestEnsureStreamCreatesAStreamWithNoGroup(t *testing.T) {
	rc, stream, key := adminClient(t)
	ctx := context.Background()
	admin := NewStreamAdmin(rc)

	created, err := admin.EnsureStream(ctx, stream)
	if err != nil || !created {
		t.Fatalf("EnsureStream = %v, %v; want true, nil", created, err)
	}
	infos, err := rc.XInfoGroups(ctx, key).Result()
	if err != nil {
		t.Fatalf("the stream does not exist: %v", err)
	}
	if len(infos) != 0 {
		t.Fatalf("the temporary group was left behind: %+v", infos)
	}
	// The stream accepts a NOMKSTREAM add, as a producer's add does.
	if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: key, NoMkStream: true, Values: map[string]any{"k": "v"}}).Err(); err != nil {
		t.Fatalf("XADD NOMKSTREAM: %v", err)
	}
	again, err := admin.EnsureStream(ctx, stream)
	if err != nil || again {
		t.Fatalf("second EnsureStream = %v, %v; want false, nil", again, err)
	}
}
