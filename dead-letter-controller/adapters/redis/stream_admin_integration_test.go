//go:build integration

package redis

import (
	"context"
	"fmt"
	"maps"
	"os"
	"strings"
	"testing"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
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

// CheckLogin signs in on a connection of its own: a login that exists with
// that password signs in, a wrong password or an unknown login is refused, and
// the admin's own connection keeps its sign-in throughout.
func TestCheckLoginSignsInAsTheLogin(t *testing.T) {
	rc, _, _ := adminClient(t)
	ctx := context.Background()
	user := "continuo-check-login-test-" + uuid.NewString()
	const password = "check-login-password"
	if err := rc.Do(ctx, "ACL", "SETUSER", user, "reset", "on", ">"+password, "-@all", "+ping", "+hello", "+client|setinfo").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rc.Do(context.Background(), "ACL", "DELUSER", user).Err() })
	admin := NewStreamAdmin(rc)

	if err := admin.CheckLogin(ctx, user, password); err != nil {
		t.Fatalf("CheckLogin with the login's password = %v, want nil", err)
	}
	err := admin.CheckLogin(ctx, user, "another-password")
	if err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("CheckLogin with another password = %v, want WRONGPASS", err)
	}
	if strings.Contains(err.Error(), "another-password") {
		t.Fatalf("the error %q carries the password", err)
	}
	if err := admin.CheckLogin(ctx, user+"-missing", password); err == nil {
		t.Fatal("CheckLogin of a login that does not exist = nil, want an error")
	}
	if err := rc.Ping(ctx).Err(); err != nil {
		t.Fatalf("the admin's own connection: %v", err)
	}
}

// A login whose rule refuses PING still signs in: CheckLogin checks the
// sign-in, not the login's commands.
func TestCheckLoginAcceptsALoginThatMayNotPing(t *testing.T) {
	rc, _, _ := adminClient(t)
	ctx := context.Background()
	user := "continuo-check-login-test-" + uuid.NewString()
	const password = "check-login-password"
	if err := rc.Do(ctx, "ACL", "SETUSER", user, "reset", "on", ">"+password, "-@all").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rc.Do(context.Background(), "ACL", "DELUSER", user).Err() })
	if err := NewStreamAdmin(rc).CheckLogin(ctx, user, password); err != nil {
		t.Fatalf("CheckLogin = %v, want nil", err)
	}
}

// unprefixedStream returns a client of the compose Redis and a stream name the
// test owns; both of its keys are deleted when the test ends.
func unprefixedStream(t *testing.T) (*goredis.Client, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		testdeps.Unavailable(t, "REDIS_ADDR not set — skipping Redis integration test")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	t.Cleanup(func() { _ = rc.Close() })
	stream := "test.stream-admin:" + uuid.NewString()
	t.Cleanup(func() { rc.Del(context.Background(), pkgredis.Key(stream), stream) })
	return rc, stream
}

func TestReadUnprefixedReportsAMissingKey(t *testing.T) {
	rc, stream := unprefixedStream(t)
	_, exists, err := NewStreamAdmin(rc).ReadUnprefixed(context.Background(), stream)
	if err != nil || exists {
		t.Fatalf("ReadUnprefixed = exists %v, %v; want false, nil", exists, err)
	}
}

// ReadUnprefixed reads the key named by the bare contract name, never the
// prefixed one: each group's pending count and last delivered entry, and the
// key's last entry.
func TestReadUnprefixedReadsEveryGroupAndTheLastEntry(t *testing.T) {
	rc, stream := unprefixedStream(t)
	ctx := context.Background()
	for _, id := range []string{"10-0", "20-0", "30-0"} {
		if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, ID: id, Values: map[string]any{"k": id}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rc.XGroupCreate(ctx, stream, "reader", "0").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rc.XGroupCreate(ctx, stream, "caught-up", "$").Err(); err != nil {
		t.Fatal(err)
	}
	read, err := rc.XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "reader", Consumer: "c", Streams: []string{stream, ">"}, Count: 2}).Result()
	if err != nil || len(read) != 1 || len(read[0].Messages) != 2 {
		t.Fatalf("XREADGROUP = %v, %v", read, err)
	}
	if err := rc.XAck(ctx, stream, "reader", "10-0").Err(); err != nil {
		t.Fatal(err)
	}
	// A prefixed key of the same name is not the unprefixed key.
	if err := rc.XGroupCreateMkStream(ctx, pkgredis.Key(stream), "prefixed-only", "0").Err(); err != nil {
		t.Fatal(err)
	}

	k, exists, err := NewStreamAdmin(rc).ReadUnprefixed(ctx, stream)
	if err != nil || !exists {
		t.Fatalf("ReadUnprefixed = exists %v, %v; want true, nil", exists, err)
	}
	if k.Last == nil || k.Last.String() != "30-0" {
		t.Fatalf("Last = %v, want 30-0", k.Last)
	}
	got := map[string]string{}
	for _, g := range k.Groups {
		got[g.Name] = fmt.Sprintf("%d %s", g.Pending, g.LastDelivered)
	}
	want := map[string]string{"reader": "1 20-0", "caught-up": "0 30-0"}
	if !maps.Equal(got, want) {
		t.Fatalf("groups = %v, want %v", got, want)
	}
}

func TestReadUnprefixedOfAKeyWithNoEntry(t *testing.T) {
	rc, stream := unprefixedStream(t)
	ctx := context.Background()
	if err := rc.XGroupCreateMkStream(ctx, stream, "g", "0").Err(); err != nil {
		t.Fatal(err)
	}
	k, exists, err := NewStreamAdmin(rc).ReadUnprefixed(ctx, stream)
	if err != nil || !exists {
		t.Fatalf("ReadUnprefixed = exists %v, %v; want true, nil", exists, err)
	}
	if k.Last != nil || len(k.Groups) != 1 || k.Groups[0].Name != "g" {
		t.Fatalf("ReadUnprefixed = %+v, want group g and no last entry", k)
	}
}

func TestDeleteUnprefixedDeletesOnlyTheUnprefixedKey(t *testing.T) {
	rc, stream := unprefixedStream(t)
	ctx := context.Background()
	for _, key := range []string{stream, pkgredis.Key(stream)} {
		if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: key, Values: map[string]any{"k": "v"}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	admin := NewStreamAdmin(rc)
	deleted, err := admin.DeleteUnprefixed(ctx, stream)
	if err != nil || !deleted {
		t.Fatalf("DeleteUnprefixed = %v, %v; want true, nil", deleted, err)
	}
	if n := rc.Exists(ctx, stream).Val(); n != 0 {
		t.Fatalf("the unprefixed key still exists")
	}
	if n := rc.Exists(ctx, pkgredis.Key(stream)).Val(); n != 1 {
		t.Fatalf("the prefixed key was deleted")
	}
	again, err := admin.DeleteUnprefixed(ctx, stream)
	if err != nil || again {
		t.Fatalf("second DeleteUnprefixed = %v, %v; want false, nil", again, err)
	}
}

// The compose Redis signs in every login with the rules the streams generator
// writes (compose/redis-users.acl), so this proves continuo_redis_migrate's
// generated rule lets streams-init read and delete the unprefixed key of a
// live stream and of a retired one.
func TestTheMigrationLoginMayReadAndDeleteTheUnprefixedKeys(t *testing.T) {
	rc, _ := unprefixedStream(t)
	ctx := context.Background()
	live, retired := streams.RemediationProposedV1, streams.Retired[0]
	t.Cleanup(func() { rc.Del(context.Background(), live, retired) })
	for _, key := range []string{live, retired} {
		if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: key, Values: map[string]any{"k": "v"}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	migrate := goredis.NewClient(&goredis.Options{
		Addr: os.Getenv("REDIS_ADDR"), Username: "continuo_redis_migrate", Password: "local-continuo_redis_migrate",
	})
	t.Cleanup(func() { _ = migrate.Close() })
	admin := NewStreamAdmin(migrate)

	if _, exists, err := admin.ReadUnprefixed(ctx, live); err != nil || !exists {
		t.Fatalf("ReadUnprefixed(%s) as continuo_redis_migrate = exists %v, %v; want true, nil", live, exists, err)
	}
	for _, key := range []string{live, retired} {
		if deleted, err := admin.DeleteUnprefixed(ctx, key); err != nil || !deleted {
			t.Fatalf("DeleteUnprefixed(%s) as continuo_redis_migrate = %v, %v; want true, nil", key, deleted, err)
		}
	}
}
