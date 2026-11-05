//go:build integration

package redis

import (
	"context"
	"os"
	"strings"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// adminClient connects to the compose Redis and returns a unique stream name
// the test owns, with the key the admin stores it under; the key is deleted when
// the test ends, and the test fails if the bare contract name exists as a key.
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
			t.Errorf("the bare-name key %q exists (%d, %v), want only %q", stream, n, err, key)
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

// migrationClient signs in as continuo_redis_migrate. The compose Redis signs in
// every login with the rules the streams generator writes
// (compose/redis-users.acl).
func migrationClient(t *testing.T) *goredis.Client {
	t.Helper()
	migrate := goredis.NewClient(&goredis.Options{
		Addr: os.Getenv("REDIS_ADDR"), Username: "continuo_redis_migrate", Password: "local-continuo_redis_migrate",
	})
	t.Cleanup(func() { _ = migrate.Close() })
	return migrate
}

// continuo_redis_migrate's generated rule lets streams-init create a stream and
// a group under the prefix, and makes it pass the permission fence.
func TestTheMigrationLoginMayCreateStreamsAndGroups(t *testing.T) {
	_, stream, _ := adminClient(t)
	_, groupless, _ := adminClient(t)
	ctx := context.Background()
	migrate := migrationClient(t)
	admin := NewStreamAdmin(migrate)

	if created, err := admin.EnsureGroup(ctx, stream, "g"); err != nil || !created {
		t.Fatalf("EnsureGroup as continuo_redis_migrate = %v, %v; want true, nil", created, err)
	}
	if created, err := admin.EnsureStream(ctx, groupless); err != nil || !created {
		t.Fatalf("EnsureStream as continuo_redis_migrate = %v, %v; want true, nil", created, err)
	}
	if err := pkgredis.CheckPermissionFence(ctx, migrate, pkgconfig.RedisACLModePerService); err != nil {
		t.Fatalf("the permission fence refuses continuo_redis_migrate: %v", err)
	}
}

// continuo_redis_migrate reaches only the keys under the prefix: a stream's bare
// contract name is outside its rule, and it may not delete even its own keys.
func TestTheMigrationLoginReachesNoBareNameAndDeletesNoKey(t *testing.T) {
	rc, stream, key := adminClient(t)
	ctx := context.Background()
	migrate := migrationClient(t)
	if _, err := NewStreamAdmin(rc).EnsureGroup(ctx, stream, "g"); err != nil {
		t.Fatal(err)
	}

	for name, err := range map[string]error{
		"EXISTS of the bare name":        migrate.Exists(ctx, stream).Err(),
		"XINFO GROUPS of the bare name":  migrate.XInfoGroups(ctx, stream).Err(),
		"DEL of a key under the prefix":  migrate.Del(ctx, key).Err(),
		"XINFO STREAM of a prefixed key": migrate.XInfoStream(ctx, key).Err(),
		"TYPE of a key under the prefix": migrate.Type(ctx, key).Err(),
	} {
		if err == nil || !strings.HasPrefix(err.Error(), "NOPERM") {
			t.Errorf("%s as continuo_redis_migrate = %v, want NOPERM", name, err)
		}
	}
	if n, err := rc.Exists(ctx, key).Result(); err != nil || n != 1 {
		t.Fatalf("the prefixed key exists = %d, %v; want 1, nil", n, err)
	}
}
