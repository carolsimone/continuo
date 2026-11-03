//go:build integration

package redis_test

import (
	"context"
	"os"
	"testing"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/testdeps"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rotationTestUser is the Redis ACL user the rotation test signs in as. The
// test creates it and deletes it again, so the server's default user, which
// every other test signs in as, keeps its password throughout.
const rotationTestUser = "continuo-password-rotation-test"

// The client reads REDIS_PASSWORD_FILE each time it opens a connection: once
// the server and the file both move to a new password, the next new connection
// signs in with it, and a connection the client already holds keeps working.
func TestNewClientReadsThePasswordFileForEveryNewConnection(t *testing.T) {
	const (
		firstPassword  = "first-rotation-password"
		secondPassword = "second-rotation-password"
	)
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		testdeps.Unavailable(t, "REDIS_ADDR not set; run `make test-go SERVICE=pkg`")
	}
	ctx := context.Background()

	admin := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	t.Cleanup(func() { _ = admin.Close() })
	setPassword := func(pw string) {
		t.Helper()
		require.NoError(t, admin.Do(ctx, "ACL", "SETUSER", rotationTestUser,
			"on", "resetpass", ">"+pw, "-@all", "+@connection").Err())
	}
	setPassword(firstPassword)
	t.Cleanup(func() { _ = admin.Do(context.Background(), "ACL", "DELUSER", rotationTestUser).Err() })

	cfg, _, writePassword := redisConfigFromFile(t, addr, rotationTestUser, firstPassword)
	client := pkgredis.NewClient(cfg)
	t.Cleanup(func() { _ = client.Close() })

	// Each client.Conn holds its own connection, so every Conn below that
	// pings first opens a new connection, which signs in.
	held := client.Conn()
	t.Cleanup(func() { _ = held.Close() })
	require.NoError(t, held.Ping(ctx).Err(), "the file's password signs in")

	// The server moves to the new password before the file does: a new
	// connection still signs in with the file's old password and is refused.
	setPassword(secondPassword)
	stale := client.Conn()
	err := stale.Ping(ctx).Err()
	_ = stale.Close()
	require.Error(t, err, "a new connection signs in with what the file holds")
	assert.Contains(t, err.Error(), "WRONGPASS")
	assert.NotContains(t, err.Error(), firstPassword, "the error does not carry the password")

	writePassword(secondPassword)
	rotated := client.Conn()
	t.Cleanup(func() { _ = rotated.Close() })
	require.NoError(t, rotated.Ping(ctx).Err(), "a new connection signs in with the rotated file")

	require.NoError(t, held.Ping(ctx).Err(), "a connection opened before the rotation keeps working")
}
