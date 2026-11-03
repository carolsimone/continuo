package redis_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// redisConfigFromFile loads a RedisConfig for addr whose password is read from
// a file holding password, and returns it with the file's path and a function
// that rewrites the file.
func redisConfigFromFile(t *testing.T, addr, username, password string) (pkgconfig.RedisConfig, string, func(string)) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "REDIS_PASSWORD")
	write := func(pw string) {
		t.Helper()
		require.NoError(t, os.WriteFile(file, []byte(pw+"\n"), 0o600))
	}
	write(password)
	t.Setenv("REDIS_ADDR", addr)
	t.Setenv("REDIS_USERNAME", username)
	t.Setenv("REDIS_PASSWORD", "")
	t.Setenv("REDIS_PASSWORD_FILE", file)
	v := &pkgconfig.Validator{}
	cfg := pkgconfig.LoadRedisFromAddr(v)
	require.Empty(t, v.Missing())
	return cfg, file, write
}

// The client's credentials come from the config at the moment a connection is
// opened: the username as loaded, the password as the file holds it then.
func TestNewClientReadsTheCredentialsWhenAConnectionOpens(t *testing.T) {
	cfg, _, write := redisConfigFromFile(t, "redis.invalid:6380", "catalog", "first")
	client := pkgredis.NewClient(cfg)
	defer func() { _ = client.Close() }()

	opts := client.Options()
	assert.Equal(t, "redis.invalid:6380", opts.Addr)
	assert.Empty(t, opts.Password, "the password is not fixed at construction")
	require.NotNil(t, opts.CredentialsProviderContext)

	user, pw, err := opts.CredentialsProviderContext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "catalog", user)
	assert.Equal(t, "first", pw)

	write("second")
	_, pw, err = opts.CredentialsProviderContext(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "second", pw, "a rotated file reaches the next connection")
}

// A password file that can no longer be read fails the connection with an
// error naming the variable, not the password.
func TestNewClientFailsAConnectionWhenThePasswordFileGoesAway(t *testing.T) {
	cfg, file, _ := redisConfigFromFile(t, "redis.invalid:6379", "", "first")
	require.NoError(t, os.Remove(file))
	client := pkgredis.NewClient(cfg)
	defer func() { _ = client.Close() }()

	_, _, err := client.Options().CredentialsProviderContext(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "REDIS_PASSWORD_FILE")
	assert.NotContains(t, err.Error(), "first")
}

// Connect gives up once the startup policy is spent and names the address.
func TestConnectFailsWhenRedisNeverAnswers(t *testing.T) {
	cfg, _, _ := redisConfigFromFile(t, "127.0.0.1:1", "", "never-in-an-error")
	policy := pkgredis.StartupPolicy{Timeout: 200 * time.Millisecond, InitialDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
	client, err := pkgredis.Connect(context.Background(), cfg, policy, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "127.0.0.1:1")
	assert.NotContains(t, err.Error(), "never-in-an-error")
}
