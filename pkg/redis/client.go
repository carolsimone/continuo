package redis

import (
	"context"
	"fmt"
	"log/slog"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	goredis "github.com/redis/go-redis/v9"
)

// NewClient builds a client for cfg that signs in on every new connection
// with cfg.Username and the password cfg.Password holds at that moment, so a
// rotated REDIS_PASSWORD_FILE reaches the next connection the client opens
// without a restart. A connection already open keeps the sign-in it made.
func NewClient(cfg pkgconfig.RedisConfig) *goredis.Client {
	username, password := cfg.Username, cfg.Password
	return goredis.NewClient(&goredis.Options{
		Addr: cfg.Addr(),
		CredentialsProviderContext: func(context.Context) (string, string, error) {
			pw, err := password.Value()
			if err != nil {
				return "", "", fmt.Errorf("%s_FILE: %w", password.Name(), err)
			}
			return username, pw, nil
		},
	})
}

// Connect builds a client with NewClient, waits for Redis to answer PING
// under policy (see WaitForRedis), so a Redis that is still starting, or
// whose Service name is not resolvable yet, delays boot instead of failing
// it, and then runs CheckPermissionFence in cfg.ACLMode. It returns an error,
// with the client closed, when the wait gives up, when Redis rejects the
// connection, when the login can read keys outside its own, or when ctx is
// cancelled (the error then wraps context.Canceled).
func Connect(ctx context.Context, cfg pkgconfig.RedisConfig, policy StartupPolicy, logger *slog.Logger) (*goredis.Client, error) {
	client := NewClient(cfg)
	if err := WaitForRedis(ctx, client, policy, logger); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping %s: %w", cfg.Addr(), err)
	}
	if err := CheckPermissionFence(ctx, client, cfg.ACLMode); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}
