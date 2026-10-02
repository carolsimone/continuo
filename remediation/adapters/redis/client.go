package redis

import (
	"context"
	"fmt"
	"log/slog"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// Config holds the connection parameters for the Redis client.
type Config struct {
	Host     string
	Port     string
	Password string
}

// NewClient constructs a Redis client using the provided config and waits for
// Redis to answer PING under policy, so a Redis that is still starting (or
// whose Service name is not resolvable yet) delays boot instead of failing it.
// It returns an error, with the client closed, when the wait gives up, when
// Redis rejects the connection, or when ctx is cancelled (the error then wraps
// context.Canceled).
func NewClient(ctx context.Context, cfg Config, policy pkgredis.StartupPolicy, logger *slog.Logger) (*goredis.Client, error) {
	c := goredis.NewClient(&goredis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password,
	})
	if err := pkgredis.WaitForRedis(ctx, c, policy, logger); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redis ping %s:%s: %w", cfg.Host, cfg.Port, err)
	}
	return c, nil
}
