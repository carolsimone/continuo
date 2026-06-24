package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// Config holds the connection parameters for the Redis client.
type Config struct {
	Host     string
	Port     string
	Password string
}

// NewClient constructs and validates a Redis client using the provided config.
// Returns an error if the server is unreachable.
func NewClient(ctx context.Context, cfg Config) (*goredis.Client, error) {
	c := goredis.NewClient(&goredis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password,
	})
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return c, nil
}
