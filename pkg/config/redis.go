package config

import (
	"fmt"
	"net"
	"strconv"
)

// RedisConfig holds connection parameters for a Redis instance.
type RedisConfig struct {
	Host     string
	Port     int
	Password string
}

// Addr returns the "host:port" dial address.
func (c RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// LoadRedis reads Redis connection config from standard env vars
// (REDIS_HOST, REDIS_PORT, REDIS_PASSWORD).
func LoadRedis() RedisConfig {
	return RedisConfig{
		Host:     env("REDIS_HOST", "localhost"),
		Port:     envInt("REDIS_PORT", 6379),
		Password: env("REDIS_PASSWORD", ""),
	}
}

// LoadRedisFromAddr reads Redis config from REDIS_ADDR ("host:port" format)
// and REDIS_PASSWORD. This supports the state service's env convention.
func LoadRedisFromAddr() RedisConfig {
	addr := env("REDIS_ADDR", "localhost:6379")
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		portStr = "6379"
	}
	port, _ := strconv.Atoi(portStr)
	return RedisConfig{
		Host:     host,
		Port:     port,
		Password: env("REDIS_PASSWORD", ""),
	}
}
