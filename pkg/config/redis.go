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

// LoadRedis reads Redis connection config from standard env vars.
// Tier 1 (required): REDIS_HOST, REDIS_PORT, REDIS_PASSWORD.
func LoadRedis(v *Validator) RedisConfig {
	return RedisConfig{
		Host:     v.Require("REDIS_HOST"),
		Port:     v.RequireInt("REDIS_PORT"),
		Password: v.Require("REDIS_PASSWORD"),
	}
}

// LoadRedisFromAddr reads Redis config from REDIS_ADDR ("host:port" format).
// Tier 1 (required): REDIS_ADDR, REDIS_PASSWORD.
func LoadRedisFromAddr(v *Validator) RedisConfig {
	addr := v.Require("REDIS_ADDR")
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		// addr has no port component — treat as host-only, default port
		host = addr
		portStr = "6379"
	}
	port, atoiErr := strconv.Atoi(portStr)
	if atoiErr != nil || port <= 0 {
		v.missing = append(v.missing, "REDIS_ADDR")
		port = 0
	}
	return RedisConfig{
		Host:     host,
		Port:     port,
		Password: v.Require("REDIS_PASSWORD"),
	}
}
