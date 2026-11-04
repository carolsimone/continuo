package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

// RedisConfig holds connection parameters for a Redis instance.
type RedisConfig struct {
	Host string
	Port int
	// Username is the ACL user to sign in as; empty signs in as the default
	// user.
	Username string
	// Password is REDIS_PASSWORD or the file REDIS_PASSWORD_FILE names. A
	// client reads it on every new connection (see pkg/redis.NewClient).
	Password Secret
	// ACLMode is REDIS_ACL_MODE: RedisACLModeShared or RedisACLModePerService.
	// In perService mode a service refuses to start when its login can read
	// keys outside its own (see pkg/redis.CheckPermissionFence).
	ACLMode string
}

// The values REDIS_ACL_MODE takes.
const (
	// RedisACLModeShared is a Redis whose logins are not fenced per service:
	// every service signs in with the same password.
	RedisACLModeShared = "shared"
	// RedisACLModePerService is a Redis where each service signs in as its own
	// login, whose rule limits it to the keys and commands the service uses.
	RedisACLModePerService = "perService"
)

// loadRedisACLMode reads REDIS_ACL_MODE, RedisACLModeShared when unset. Any
// other value than the two modes is recorded on v naming the variable, and
// RedisACLModePerService is returned so a service that ignores v still runs
// the stricter check.
func loadRedisACLMode(v *Validator) string {
	raw := os.Getenv("REDIS_ACL_MODE")
	switch raw {
	case "":
		return RedisACLModeShared
	case RedisACLModeShared, RedisACLModePerService:
		return raw
	default:
		v.Add(fmt.Sprintf("REDIS_ACL_MODE (invalid value %q: expected %s or %s)", raw, RedisACLModeShared, RedisACLModePerService))
		return RedisACLModePerService
	}
}

// Addr returns the "host:port" dial address.
func (c RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// LoadRedis reads Redis connection config from standard env vars.
// Tier 1 (required): REDIS_HOST, REDIS_PORT, REDIS_PASSWORD (or
// REDIS_PASSWORD_FILE). Optional: REDIS_USERNAME, REDIS_ACL_MODE.
func LoadRedis(v *Validator) RedisConfig {
	return RedisConfig{
		Host:     v.Require("REDIS_HOST"),
		Port:     v.RequireInt("REDIS_PORT"),
		Username: EnvOrDefault("REDIS_USERNAME", ""),
		Password: v.RequireSecret("REDIS_PASSWORD"),
		ACLMode:  loadRedisACLMode(v),
	}
}

// LoadRedisFromAddr reads Redis config from REDIS_ADDR ("host:port" format).
// Tier 1 (required): REDIS_ADDR, REDIS_PASSWORD (or REDIS_PASSWORD_FILE).
// Optional: REDIS_USERNAME, REDIS_ACL_MODE.
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
		Username: EnvOrDefault("REDIS_USERNAME", ""),
		Password: v.RequireSecret("REDIS_PASSWORD"),
		ACLMode:  loadRedisACLMode(v),
	}
}

// LoadOptionalRedis reads Redis config from REDIS_ADDR for a service that
// runs without Redis when REDIS_ADDR is unset. It returns false, and records
// nothing on v, when REDIS_ADDR is unset; otherwise it reads the same
// variables as LoadRedisFromAddr, the password included.
func LoadOptionalRedis(v *Validator) (RedisConfig, bool) {
	if os.Getenv("REDIS_ADDR") == "" {
		return RedisConfig{}, false
	}
	return LoadRedisFromAddr(v), true
}
