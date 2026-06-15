package config

import (
	"os"
	"strconv"
	"time"
)

// Validator accumulates missing required env var names during config loading.
// Call Missing() after Load() to get the full list; fail fast if non-empty.
type Validator struct {
	missing []string
}

// Require reads a required string env var. Records the key if unset or empty.
func (v *Validator) Require(key string) string {
	val := os.Getenv(key)
	if val == "" {
		v.missing = append(v.missing, key)
	}
	return val
}

// RequireInt reads a required int env var. Records the key if unset or not parseable.
func (v *Validator) RequireInt(key string) int {
	val := os.Getenv(key)
	if val == "" {
		v.missing = append(v.missing, key)
		return 0
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		v.missing = append(v.missing, key)
		return 0
	}
	return n
}

// Missing returns all keys that were required but not set.
func (v *Validator) Missing() []string {
	return v.missing
}

// Add records an arbitrary validation failure (e.g. an invalid value or a
// conditional-required var) in the same missing list so callers can report a
// complete set of problems in one fail-fast pass.
func (v *Validator) Add(name string) {
	v.missing = append(v.missing, name)
}

// env reads a string environment variable with a fallback default (Tier 2 vars).
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envInt reads an integer environment variable with a fallback default (Tier 2 vars).
func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

// EnvOrDefault reads a string environment variable with a fallback default.
func EnvOrDefault(key, fallback string) string {
	return env(key, fallback)
}

// EnvIntOrDefault reads an integer environment variable with a fallback default.
func EnvIntOrDefault(key string, fallback int) int {
	return envInt(key, fallback)
}

// EnvDurationOrDefault reads a Go duration environment variable (e.g. "15s",
// "1m30s") with a fallback default. An unset or unparseable value yields the
// fallback so callers need not require the variable.
func EnvDurationOrDefault(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
