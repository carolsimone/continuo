package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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

// DurationOrDefault reads an OPTIONAL Go duration env var (e.g. "15s",
// "1m30s"), returning fallback when it is unset or empty.
//
// A value that is set but does not parse is recorded as a validation failure
// naming the key, so start-up fails rather than the service running a default
// the operator never asked for. That distinction is the whole point of this
// helper over EnvDurationOrDefault: an install declares such a value once, in
// its chart values, and a typo there otherwise produces a system that looks
// configured — the key greps cleanly — while every process runs the author's
// default. The fallback is still returned so the caller holds a usable value
// while it finishes collecting the rest of the configuration problems.
func (v *Validator) DurationOrDefault(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		v.Add(fmt.Sprintf("%s (invalid duration %q: expected a Go duration such as 15s, 1m30s, or 20m)", key, raw))
		return fallback
	}
	return d
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

// EnvBoolOrDefault reads a boolean environment variable with a fallback
// default. Accepts the values accepted by strconv.ParseBool (true/false, 1/0,
// t/f, T/F, TRUE/FALSE, etc.) plus "yes"/"no" and "on"/"off"
// (case-insensitive). An unset, empty, or unrecognized value yields the
// fallback without panicking.
func EnvBoolOrDefault(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	// strconv.ParseBool handles: 1/0, t/f, T/F, TRUE/FALSE, true/false.
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	// Extend with yes/no and on/off.
	switch strings.ToLower(v) {
	case "yes", "on":
		return true
	case "no", "off":
		return false
	}
	return fallback
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
