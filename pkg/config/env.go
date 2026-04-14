package config

import (
	"os"
	"strconv"
)

// env reads a string environment variable with a fallback default.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envInt reads an integer environment variable with a fallback default.
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
