package config

import "fmt"

// PostgresConfig holds connection parameters for a PostgreSQL instance.
type PostgresConfig struct {
	Host     string
	Port     int
	DB       string
	User     string
	Password string
	SSLMode  string
}

// DSN returns a libpq-style connection string.
func (c PostgresConfig) DSN() string {
	sslMode := c.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		c.Host, c.Port, c.DB, c.User, c.Password, sslMode,
	)
}

// LoadPostgres reads PostgreSQL connection config from standard env vars.
// Tier 1 (required): POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD.
// Tier 2 (defaults): POSTGRES_PORT=5432, DB_SSLMODE=disable.
func LoadPostgres(v *Validator) PostgresConfig {
	return PostgresConfig{
		Host:     v.Require("POSTGRES_HOST"),
		Port:     envInt("POSTGRES_PORT", 5432),
		DB:       v.Require("POSTGRES_DB"),
		User:     v.Require("POSTGRES_USER"),
		Password: v.Require("POSTGRES_PASSWORD"),
		SSLMode:  env("DB_SSLMODE", "disable"),
	}
}
