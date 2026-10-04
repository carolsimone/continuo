package config

import "fmt"

// PoolConfig bounds a Postgres connection pool. A zero field means the
// environment did not set it; pkg/db then takes the opening service's default.
type PoolConfig struct {
	MaxOpenConns int
	MaxIdleConns int
}

// PostgresConfig holds connection parameters for a PostgreSQL instance.
type PostgresConfig struct {
	Host     string
	Port     int
	DB       string
	User     string
	Password string
	SSLMode  string
	Pool     PoolConfig
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

// LoadPostgresPool reads DB_MAX_OPEN_CONNS and DB_MAX_IDLE_CONNS. Both are
// optional. A value that is not a whole number of at least 1, or an idle limit
// above an open limit set beside it, is recorded on v.
func LoadPostgresPool(v *Validator) PoolConfig {
	p := PoolConfig{
		MaxOpenConns: v.PositiveIntOrZero("DB_MAX_OPEN_CONNS"),
		MaxIdleConns: v.PositiveIntOrZero("DB_MAX_IDLE_CONNS"),
	}
	if p.MaxOpenConns > 0 && p.MaxIdleConns > p.MaxOpenConns {
		v.Add(fmt.Sprintf("DB_MAX_IDLE_CONNS (%d exceeds DB_MAX_OPEN_CONNS %d)", p.MaxIdleConns, p.MaxOpenConns))
	}
	return p
}

// loadPostgresCommon reads PostgreSQL connection config from standard env vars,
// optionally using defaultDB for POSTGRES_DB when unset.
func loadPostgresCommon(v *Validator, defaultDB string) PostgresConfig {
	var db string
	if defaultDB != "" {
		// When a default is provided, use it if POSTGRES_DB is unset or empty,
		// and do not record the empty value as missing.
		db = env("POSTGRES_DB", defaultDB)
	} else {
		// Otherwise, require POSTGRES_DB and record it as missing if absent.
		db = v.Require("POSTGRES_DB")
	}
	return PostgresConfig{
		Host:     v.Require("POSTGRES_HOST"),
		Port:     envInt("POSTGRES_PORT", 5432),
		DB:       db,
		User:     v.Require("POSTGRES_USER"),
		Password: v.Require("POSTGRES_PASSWORD"),
		SSLMode:  env("DB_SSLMODE", "disable"),
		Pool:     LoadPostgresPool(v),
	}
}

// LoadPostgres reads PostgreSQL connection config from standard env vars.
// Tier 1 (required): POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD.
// Tier 2 (defaults): POSTGRES_PORT=5432, DB_SSLMODE=disable.
// DB_MAX_OPEN_CONNS and DB_MAX_IDLE_CONNS are optional pool limits; a zero value means the environment did not set it.
func LoadPostgres(v *Validator) PostgresConfig {
	return loadPostgresCommon(v, "")
}

// LoadPostgresWithDefaultDB is LoadPostgres for a service that falls back to
// defaultDB when POSTGRES_DB is unset; every other key is read as LoadPostgres
// reads it.
func LoadPostgresWithDefaultDB(v *Validator, defaultDB string) PostgresConfig {
	return loadPostgresCommon(v, defaultDB)
}
