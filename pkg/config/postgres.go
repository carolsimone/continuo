package config

import (
	"fmt"
	"strconv"
	"strings"
)

// PoolConfig bounds a Postgres connection pool. A zero field means the
// environment did not set it; pkg/db then takes the opening service's default.
type PoolConfig struct {
	MaxOpenConns int
	MaxIdleConns int
}

// PostgresConfig holds connection parameters for a PostgreSQL instance.
// Password is read again each time a connection string is built, so a rotated
// POSTGRES_PASSWORD_FILE reaches the next connection.
type PostgresConfig struct {
	Host     string
	Port     int
	DB       string
	User     string
	Password Secret
	SSLMode  string
	Pool     PoolConfig
}

// DSN returns a libpq-style connection string carrying the password as it
// reads now. Every value is quoted, so a password holding spaces, quotes or
// backslashes reaches Postgres unchanged. It returns an error when the
// password file can no longer be read.
func (c PostgresConfig) DSN() (string, error) {
	pw, err := c.Password.Value()
	if err != nil {
		return "", fmt.Errorf("%s_FILE: %w", c.Password.Name(), err)
	}
	return c.connString(&pw), nil
}

// DSNWithoutPassword returns the connection string DSN builds, without the
// password.
func (c PostgresConfig) DSNWithoutPassword() string {
	return c.connString(nil)
}

// connString renders every parameter as key='value'; the password is left out
// when password is nil.
func (c PostgresConfig) connString(password *string) string {
	sslMode := c.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	params := []string{
		"host=" + quoteDSNValue(c.Host),
		"port=" + quoteDSNValue(strconv.Itoa(c.Port)),
		"dbname=" + quoteDSNValue(c.DB),
		"user=" + quoteDSNValue(c.User),
	}
	if password != nil {
		params = append(params, "password="+quoteDSNValue(*password))
	}
	params = append(params, "sslmode="+quoteDSNValue(sslMode))
	return strings.Join(params, " ")
}

// dsnValueEscaper escapes the two characters a quoted libpq value cannot hold
// as they are: a backslash and a single quote.
var dsnValueEscaper = strings.NewReplacer(`\`, `\\`, `'`, `\'`)

// quoteDSNValue quotes v as a libpq connection-string value.
func quoteDSNValue(v string) string {
	return "'" + dsnValueEscaper.Replace(v) + "'"
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
		Port:     v.PortOrDefault("POSTGRES_PORT", 5432),
		DB:       db,
		User:     v.Require("POSTGRES_USER"),
		Password: v.RequireSecret("POSTGRES_PASSWORD"),
		SSLMode:  env("DB_SSLMODE", "disable"),
		Pool:     LoadPostgresPool(v),
	}
}

// LoadPostgres reads PostgreSQL connection config from standard env vars.
// Tier 1 (required): POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, and
// POSTGRES_PASSWORD or POSTGRES_PASSWORD_FILE.
// Tier 2 (defaults): POSTGRES_PORT=5432, DB_SSLMODE=disable. A POSTGRES_PORT
// that is not a port number is recorded on v.
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
