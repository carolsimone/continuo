package database

import (
	"fmt"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// NewConnection creates a PostgreSQL connection from a PostgresConfig.
func NewConnection(pg pkgconfig.PostgresConfig) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", pg.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	maxOpenConns := pkgconfig.EnvIntOrDefault("DB_MAX_OVERFLOW", 10)
	maxIdleConns := pkgconfig.EnvIntOrDefault("DB_POOL_SIZE", 4)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(2 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return db, nil
}

// GetPostgresConnection creates a PostgreSQL connection by reading env vars directly.
// It does NOT use the service Validator, so missing vars are not caught at startup.
// Prefer NewConnection with a pkgconfig.PostgresConfig loaded via LoadPostgres(v) instead.
func GetPostgresConnection() (*sqlx.DB, error) {
	v := &pkgconfig.Validator{}
	return NewConnection(pkgconfig.LoadPostgres(v))
}
