package postgres

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// NewDB opens a sqlx.DB connection using the provided PostgresConfig.
func NewDB(cfg pkgconfig.PostgresConfig) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	return db, nil
}
