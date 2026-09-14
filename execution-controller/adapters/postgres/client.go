package postgres

import (
	"fmt"
	"log/slog"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// NewPostgresClient opens and pings a connection built from cfg, including the
// configured sslmode (DB_SSLMODE), so a TLS-only database is honoured.
func NewPostgresClient(cfg pkgconfig.PostgresConfig, logger *slog.Logger) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", cfg.DSN())
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "host", cfg.Host, "port", cfg.Port, "dbname", cfg.DB, "error", err)
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	if err := db.Ping(); err != nil {
		logger.Error("Failed to ping PostgreSQL", "error", err)
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}
	logger.Info("Connected to PostgreSQL", "host", cfg.Host, "port", cfg.Port, "dbname", cfg.DB, "sslmode", cfg.SSLMode)
	return db, nil
}
