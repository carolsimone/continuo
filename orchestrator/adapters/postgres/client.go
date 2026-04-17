package postgres

import (
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// NewPostgresClient creates a new PostgreSQL client connection
func NewPostgresClient(host string, port int, dbname, user, password string, logger *slog.Logger) (*sqlx.DB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		host, port, dbname, user, password,
	)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL",
			"host", host,
			"port", port,
			"dbname", dbname,
			"error", err,
		)
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	if err := db.Ping(); err != nil {
		logger.Error("Failed to ping PostgreSQL", "error", err)
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	logger.Info("Connected to PostgreSQL",
		"host", host,
		"port", port,
		"dbname", dbname,
	)

	return db, nil
}
