package test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// getEnvOrDefault returns the value of an env var or a fallback default.
func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// checkTCPReachable returns an error if addr (host:port) is not reachable
// within timeout. Used to skip integration tests when services are absent.
func checkTCPReachable(addr string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Errorf("service not reachable at %s: %w", addr, err)
	}
	conn.Close()
	return nil
}

// setupPostgres starts a PostgreSQL testcontainer and runs migrations to create
// the canonical k8s_outbox schema used by the standardized outbox pattern.
func setupPostgres(t *testing.T) (*sqlx.DB, func()) {
	ctx := context.Background()

	// Start PostgreSQL container
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start PostgreSQL container")

	// Get container host and port
	host, err := postgresContainer.Host(ctx)
	require.NoError(t, err, "Failed to get container host")

	port, err := postgresContainer.MappedPort(ctx, "5432")
	require.NoError(t, err, "Failed to get container port")

	// Force IPv4 for macOS compatibility
	if host == "localhost" {
		host = "127.0.0.1"
	}

	// Create connection string
	connStr := "host=" + host + " port=" + port.Port() + " user=testuser password=testpass dbname=testdb sslmode=disable"

	// Retry connection to database (port mapping may take a moment)
	var db *sqlx.DB
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		db, err = sqlx.Connect("postgres", connStr)
		if err == nil {
			break
		}
		t.Logf("Connection attempt %d/%d failed, retrying...", i+1, maxRetries)
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err, "Failed to connect to database after retries")

	// Create the canonical k8s_outbox table (matches V9 migration schema)
	_, err = db.ExecContext(ctx, `
		CREATE TABLE k8s_outbox (
			id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			message_processing_id  UUID NULL,
			aggregate_type         TEXT NOT NULL,
			aggregate_id           UUID NOT NULL,
			event_type             TEXT NOT NULL,
			payload                JSONB NOT NULL,
			stream_name            TEXT NOT NULL,
			status                 TEXT NOT NULL DEFAULT 'pending',
			retry_count            INT  NOT NULL DEFAULT 0,
			max_retries            INT  NOT NULL DEFAULT 3,
			created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			processed_at           TIMESTAMPTZ NULL,
			error_message          TEXT NULL,
			CONSTRAINT k8s_outbox_status_check
				CHECK (status IN ('pending', 'processed', 'failed'))
		);

		CREATE INDEX idx_k8s_outbox_pending
			ON k8s_outbox (created_at)
			WHERE status = 'pending';

		CREATE INDEX idx_k8s_outbox_aggregate
			ON k8s_outbox (aggregate_type, aggregate_id);
	`)
	require.NoError(t, err, "Failed to create k8s_outbox table")

	// Create the processed_events dedup table used by consumer-side dedup
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS processed_events (
			outbox_entry_id UUID        PRIMARY KEY,
			processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_k8s_processed_events_processed_at
			ON processed_events(processed_at);
	`)
	require.NoError(t, err, "Failed to create processed_events table")

	// Cleanup function
	cleanup := func() {
		db.Close()
		postgresContainer.Terminate(ctx)
	}

	return db, cleanup
}
