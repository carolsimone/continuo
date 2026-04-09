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

// setupPostgres starts a PostgreSQL testcontainer and runs migrations
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

	// Run migration V1: create k8s_status_outbox table
	migrationV1 := `
		CREATE TABLE k8s_status_outbox (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

			-- Event identification
			event_type VARCHAR(50) NOT NULL,
			stream_name VARCHAR(100) NOT NULL,

			-- Task context (copied from consumed message)
			task_id UUID NOT NULL,
			schedule_id UUID NOT NULL,
			schedule_name VARCHAR(255) NOT NULL,
			service_name VARCHAR(255) NOT NULL,
			schema_name VARCHAR(255) NOT NULL,
			table_name VARCHAR(255) NOT NULL,
			job_name VARCHAR(63) NOT NULL,

			-- Event-specific data
			error_message TEXT,
			task_retry_count INT DEFAULT 0,
			check_after BIGINT,

			-- State update flags (what gRPC calls to make in OutboxProcessor)
			update_task_status BOOLEAN DEFAULT false,
			new_task_status VARCHAR(20),
			new_retry_count INT,
			create_execution BOOLEAN DEFAULT false,
			execution_started_at TIMESTAMP WITH TIME ZONE,
			execution_completed_at TIMESTAMP WITH TIME ZONE,
			execution_seconds DOUBLE PRECISION,

			-- Outbox processing status
			status VARCHAR(20) NOT NULL DEFAULT 'pending',
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			processed_at TIMESTAMP WITH TIME ZONE,
			outbox_retry_count INT DEFAULT 0,
			max_retries INT DEFAULT 3,
			outbox_error_message TEXT,

			CONSTRAINT k8s_status_outbox_status_check CHECK (
				status IN ('pending', 'processed', 'failed')
			)
		);

		-- Index for efficient polling by OutboxProcessor
		CREATE INDEX idx_k8s_status_outbox_pending
		ON k8s_status_outbox(created_at)
		WHERE status = 'pending' AND outbox_retry_count < max_retries;

		-- Index for monitoring failed entries
		CREATE INDEX idx_k8s_status_outbox_failed
		ON k8s_status_outbox(created_at)
		WHERE status = 'failed';
	`

	_, err = db.ExecContext(ctx, migrationV1)
	require.NoError(t, err, "Failed to run migration V1")

	// Run migration V2: add stuck entry index
	migrationV2 := `
		CREATE INDEX idx_k8s_status_outbox_stuck
		ON k8s_status_outbox(created_at)
		WHERE status = 'pending' AND outbox_retry_count >= max_retries;
	`

	_, err = db.ExecContext(ctx, migrationV2)
	require.NoError(t, err, "Failed to run migration V2")

	// Run migration V3: processed_events dedup table
	migrationV3 := `
		CREATE TABLE IF NOT EXISTS processed_events (
			outbox_entry_id UUID        PRIMARY KEY,
			processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_k8s_processed_events_processed_at
			ON processed_events(processed_at);
	`
	_, err = db.ExecContext(ctx, migrationV3)
	require.NoError(t, err, "Failed to run migration V3")

	// Run migration V4: add node_type column to k8s_status_outbox
	migrationV4 := `
		ALTER TABLE k8s_status_outbox
		    ADD COLUMN node_type TEXT NOT NULL DEFAULT 'dbt-model';
	`
	_, err = db.ExecContext(ctx, migrationV4)
	require.NoError(t, err, "Failed to run migration V4")

	// Run migration V5: add execution_id and log_s3_key to k8s_status_outbox
	migrationV5 := `
		ALTER TABLE k8s_status_outbox
		    ADD COLUMN execution_id UUID,
		    ADD COLUMN log_s3_key   TEXT;
	`
	_, err = db.ExecContext(ctx, migrationV5)
	require.NoError(t, err, "Failed to run migration V5")

	// Cleanup function
	cleanup := func() {
		db.Close()
		postgresContainer.Terminate(ctx)
	}

	return db, cleanup
}
