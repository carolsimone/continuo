package test

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupPostgres starts a PostgreSQL testcontainer and runs migrations to create
// the execution database's schema (execution_outbox, deployments,
// cancelled_schedules, message_processing) used by the standardized outbox
// pattern.
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

	// Apply the real db/migration/execution/V*.sql migrations in version order,
	// keeping the testcontainer schema (execution_outbox, deployments,
	// message_processing, and any later columns like
	// message_processing.outbox_entry_id) in lock-step with production.
	// Hand-rolled inline DDL drifts the first time a new migration adds a
	// column; this can't.
	require.NoError(t, ApplyMigrations(db.DB), "apply execution database migrations")

	// Cleanup function
	cleanup := func() {
		_ = db.Close()
		_ = postgresContainer.Terminate(ctx)
	}

	return db, cleanup
}
