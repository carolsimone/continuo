package test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/testmigrations"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// k8sMigrationDir resolves <repo>/db/migration/k8s/ from this source file's
// path so the testcontainer schema is always in lock-step with production.
func k8sMigrationDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed — cannot locate k8s-controller/test/testutil.go")
	}
	// thisFile = <repo>/k8s-controller/test/testutil.go
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	return filepath.Join(repoRoot, "db", "migration", "k8s"), nil
}

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

	// Apply the real db/migration/k8s/V*.sql migrations in version order.
	// Mirrors state/test and executor-controller/test, and keeps the
	// testcontainer schema (k8s_outbox, message_processing, and any later
	// columns like message_processing.outbox_entry_id) in lock-step with
	// production. Hand-rolled inline DDL drifts the first time a new
	// migration adds a column; this can't.
	dir, err := k8sMigrationDir()
	require.NoError(t, err, "resolve k8s migration dir")
	require.NoError(t, testmigrations.Apply(db.DB, dir), "apply k8s migrations")

	// Cleanup function
	cleanup := func() {
		db.Close()
		postgresContainer.Terminate(ctx)
	}

	return db, cleanup
}
