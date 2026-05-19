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

// setupPostgres starts a PostgreSQL testcontainer and applies the
// executor-controller Flyway migrations via ApplyMigrations so the test
// schema stays in lock-step with production. Eliminates the prior bug
// where the inline schema diverged from db/migration/executor/V*.sql and
// silently dropped new columns.
func setupPostgres(t *testing.T) (*sqlx.DB, func()) {
	ctx := context.Background()

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

	host, err := postgresContainer.Host(ctx)
	require.NoError(t, err, "Failed to get container host")

	port, err := postgresContainer.MappedPort(ctx, "5432")
	require.NoError(t, err, "Failed to get container port")

	if host == "localhost" {
		host = "127.0.0.1"
	}

	connStr := "host=" + host + " port=" + port.Port() + " user=testuser password=testpass dbname=testdb sslmode=disable"

	var db *sqlx.DB
	for i := 0; i < 10; i++ {
		db, err = sqlx.Connect("postgres", connStr)
		if err == nil {
			break
		}
		t.Logf("Connection attempt %d/10 failed, retrying...", i+1)
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err, "Failed to connect to database after retries")

	require.NoError(t, ApplyMigrations(db.DB), "Failed to apply executor Flyway migrations")

	cleanup := func() {
		db.Close()
		postgresContainer.Terminate(ctx)
	}

	return db, cleanup
}
