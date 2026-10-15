package postgres_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDB opens the Flyway-built orchestrator database named by the
// POSTGRES_* environment, with the docker-compose defaults.
func newTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	host := envOrDefault("POSTGRES_HOST", "postgres")
	port := envOrDefault("POSTGRES_PORT", "5432")
	dbName := envOrDefault("POSTGRES_DB", "continuo_orchestrator")
	user := envOrDefault("POSTGRES_USER", "continuo_svc")
	password := envOrDefault("POSTGRES_PASSWORD", "continuo")
	connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		host, port, dbName, user, password)
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		testdeps.Unavailable(t, "Postgres unavailable: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// relation returns the name of the public relation called name, or an invalid
// NullString when the schema has no such relation.
func relation(t *testing.T, db *sqlx.DB, name string) sql.NullString {
	t.Helper()
	var reg sql.NullString
	require.NoError(t, db.Get(&reg, `SELECT to_regclass('public.' || $1)::text`, name))
	return reg
}

// orchestrator keeps no cancellation guard, so V16 drops cancelled_schedules;
// the outbox the relay publishes from stays.
func TestOrchestratorSchema_HasNoCancelledSchedulesTable(t *testing.T) {
	db := newTestDB(t)
	require.True(t, relation(t, db, "orchestrator_outbox").Valid,
		"orchestrator_outbox is missing: run the orchestrator migrations (flyway-orchestrator)")
	assert.False(t, relation(t, db, "cancelled_schedules").Valid,
		"cancelled_schedules still exists: run the orchestrator migrations through V16 (flyway-orchestrator)")
}
