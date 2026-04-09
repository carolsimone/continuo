package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/dependency-controller/adapters/postgres"
	"github.com/carolsimone/continuo/dependency-controller/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestDB(t *testing.T) (*sqlx.DB, func()) {
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

	// Run migrations - V1: create outbox table first
	migrationV1 := `
		CREATE TABLE outbox (
			id uuid DEFAULT gen_random_uuid() NOT NULL,
			aggregate_type character varying(50) NOT NULL,
			aggregate_id uuid NOT NULL,
			event_type character varying(50) NOT NULL,
			payload jsonb NOT NULL,
			stream_name character varying(100) NOT NULL,
			created_at timestamp with time zone DEFAULT now() NOT NULL,
			processed_at timestamp with time zone,
			status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
			retry_count integer DEFAULT 0 NOT NULL,
			max_retries integer DEFAULT 3 NOT NULL,
			error_message text,
			CONSTRAINT outbox_pkey PRIMARY KEY (id),
			CONSTRAINT outbox_status_check CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'processed'::character varying, 'failed'::character varying])::text[])))
		);

		CREATE INDEX idx_outbox_aggregate ON outbox USING btree (aggregate_type, aggregate_id);
		CREATE INDEX idx_outbox_status_created ON outbox USING btree (status, created_at) WHERE ((status)::text = 'pending'::text);
	`

	_, err = db.ExecContext(ctx, migrationV1)
	require.NoError(t, err, "Failed to run migration V1")

	// Run migration V2: create message_processing table and add foreign key to outbox
	migrationV2 := `
		CREATE TABLE IF NOT EXISTS message_processing (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			message_id VARCHAR(255) NOT NULL UNIQUE,
			stream_name VARCHAR(100) NOT NULL,
			state VARCHAR(50) NOT NULL CHECK (state IN ('processing', 'completed', 'acked')),
			payload JSONB NOT NULL,
			error TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		);

		CREATE INDEX IF NOT EXISTS idx_message_processing_message_id ON message_processing(message_id);
		CREATE INDEX IF NOT EXISTS idx_message_processing_state ON message_processing(state);
		CREATE INDEX IF NOT EXISTS idx_message_processing_created_at ON message_processing(created_at);

		ALTER TABLE outbox ADD COLUMN IF NOT EXISTS message_processing_id UUID REFERENCES message_processing(id);
		CREATE INDEX IF NOT EXISTS idx_outbox_message_processing_id ON outbox(message_processing_id);
	`

	_, err = db.ExecContext(ctx, migrationV2)
	require.NoError(t, err, "Failed to run migration V2")

	// Cleanup function
	cleanup := func() {
		db.Close()
		postgresContainer.Terminate(ctx)
	}

	return db, cleanup
}

func TestInsertIfNotExists_NewMessage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo := postgres.NewMessageProcessingRepository(db, nil)
	ctx := context.Background()

	payload := map[string]string{"test": "data"}
	payloadJSON, _ := json.Marshal(payload)

	msgProc := &model.MessageProcessing{
		MessageID:  "1738756432123-0",
		StreamName: "update.table:v1",
		State:      model.MessageProcessingStateProcessing,
		Payload:    payloadJSON,
	}

	id, inserted, err := repo.InsertIfNotExists(ctx, msgProc)

	require.NoError(t, err)
	assert.True(t, inserted, "Expected new message to be inserted")
	assert.NotEqual(t, uuid.Nil, id)

	// Verify it was inserted
	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM message_processing WHERE message_id = $1", msgProc.MessageID)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestInsertIfNotExists_DuplicateMessage(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo := postgres.NewMessageProcessingRepository(db, nil)
	ctx := context.Background()

	payload := map[string]string{"test": "data"}
	payloadJSON, _ := json.Marshal(payload)

	msgProc := &model.MessageProcessing{
		MessageID:  "1738756432123-1",
		StreamName: "update.table:v1",
		State:      model.MessageProcessingStateProcessing,
		Payload:    payloadJSON,
	}

	// Insert first time
	id1, inserted1, err := repo.InsertIfNotExists(ctx, msgProc)
	require.NoError(t, err)
	assert.True(t, inserted1)

	// Try to insert again
	id2, inserted2, err := repo.InsertIfNotExists(ctx, msgProc)
	require.NoError(t, err)
	assert.False(t, inserted2, "Expected duplicate to not be inserted")
	assert.Equal(t, id1, id2, "Should return existing ID")
}
