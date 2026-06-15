//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-runner/adapters/postgres"
	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/carolsimone/continuo/agent-runner/domain/repository"
	artest "github.com/carolsimone/continuo/agent-runner/test"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// sharedDB is the single Postgres connection shared across all tests in this
// package. Initialised once in TestMain.
var sharedDB *sqlx.DB

// TestMain starts ONE postgres container, applies migrations once, and shares
// the connection across all tests. Each test truncates the tables for isolation.
func TestMain(m *testing.M) {
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
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start postgres container: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = c.Terminate(ctx) }()

	host, err := c.Host(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get postgres host: %v\n", err)
		os.Exit(1)
	}
	// colima/macOS: testcontainers may return "localhost" which resolves to ::1
	// (IPv6), but the Docker-mapped port only listens on 127.0.0.1 (IPv4).
	if host == "localhost" {
		host = "127.0.0.1"
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get postgres port: %v\n", err)
		os.Exit(1)
	}

	dsn := "host=" + host + " port=" + port.Port() + " user=testuser password=testpass dbname=testdb sslmode=disable"
	var db *sqlx.DB
	for i := 0; i < 10; i++ {
		db, err = sqlx.Connect("postgres", dsn)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to postgres: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := artest.ApplyMigrations(db.DB); err != nil {
		fmt.Fprintf(os.Stderr, "failed to apply migrations: %v\n", err)
		os.Exit(1)
	}
	sharedDB = db

	os.Exit(m.Run())
}

// truncateTables removes all rows from every table so each test starts clean.
func truncateTables(t *testing.T) {
	t.Helper()
	_, err := sharedDB.ExecContext(context.Background(), `TRUNCATE threads, messages, pending_actions CASCADE`)
	require.NoError(t, err)
}

func TestThreadRepository_RoundTrip(t *testing.T) {
	truncateTables(t)
	repo := postgres.NewThreadRepository(sharedDB)
	ctx := context.Background()

	th, err := repo.CreateThread(ctx, "alice")
	require.NoError(t, err)

	// GetThread with wrong owner must return ErrNotFound.
	_, err = repo.GetThread(ctx, th.ID, "bob")
	require.Error(t, err)
	assert.True(t, errors.Is(err, repository.ErrNotFound), "expected ErrNotFound, got: %v", err)

	m1, err := repo.AppendMessage(ctx, th.ID, domain.RoleUser, json.RawMessage(`{"text":"hi"}`))
	require.NoError(t, err)
	m2, err := repo.AppendMessage(ctx, th.ID, domain.RoleAssistant, json.RawMessage(`{"text":"hello"}`))
	require.NoError(t, err)
	assert.Equal(t, 1, m1.Seq)
	assert.Equal(t, 2, m2.Seq)

	msgs, err := repo.ListMessages(ctx, th.ID)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	assert.Equal(t, domain.RoleUser, msgs[0].Role)

	// ListMessages on an empty thread must return an empty (non-nil) slice.
	th2, err := repo.CreateThread(ctx, "carol")
	require.NoError(t, err)
	emptyMsgs, err := repo.ListMessages(ctx, th2.ID)
	require.NoError(t, err)
	assert.NotNil(t, emptyMsgs)
	assert.Empty(t, emptyMsgs)

	got, err := repo.GetThread(ctx, th.ID, "alice")
	require.NoError(t, err)
	assert.True(t, got.UpdatedAt.After(got.CreatedAt) || got.UpdatedAt.Equal(got.CreatedAt))
}

func TestThreadRepository_PendingActionsAndRetention(t *testing.T) {
	truncateTables(t)
	repo := postgres.NewThreadRepository(sharedDB)
	ctx := context.Background()

	th, err := repo.CreateThread(ctx, "alice")
	require.NoError(t, err)

	a := &domain.PendingAction{
		ID: uuid.New(), ThreadID: th.ID, Tool: "schedule_trigger",
		Args: map[string]string{"schedule-name": "daily"}, Summary: "Trigger schedule \"daily\" now?",
		Status: domain.ActionPending, CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
	}
	require.NoError(t, repo.CreatePendingAction(ctx, a))
	require.NoError(t, repo.ResolvePendingAction(ctx, a.ID, domain.ActionApproved))

	// ResolvePendingAction on a non-existent id must return ErrNotFound.
	err = repo.ResolvePendingAction(ctx, uuid.New(), domain.ActionApproved)
	require.Error(t, err)
	assert.True(t, errors.Is(err, repository.ErrNotFound), "expected ErrNotFound, got: %v", err)

	idle, err := repo.ListIdleThreads(ctx, time.Now().UTC().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, idle, 1)
	require.NoError(t, repo.DeleteThread(ctx, th.ID))

	// GetThread after delete must return ErrNotFound (not a generic error).
	_, err = repo.GetThread(ctx, th.ID, "alice")
	require.Error(t, err)
	assert.True(t, errors.Is(err, repository.ErrNotFound), "expected ErrNotFound after delete, got: %v", err)
}

func TestThreadRepository_DeleteThreadIfIdle(t *testing.T) {
	truncateTables(t)
	repo := postgres.NewThreadRepository(sharedDB)
	ctx := context.Background()

	th, err := repo.CreateThread(ctx, "alice")
	require.NoError(t, err)

	// A cutoff BEFORE the thread's updated_at = thread is still active → no delete.
	pastCutoff := time.Now().UTC().Add(-time.Hour)
	deleted, err := repo.DeleteThreadIfIdle(ctx, th.ID, pastCutoff)
	require.NoError(t, err)
	assert.False(t, deleted, "an active thread (updated_at >= cutoff) must not be deleted")
	_, err = repo.GetThread(ctx, th.ID, "alice")
	require.NoError(t, err, "thread must still exist after a skipped conditional delete")

	// A cutoff AFTER the thread's updated_at = genuinely idle → delete.
	futureCutoff := time.Now().UTC().Add(time.Hour)
	deleted, err = repo.DeleteThreadIfIdle(ctx, th.ID, futureCutoff)
	require.NoError(t, err)
	assert.True(t, deleted, "an idle thread (updated_at < cutoff) must be deleted")
	_, err = repo.GetThread(ctx, th.ID, "alice")
	assert.True(t, errors.Is(err, repository.ErrNotFound), "thread must be gone after conditional delete")
}
