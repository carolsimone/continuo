//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-runner/adapters/postgres"
	"github.com/carolsimone/continuo/agent-runner/domain"
	artest "github.com/carolsimone/continuo/agent-runner/test"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupDB(t *testing.T) *sqlx.DB {
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
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, err := c.Host(ctx)
	require.NoError(t, err)
	// colima/macOS: testcontainers may return "localhost" which resolves to ::1
	// (IPv6), but the Docker-mapped port only listens on 127.0.0.1 (IPv4).
	if host == "localhost" {
		host = "127.0.0.1"
	}
	port, err := c.MappedPort(ctx, "5432")
	require.NoError(t, err)
	dsn := "host=" + host + " port=" + port.Port() + " user=testuser password=testpass dbname=testdb sslmode=disable"
	var db *sqlx.DB
	for i := 0; i < 10; i++ {
		db, err = sqlx.Connect("postgres", dsn)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err)
	require.NoError(t, artest.ApplyMigrations(db.DB))
	return db
}

func TestThreadRepository_RoundTrip(t *testing.T) {
	db := setupDB(t)
	repo := postgres.NewThreadRepository(db)
	ctx := context.Background()

	th, err := repo.CreateThread(ctx, "alice")
	require.NoError(t, err)

	_, err = repo.GetThread(ctx, th.ID, "bob")
	assert.Error(t, err)

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

	got, err := repo.GetThread(ctx, th.ID, "alice")
	require.NoError(t, err)
	assert.True(t, got.UpdatedAt.After(got.CreatedAt) || got.UpdatedAt.Equal(got.CreatedAt))
}

func TestThreadRepository_PendingActionsAndRetention(t *testing.T) {
	db := setupDB(t)
	repo := postgres.NewThreadRepository(db)
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

	idle, err := repo.ListIdleThreads(ctx, time.Now().UTC().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, idle, 1)
	require.NoError(t, repo.DeleteThread(ctx, th.ID))
	_, err = repo.GetThread(ctx, th.ID, "alice")
	assert.Error(t, err)
}
