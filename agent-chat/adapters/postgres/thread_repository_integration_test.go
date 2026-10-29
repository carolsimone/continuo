//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-chat/adapters/postgres"
	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/agent-chat/domain/repository"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDB connects to the agent-chat database that the Flyway migrations
// built (`make test-deps-up`); a test never creates or migrates a schema. The
// database must be dedicated to the tests and have no other writer: a table
// that already holds rows is refused, so every row a test sees is one it wrote.
// The cleanup deletes the threads, and with them their messages and pending
// actions, which carry a cascading foreign key to their thread.
func newTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	v := &pkgconfig.Validator{}
	cfg := pkgconfig.LoadPostgres(v)
	if len(v.Missing()) > 0 {
		testdeps.Unavailable(t, "POSTGRES_* not set; run `make test-go SERVICE=agent-chat`")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := sqlx.ConnectContext(ctx, "postgres", cfg.DSN())
	if err != nil {
		testdeps.Unavailable(t, "agent-chat postgres unreachable: %v", err)
	}
	t.Cleanup(func() { assert.NoError(t, db.Close()) })

	for _, table := range []string{"threads", "messages", "pending_actions"} {
		var present bool
		require.NoError(t, db.GetContext(ctx, &present, `SELECT to_regclass($1) IS NOT NULL`, table))
		require.True(t, present, "%s missing: run the Flyway migrations from db/migration/agent_chat before testing", table)
		var count int
		require.NoError(t, db.GetContext(ctx, &count, `SELECT count(*) FROM `+table))
		require.Zero(t, count, "%s contains rows: use an empty, dedicated agent-chat test database without a running agent-chat", table)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, err := db.ExecContext(cleanupCtx, `DELETE FROM threads`)
		assert.NoError(t, err, "delete the test-owned threads")
	})
	return db
}

func TestThreadRepository_RoundTrip(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewThreadRepository(db)
	ctx := context.Background()

	th, err := repo.CreateThread(ctx, "alice")
	require.NoError(t, err)

	// GetThread with wrong owner must return ErrNotFound.
	_, err = repo.GetThread(ctx, th.ID, "bob")
	require.Error(t, err)
	assert.True(t, errors.Is(err, repository.ErrNotFound), "expected ErrNotFound, got: %v", err)

	m1, err := repo.AppendMessage(ctx, th.ID, domain.RoleUser, domain.TextContent{Text: "hi"})
	require.NoError(t, err)
	m2, err := repo.AppendMessage(ctx, th.ID, domain.RoleAssistant, domain.TextContent{Text: "hello"})
	require.NoError(t, err)
	assert.Equal(t, 1, m1.Seq)
	assert.Equal(t, 2, m2.Seq)

	msgs, err := repo.ListMessages(ctx, th.ID)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	assert.Equal(t, domain.RoleUser, msgs[0].Role)
	// Content decodes back to the domain variant the role selects.
	assert.Equal(t, domain.TextContent{Text: "hi"}, msgs[0].Content)
	assert.Equal(t, domain.TextContent{Text: "hello"}, msgs[1].Content)

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

// TestThreadRepository_ContentRoundTripAllVariants stores each content variant
// through AppendMessage and reads it back, asserting the persisted value decodes
// to the exact domain content. This guards the messages.content JSONB persistence
// against a lossy encode/decode; the exact stored byte shape of each variant is
// pinned separately by the serialization package's golden tests.
func TestThreadRepository_ContentRoundTripAllVariants(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewThreadRepository(db)
	ctx := context.Background()

	th, err := repo.CreateThread(ctx, "alice")
	require.NoError(t, err)

	want := []domain.Content{
		domain.TextContent{Text: "hi"},
		domain.ToolCallContent{CallID: "c1", Tool: "run_sql", Args: map[string]string{"query": "select 1"}},
		domain.ToolResultContent{CallID: "c1", Output: "ok", IsError: true},
	}
	roles := []domain.Role{domain.RoleUser, domain.RoleToolCall, domain.RoleToolResult}
	for i, c := range want {
		_, err := repo.AppendMessage(ctx, th.ID, roles[i], c)
		require.NoError(t, err)
	}

	got, err := repo.ListMessages(ctx, th.ID)
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i := range want {
		assert.Equal(t, roles[i], got[i].Role)
		assert.Equal(t, want[i], got[i].Content)
	}
}

// TestThreadRepository_AppendMessage_RejectsRoleContentMismatch verifies the
// write path refuses a role/content pairing that would round-trip as the wrong
// variant, and persists nothing when it does.
func TestThreadRepository_AppendMessage_RejectsRoleContentMismatch(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewThreadRepository(db)
	ctx := context.Background()

	th, err := repo.CreateThread(ctx, "alice")
	require.NoError(t, err)

	_, err = repo.AppendMessage(ctx, th.ID, domain.RoleUser, domain.ToolCallContent{CallID: "c1", Tool: "t"})
	require.Error(t, err, "a user message carrying tool-call content must be rejected")

	msgs, err := repo.ListMessages(ctx, th.ID)
	require.NoError(t, err)
	assert.Empty(t, msgs, "the rejected message must not be persisted")
}

func TestThreadRepository_PendingActionsAndRetention(t *testing.T) {
	db := newTestDB(t)
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

func TestThreadRepository_GetPendingAction(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewThreadRepository(db)
	ctx := context.Background()

	th, err := repo.CreateThread(ctx, "alice")
	require.NoError(t, err)

	// No pending action yet → ErrNotFound.
	_, err = repo.GetPendingAction(ctx, th.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, repository.ErrNotFound), "expected ErrNotFound, got: %v", err)

	now := time.Now().UTC()
	// An older pending action and a newer one; an expired one; a resolved one.
	older := &domain.PendingAction{ID: uuid.New(), ThreadID: th.ID, Tool: "schedule_trigger", Args: map[string]string{"schedule-name": "old"}, Summary: "old", Status: domain.ActionPending, CreatedAt: now.Add(-2 * time.Minute), ExpiresAt: now.Add(5 * time.Minute)}
	newer := &domain.PendingAction{ID: uuid.New(), ThreadID: th.ID, Tool: "schedule_trigger", Args: map[string]string{"schedule-name": "new"}, Summary: "new", Status: domain.ActionPending, CreatedAt: now.Add(-1 * time.Minute), ExpiresAt: now.Add(5 * time.Minute)}
	expired := &domain.PendingAction{ID: uuid.New(), ThreadID: th.ID, Tool: "schedule_trigger", Args: map[string]string{"schedule-name": "exp"}, Summary: "exp", Status: domain.ActionPending, CreatedAt: now, ExpiresAt: now.Add(-time.Second)}
	require.NoError(t, repo.CreatePendingAction(ctx, older))
	require.NoError(t, repo.CreatePendingAction(ctx, newer))
	require.NoError(t, repo.CreatePendingAction(ctx, expired))

	// Returns the most recent still-pending, non-expired action with args intact.
	got, err := repo.GetPendingAction(ctx, th.ID)
	require.NoError(t, err)
	assert.Equal(t, newer.ID, got.ID)
	assert.Equal(t, domain.ActionPending, got.Status)
	assert.Equal(t, map[string]string{"schedule-name": "new"}, got.Args)

	// Once the newer action is resolved, the older one becomes the candidate.
	require.NoError(t, repo.ResolvePendingAction(ctx, newer.ID, domain.ActionApproved))
	got, err = repo.GetPendingAction(ctx, th.ID)
	require.NoError(t, err)
	assert.Equal(t, older.ID, got.ID)

	// With every action resolved or expired, nothing is resumable.
	require.NoError(t, repo.ResolvePendingAction(ctx, older.ID, domain.ActionDenied))
	_, err = repo.GetPendingAction(ctx, th.ID)
	assert.True(t, errors.Is(err, repository.ErrNotFound), "expected ErrNotFound when no resumable action, got: %v", err)
}

func TestThreadRepository_DeleteThreadIfIdle(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewThreadRepository(db)
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
