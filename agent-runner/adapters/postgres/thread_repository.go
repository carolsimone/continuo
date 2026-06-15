package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/carolsimone/continuo/agent-runner/domain/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// ThreadRepository is a Postgres-backed implementation of repository.ThreadRepository.
type ThreadRepository struct {
	db *sqlx.DB
}

// Compile-time assertion that ThreadRepository implements the port.
var _ repository.ThreadRepository = (*ThreadRepository)(nil)

// NewThreadRepository returns a ThreadRepository backed by the given sqlx.DB.
func NewThreadRepository(db *sqlx.DB) *ThreadRepository {
	return &ThreadRepository{db: db}
}

// CreateThread inserts a new Thread owned by userID and returns it.
func (r *ThreadRepository) CreateThread(ctx context.Context, userID string) (*domain.Thread, error) {
	now := time.Now().UTC()
	t := &domain.Thread{
		ID:        uuid.New(),
		UserID:    userID,
		CreatedAt: now,
		UpdatedAt: now,
	}
	const q = `INSERT INTO threads (id, user_id, created_at, updated_at) VALUES ($1, $2, $3, $4)`
	if _, err := r.db.ExecContext(ctx, q, t.ID, t.UserID, t.CreatedAt, t.UpdatedAt); err != nil {
		return nil, fmt.Errorf("create thread: %w", err)
	}
	return t, nil
}

// GetThread returns the thread with the given id only if it belongs to userID.
// Returns an error if the thread does not exist or is owned by a different user.
func (r *ThreadRepository) GetThread(ctx context.Context, id uuid.UUID, userID string) (*domain.Thread, error) {
	const q = `SELECT id, user_id, created_at, updated_at FROM threads WHERE id = $1 AND user_id = $2`
	var t domain.Thread
	err := r.db.QueryRowContext(ctx, q, id, userID).Scan(&t.ID, &t.UserID, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get thread %s for user %s: %w", id, userID, repository.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get thread %s: %w", id, err)
	}
	return &t, nil
}

// AppendMessage assigns the next sequential seq within the thread, inserts the
// message, and bumps the thread's updated_at — all inside a single transaction.
func (r *ThreadRepository) AppendMessage(ctx context.Context, threadID uuid.UUID, role domain.Role, content json.RawMessage) (*domain.Message, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx for append message: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Lock the thread row so concurrent appends to the same thread serialize and
	// cannot compute the same next seq (which would collide on UNIQUE(thread_id,seq)).
	if _, err := tx.ExecContext(ctx, `SELECT 1 FROM threads WHERE id = $1 FOR UPDATE`, threadID); err != nil {
		return nil, fmt.Errorf("lock thread %s: %w", threadID, err)
	}

	// Compute the next sequence number atomically inside the transaction.
	var seq int
	const seqQ = `SELECT COALESCE(MAX(seq), 0) + 1 FROM messages WHERE thread_id = $1`
	if err := tx.QueryRowContext(ctx, seqQ, threadID).Scan(&seq); err != nil {
		return nil, fmt.Errorf("compute next seq for thread %s: %w", threadID, err)
	}

	msg := &domain.Message{
		ID:        uuid.New(),
		ThreadID:  threadID,
		Seq:       seq,
		Role:      role,
		Content:   content,
		CreatedAt: time.Now().UTC(),
	}

	const insertQ = `INSERT INTO messages (id, thread_id, seq, role, content, created_at) VALUES ($1, $2, $3, $4, $5, $6)`
	if _, err := tx.ExecContext(ctx, insertQ, msg.ID, msg.ThreadID, msg.Seq, string(msg.Role), []byte(msg.Content), msg.CreatedAt); err != nil {
		return nil, fmt.Errorf("insert message for thread %s: %w", threadID, err)
	}

	const updateQ = `UPDATE threads SET updated_at = $1 WHERE id = $2`
	if _, err := tx.ExecContext(ctx, updateQ, msg.CreatedAt, threadID); err != nil {
		return nil, fmt.Errorf("bump updated_at for thread %s: %w", threadID, err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit append message tx: %w", err)
	}
	return msg, nil
}

// ListMessages returns all messages in the thread ordered by seq ascending.
func (r *ThreadRepository) ListMessages(ctx context.Context, threadID uuid.UUID) ([]domain.Message, error) {
	const q = `SELECT id, thread_id, seq, role, content, created_at FROM messages WHERE thread_id = $1 ORDER BY seq`
	rows, err := r.db.QueryContext(ctx, q, threadID)
	if err != nil {
		return nil, fmt.Errorf("list messages for thread %s: %w", threadID, err)
	}
	defer rows.Close()

	out := []domain.Message{}
	for rows.Next() {
		var m domain.Message
		var role string
		var content []byte
		if err := rows.Scan(&m.ID, &m.ThreadID, &m.Seq, &role, &content, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		m.Role = domain.Role(role)
		m.Content = json.RawMessage(content)
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages for thread %s: %w", threadID, err)
	}
	return out, nil
}

// CreatePendingAction inserts a PendingAction, marshaling Args to JSONB.
func (r *ThreadRepository) CreatePendingAction(ctx context.Context, a *domain.PendingAction) error {
	argsJSON, err := json.Marshal(a.Args)
	if err != nil {
		return fmt.Errorf("marshal pending action args: %w", err)
	}
	const q = `INSERT INTO pending_actions (id, thread_id, tool, args, summary, status, created_at, expires_at)
	            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	if _, err := r.db.ExecContext(ctx, q, a.ID, a.ThreadID, a.Tool, argsJSON, a.Summary, string(a.Status), a.CreatedAt, a.ExpiresAt); err != nil {
		return fmt.Errorf("create pending action %s: %w", a.ID, err)
	}
	return nil
}

// ResolvePendingAction transitions a pending action from "pending" to the given
// status. Returns an error wrapping repository.ErrNotFound if the action is not
// found or has already been resolved.
func (r *ThreadRepository) ResolvePendingAction(ctx context.Context, id uuid.UUID, status domain.ActionStatus) error {
	const q = `UPDATE pending_actions SET status = $1 WHERE id = $2 AND status = $3`
	res, err := r.db.ExecContext(ctx, q, string(status), id, string(domain.ActionPending))
	if err != nil {
		return fmt.Errorf("resolve pending action %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for resolve pending action %s: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("resolve pending action %s: %w", id, repository.ErrNotFound)
	}
	return nil
}

// ListIdleThreads returns threads whose updated_at is before cutoff, ordered by
// updated_at ascending (oldest first), up to limit results. These are candidates
// for retention eviction.
func (r *ThreadRepository) ListIdleThreads(ctx context.Context, cutoff time.Time, limit int) ([]domain.Thread, error) {
	const q = `SELECT id, user_id, created_at, updated_at FROM threads WHERE updated_at < $1 ORDER BY updated_at LIMIT $2`
	rows, err := r.db.QueryContext(ctx, q, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("list idle threads: %w", err)
	}
	defer rows.Close()

	var threads []domain.Thread
	for rows.Next() {
		var t domain.Thread
		if err := rows.Scan(&t.ID, &t.UserID, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan idle thread: %w", err)
		}
		threads = append(threads, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate idle threads: %w", err)
	}
	return threads, nil
}

// DeleteThread removes the thread and all its messages and pending_actions via
// cascade delete.
func (r *ThreadRepository) DeleteThread(ctx context.Context, id uuid.UUID) error {
	const q = `DELETE FROM threads WHERE id = $1`
	if _, err := r.db.ExecContext(ctx, q, id); err != nil {
		return fmt.Errorf("delete thread %s: %w", id, err)
	}
	return nil
}
