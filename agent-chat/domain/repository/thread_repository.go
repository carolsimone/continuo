package repository

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/google/uuid"
)

// ErrNotFound is returned when a thread or pending action does not exist (or is
// not owned by the requesting user). Callers map it to a not-found result.
var ErrNotFound = errors.New("agent-chat: not found")

// ThreadRepository persists conversations and pending confirmations.
type ThreadRepository interface {
	CreateThread(ctx context.Context, userID string) (*domain.Thread, error)
	// GetThread returns the thread only if it belongs to userID (ownership check).
	GetThread(ctx context.Context, id uuid.UUID, userID string) (*domain.Thread, error)
	// AppendMessage assigns the next seq and bumps the thread's updated_at.
	AppendMessage(ctx context.Context, threadID uuid.UUID, role domain.Role, content json.RawMessage) (*domain.Message, error)
	ListMessages(ctx context.Context, threadID uuid.UUID) ([]domain.Message, error)
	CreatePendingAction(ctx context.Context, a *domain.PendingAction) error
	ResolvePendingAction(ctx context.Context, id uuid.UUID, status domain.ActionStatus) error
	// GetPendingAction returns the most recent still-pending, non-expired action
	// for a thread, used to resume a confirmation after a reconnect. It returns
	// ErrNotFound when no such action exists.
	GetPendingAction(ctx context.Context, threadID uuid.UUID) (*domain.PendingAction, error)
	// ListIdleThreads returns threads not updated since cutoff (retention candidates).
	ListIdleThreads(ctx context.Context, cutoff time.Time, limit int) ([]domain.Thread, error)
	DeleteThread(ctx context.Context, id uuid.UUID) error
}
