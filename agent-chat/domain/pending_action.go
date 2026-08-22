package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ActionStatus is the lifecycle of a mutating tool call awaiting human approval.
type ActionStatus string

const (
	ActionPending  ActionStatus = "pending"
	ActionApproved ActionStatus = "approved"
	ActionDenied   ActionStatus = "denied"
	ActionExpired  ActionStatus = "expired"
)

// PendingAction is a mutating tool call paused until the user confirms or denies it.
type PendingAction struct {
	ID        uuid.UUID
	ThreadID  uuid.UUID
	Tool      string
	Args      map[string]string
	Summary   string
	Status    ActionStatus
	CreatedAt time.Time
	ExpiresAt time.Time
}

// IsExpired reports whether the action's approval window has passed.
func (a *PendingAction) IsExpired(now time.Time) bool {
	return now.After(a.ExpiresAt)
}

// Resolve transitions a pending action to a terminal status exactly once.
// Returns an error if the action has already been resolved.
func (a *PendingAction) Resolve(to ActionStatus) error {
	if a.Status != ActionPending {
		return fmt.Errorf("pending action %s already resolved to %s", a.ID, a.Status)
	}
	a.Status = to
	return nil
}
