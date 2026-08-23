package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPendingAction_IsExpired(t *testing.T) {
	now := time.Now()
	a := PendingAction{Status: ActionPending, ExpiresAt: now.Add(time.Minute)}
	assert.False(t, a.IsExpired(now))
	assert.True(t, a.IsExpired(now.Add(2*time.Minute)))
}

func TestPendingAction_OnlyPendingResolves(t *testing.T) {
	a := PendingAction{Status: ActionPending}
	assert.NoError(t, a.Resolve(ActionApproved))
	assert.Equal(t, ActionApproved, a.Status)
	assert.Error(t, a.Resolve(ActionDenied), "already-resolved action must not transition again")
}
