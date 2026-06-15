package chat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateLimiter_PerUserPerMinute(t *testing.T) {
	now := time.Now()
	rl := NewRateLimiter(2)
	assert.True(t, rl.Allow("alice", now))
	assert.True(t, rl.Allow("alice", now))
	assert.False(t, rl.Allow("alice", now))
	assert.True(t, rl.Allow("bob", now))
	assert.True(t, rl.Allow("alice", now.Add(61*time.Second)))
}
