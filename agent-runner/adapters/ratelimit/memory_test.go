package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemory_PerUserPerMinute(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	rl := NewMemory(2)
	rl.now = func() time.Time { return now }

	allow := func() bool {
		ok, err := rl.Allow(ctx, "alice")
		require.NoError(t, err)
		return ok
	}

	assert.True(t, allow())
	assert.True(t, allow())
	assert.False(t, allow(), "third message in the window is rejected")

	// A different user is tracked independently.
	ok, err := rl.Allow(ctx, "bob")
	require.NoError(t, err)
	assert.True(t, ok)

	// After the window slides past the earlier hits, alice is allowed again.
	now = now.Add(61 * time.Second)
	assert.True(t, allow())
}
