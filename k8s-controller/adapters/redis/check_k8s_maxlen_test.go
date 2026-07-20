package redis

import (
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
)

// TestRecirculateArgs_SetsMaxLenApprox asserts the not-yet-due recirculation
// XAdd is capped, so the busy-wait re-add cannot grow check.k8s:v1 unbounded
// (issue #282). This is a Phase-1 backstop; Phase 2 removes the recirculation.
func TestRecirculateArgs_SetsMaxLenApprox(t *testing.T) {
	args := recirculateArgs(map[string]interface{}{"check_after": "123"})

	assert.Equal(t, streams.CheckK8sV1, args.Stream)
	assert.Equal(t, int64(checkStreamMaxLen), args.MaxLen, "MaxLen cap must be applied")
	assert.True(t, args.Approx, "cap must use approximate (~) trimming")
	assert.Equal(t, int64(10000), args.MaxLen, "cap must match the project-wide convention")
}
