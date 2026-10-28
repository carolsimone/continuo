package commands_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/service/commands"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// agedRequests is a memory repository whose DeleteOlderThan records its age
// and answers deleted.
type agedRequests struct {
	*memoryRequests
	age     time.Duration
	deleted int64
}

func (a *agedRequests) DeleteOlderThan(_ context.Context, age time.Duration) (int64, error) {
	a.age = age
	return a.deleted, nil
}

// The pruner deletes the requests older than the retention it is given and
// reports how many went.
func TestPruner_DeletesTheRequestsOlderThanTheRetention(t *testing.T) {
	requests := &agedRequests{memoryRequests: newMemoryRequests(), deleted: 3}
	newUoW, u := fakeUoW(requests.memoryRequests)
	u.CommandRequestRepo = requests

	n, err := commands.NewPruner(newUoW).Prune(context.Background(), 30*24*time.Hour, 1000)

	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, 30*24*time.Hour, requests.age)
}
