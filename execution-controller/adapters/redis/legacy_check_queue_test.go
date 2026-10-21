package redis_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	executorredis "github.com/carolsimone/continuo/execution-controller/adapters/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLegacyCheckQueue_ClearDeletesBothKeysAndCountsThem(t *testing.T) {
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()
	require.NoError(t, client.ZAdd(ctx, executorredis.LegacyCheckPendingKey, goredis.Z{Score: 1, Member: "job-a"}).Err())
	require.NoError(t, client.HSet(ctx, executorredis.LegacyCheckTicketsKey, "job-a", "{}").Err())

	q := executorredis.NewLegacyCheckQueue(client)
	n, err := q.Clear(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.False(t, mr.Exists(executorredis.LegacyCheckPendingKey))
	assert.False(t, mr.Exists(executorredis.LegacyCheckTicketsKey))

	n, err = q.Clear(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "nothing left to delete")
}
