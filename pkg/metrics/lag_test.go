package metrics_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/metrics"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/testdeps"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumers_LagAndPendingAreReadAtScrape(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		testdeps.Unavailable(t, "REDIS_ADDR not set; run `make test-go SERVICE=pkg`")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	defer rc.Close()
	ctx := context.Background()
	stream := fmt.Sprintf("test-metrics-lag-%d", time.Now().UnixNano())
	key := pkgredis.Key(stream)
	t.Cleanup(func() { rc.Del(ctx, key) })
	require.NoError(t, rc.XGroupCreateMkStream(ctx, key, "g", "0").Err())
	for i := 0; i < 3; i++ {
		require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: key, Values: map[string]any{"i": i}}).Err())
	}
	_, err := rc.XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{key, ">"}, Count: 1}).Result()
	require.NoError(t, err)

	reg := metrics.New("test-svc")
	reg.WatchRedis(rc)
	reg.Consumers().Watch(stream, "g")
	body := scrape(t, reg.Handler())
	// The gauges are labelled with the contract stream name and read the prefixed key.
	assert.Contains(t, body, fmt.Sprintf(`continuo_consumer_lag{group="g",service="test-svc",stream="%s"} 2`, stream))
	assert.Contains(t, body, fmt.Sprintf(`continuo_consumer_pending{group="g",service="test-svc",stream="%s"} 1`, stream))
}
