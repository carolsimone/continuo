//go:build integration

package metrics

import (
	"context"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

func TestWatchStreams_ReportsLengthAndMemory(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		testdeps.Unavailable(t, "REDIS_ADDR not set — skipping Redis integration test")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	t.Cleanup(func() { _ = rc.Close() })
	ctx := context.Background()
	stream := "test.metrics:" + uuid.NewString()
	missing := "test.metrics.missing:" + uuid.NewString()
	t.Cleanup(func() { rc.Del(ctx, stream) })
	for i := 0; i < 2; i++ {
		if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: map[string]any{"k": "v"}}).Err(); err != nil {
			t.Fatal(err)
		}
	}

	reg := pkgmetrics.New("dead-letter-controller")
	WatchStreams(reg, rc, []string{stream, missing})
	body := scrape(reg)

	for _, want := range []string{
		`continuo_stream_length{service="dead-letter-controller",stream="` + stream + `"} 2`,
		`continuo_stream_length{service="dead-letter-controller",stream="` + missing + `"} 0`,
		`continuo_redis_maxmemory_bytes{service="dead-letter-controller"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s\n%s", want, body)
		}
	}
	m := regexp.MustCompile(`(?m)^continuo_redis_used_memory_bytes\{service="dead-letter-controller"\} (\S+)$`).FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("missing continuo_redis_used_memory_bytes\n%s", body)
	}
	if v, err := strconv.ParseFloat(m[1], 64); err != nil || v <= 0 {
		t.Errorf("used memory = %q, want a positive number", m[1])
	}
}
