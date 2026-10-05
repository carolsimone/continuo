//go:build integration

package outbox

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/testdeps"
	goredis "github.com/redis/go-redis/v9"
)

// TestStringifyFields_MatchesGoRedisEncoding writes each value type through
// go-redis's XADD and reads it back: StringifyFields must produce exactly what
// Redis stored, so a redriven event is byte-identical to the original publish.
func TestStringifyFields_MatchesGoRedisEncoding(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		testdeps.Unavailable(t, "REDIS_ADDR not set — skipping Redis integration test")
	}
	rc := goredis.NewClient(&goredis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD")})
	defer rc.Close()
	ctx := context.Background()
	stream := "test.outbox.render:" + time.Now().Format("150405.000000000")
	defer rc.Del(ctx, stream)

	values := map[string]any{
		"s": "x", "b": []byte("y"), "i": 42, "i64": int64(-7), "u": uint32(9),
		"f": 1.25, "f32": float32(0.1), "t": true, "fa": false,
		"ts": time.Date(2026, 10, 5, 1, 2, 3, 4, time.UTC),
	}
	if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: values}).Err(); err != nil {
		t.Fatal(err)
	}
	msgs, err := rc.XRange(ctx, stream, "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("xrange: %v %d", err, len(msgs))
	}
	got, err := StringifyFields(values)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range msgs[0].Values {
		if got[k] != v.(string) {
			t.Errorf("%s: StringifyFields=%q, Redis stored %q", k, got[k], v)
		}
	}
}
