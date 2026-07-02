package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

func newTestRedis(t *testing.T) *goredis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	return goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
}

func TestLLMResponseCache_PutGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := NewLLMResponseCache(newTestRedis(t), time.Hour)

	want := ports.ProposeResult{
		ProposedSQL:            "SELECT 1",
		ProposedContent:        "SELECT 1",
		TargetFile:             "models/a.sql",
		Rationale:              "fix the typo",
		Confidence:             "high",
		SuspectedRootCauseNode: "model.svc.upstream",
		Model:                  "claude-x",
	}

	if err := c.Put(ctx, "llmcache:abc", want); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, ok, err := c.Get(ctx, "llmcache:abc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("expected cache hit, got miss")
	}
	if got != want {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestLLMResponseCache_MissReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	c := NewLLMResponseCache(newTestRedis(t), time.Hour)

	_, ok, err := c.Get(ctx, "llmcache:missing")
	if err != nil {
		t.Fatalf("Get on missing key must not error, got: %v", err)
	}
	if ok {
		t.Fatal("expected miss for absent key, got hit")
	}
}

func TestLLMResponseCache_PutSetsTTL(t *testing.T) {
	ctx := context.Background()
	client := newTestRedis(t)
	c := NewLLMResponseCache(client, time.Hour)

	if err := c.Put(ctx, "llmcache:ttl", ports.ProposeResult{Model: "m"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ttl, err := client.TTL(ctx, "llmcache:ttl").Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl <= 0 || ttl > time.Hour {
		t.Fatalf("expected a positive TTL <= 1h, got %v", ttl)
	}
}
