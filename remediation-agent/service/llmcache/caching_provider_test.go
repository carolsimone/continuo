package llmcache

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// fakeProvider is a stub LLMProvider that records how many times it was called
// and returns a fixed result (or error).
type fakeProvider struct {
	calls  int
	result ports.ProposeResult
	err    error
}

func (f *fakeProvider) Propose(_ context.Context, _ ports.ProposeRequest) (ports.ProposeResult, error) {
	f.calls++
	if f.err != nil {
		return ports.ProposeResult{}, f.err
	}
	return f.result, nil
}

// fakeCache is an in-memory LLMResponseCache whose Get/Put can be forced to
// error, to exercise the decorator's best-effort fall-through.
type fakeCache struct {
	store   map[string]ports.ProposeResult
	getErr  error
	putErr  error
	getKeys []string
	putKeys []string
}

func newFakeCache() *fakeCache {
	return &fakeCache{store: map[string]ports.ProposeResult{}}
}

func (c *fakeCache) Get(_ context.Context, key string) (ports.ProposeResult, bool, error) {
	c.getKeys = append(c.getKeys, key)
	if c.getErr != nil {
		return ports.ProposeResult{}, false, c.getErr
	}
	res, ok := c.store[key]
	return res, ok, nil
}

func (c *fakeCache) Put(_ context.Context, key string, result ports.ProposeResult) error {
	c.putKeys = append(c.putKeys, key)
	if c.putErr != nil {
		return c.putErr
	}
	c.store[key] = result
	return nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func sampleReq() ports.ProposeRequest {
	return ports.ProposeRequest{System: "sys", User: "user", ToolName: "propose_fix"}
}

func sampleResult() ports.ProposeResult {
	return ports.ProposeResult{ProposedContent: "SELECT 1", Rationale: "r", Confidence: "high", Model: "m"}
}

// Miss: provider is called once and the result is written to the cache.
func TestPropose_MissCallsProviderThenPuts(t *testing.T) {
	provider := &fakeProvider{result: sampleResult()}
	cache := newFakeCache()
	p := New(provider, cache, "model-x", testLogger())

	got, err := p.Propose(context.Background(), sampleReq())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != sampleResult() {
		t.Fatalf("result mismatch: got %+v", got)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
	if len(cache.putKeys) != 1 {
		t.Fatalf("cache Put count = %d, want 1", len(cache.putKeys))
	}
}

// Hit: a pre-populated cache short-circuits the provider entirely.
func TestPropose_HitSkipsProvider(t *testing.T) {
	provider := &fakeProvider{result: ports.ProposeResult{ProposedContent: "FROM PROVIDER"}}
	cache := newFakeCache()
	p := New(provider, cache, "model-x", testLogger())

	key, err := Key("model-x", sampleReq())
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	cached := ports.ProposeResult{ProposedContent: "FROM CACHE"}
	cache.store[key] = cached

	got, err := p.Propose(context.Background(), sampleReq())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != cached {
		t.Fatalf("expected cached result, got %+v", got)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0 on cache hit", provider.calls)
	}
	if len(cache.putKeys) != 0 {
		t.Fatalf("cache Put count = %d, want 0 on cache hit", len(cache.putKeys))
	}
}

// Get error: the decorator treats it as a miss and falls through to the provider
// without surfacing the error.
func TestPropose_GetErrorFallsThrough(t *testing.T) {
	provider := &fakeProvider{result: sampleResult()}
	cache := newFakeCache()
	cache.getErr = errors.New("redis down")
	p := New(provider, cache, "model-x", testLogger())

	got, err := p.Propose(context.Background(), sampleReq())
	if err != nil {
		t.Fatalf("Get error must not surface, got: %v", err)
	}
	if got != sampleResult() {
		t.Fatalf("result mismatch: got %+v", got)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
}

// Put error: the provider result is still returned; the error is swallowed.
func TestPropose_PutErrorReturnsResult(t *testing.T) {
	provider := &fakeProvider{result: sampleResult()}
	cache := newFakeCache()
	cache.putErr = errors.New("redis down")
	p := New(provider, cache, "model-x", testLogger())

	got, err := p.Propose(context.Background(), sampleReq())
	if err != nil {
		t.Fatalf("Put error must not surface, got: %v", err)
	}
	if got != sampleResult() {
		t.Fatalf("result mismatch: got %+v", got)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
}

// Provider error: it propagates, and nothing is written to the cache.
func TestPropose_ProviderErrorPropagatesNoPut(t *testing.T) {
	wantErr := errors.New("llm exploded")
	provider := &fakeProvider{err: wantErr}
	cache := newFakeCache()
	p := New(provider, cache, "model-x", testLogger())

	_, err := p.Propose(context.Background(), sampleReq())
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected provider error to propagate, got: %v", err)
	}
	if len(cache.putKeys) != 0 {
		t.Fatalf("cache Put count = %d, want 0 on provider error", len(cache.putKeys))
	}
}

// Key stability: an identical request yields the same key; a changed field or a
// changed model id yields a different key.
func TestKey_Stability(t *testing.T) {
	base := sampleReq()

	k1, err := Key("model-x", base)
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	k1again, err := Key("model-x", sampleReq())
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if k1 != k1again {
		t.Fatalf("identical request produced different keys: %q vs %q", k1, k1again)
	}

	changed := base
	changed.User = "different user"
	k2, err := Key("model-x", changed)
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if k1 == k2 {
		t.Fatal("changed request field produced the same key")
	}

	k3, err := Key("model-y", base)
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if k1 == k3 {
		t.Fatal("changed model id produced the same key")
	}

	const prefix = "llmcache:"
	if len(k1) <= len(prefix) || k1[:len(prefix)] != prefix {
		t.Fatalf("key missing %q prefix: %q", prefix, k1)
	}
}
