package llmcache

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// fakeProvider is a controllable ports.LLMProvider that records call count.
type fakeProvider struct {
	result ports.ProposeResult
	err    error
	calls  int
}

func (f *fakeProvider) Propose(_ context.Context, _ ports.ProposeRequest) (ports.ProposeResult, error) {
	f.calls++
	if f.err != nil {
		return ports.ProposeResult{}, f.err
	}
	return f.result, nil
}

// fakeCache is a controllable ports.LLMResponseCache. It can force Get/Put
// errors and records what was Put.
type fakeCache struct {
	store   map[string]ports.ProposeResult
	getErr  error
	putErr  error
	getHits int
	puts    int
	lastKey string
}

func newFakeCache() *fakeCache {
	return &fakeCache{store: map[string]ports.ProposeResult{}}
}

func (f *fakeCache) Get(_ context.Context, key string) (ports.ProposeResult, bool, error) {
	if f.getErr != nil {
		return ports.ProposeResult{}, false, f.getErr
	}
	v, ok := f.store[key]
	if ok {
		f.getHits++
	}
	return v, ok, nil
}

func (f *fakeCache) Put(_ context.Context, key string, result ports.ProposeResult) error {
	f.puts++
	f.lastKey = key
	if f.putErr != nil {
		return f.putErr
	}
	f.store[key] = result
	return nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPropose_MissCallsProviderThenPuts(t *testing.T) {
	inner := &fakeProvider{result: ports.ProposeResult{ProposedSQL: "SELECT 1", Model: "m1"}}
	cache := newFakeCache()
	p := New(inner, cache, "m1", testLogger())

	req := ports.ProposeRequest{System: "s", User: "u", ToolName: "propose_fix"}
	got, err := p.Propose(context.Background(), req)

	require.NoError(t, err)
	assert.Equal(t, inner.result, got)
	assert.Equal(t, 1, inner.calls, "provider called exactly once on a miss")
	assert.Equal(t, 1, cache.puts, "result stored on a miss")
}

func TestPropose_HitSkipsProvider(t *testing.T) {
	inner := &fakeProvider{result: ports.ProposeResult{ProposedSQL: "provider", Model: "m1"}}
	cache := newFakeCache()
	req := ports.ProposeRequest{System: "s", User: "u", ToolName: "propose_fix"}

	key, err := CacheKey("m1", req)
	require.NoError(t, err)
	cached := ports.ProposeResult{ProposedSQL: "cached", Model: "m1"}
	cache.store[key] = cached

	p := New(inner, cache, "m1", testLogger())
	got, err := p.Propose(context.Background(), req)

	require.NoError(t, err)
	assert.Equal(t, cached, got, "cached result returned")
	assert.Equal(t, 0, inner.calls, "provider not called on a hit")
	assert.Equal(t, 0, cache.puts, "no re-Put on a hit")
}

func TestPropose_GetErrorFallsThroughToProvider(t *testing.T) {
	inner := &fakeProvider{result: ports.ProposeResult{ProposedSQL: "provider", Model: "m1"}}
	cache := newFakeCache()
	cache.getErr = errors.New("redis down")

	p := New(inner, cache, "m1", testLogger())
	req := ports.ProposeRequest{System: "s", User: "u"}
	got, err := p.Propose(context.Background(), req)

	require.NoError(t, err, "cache Get error is never surfaced")
	assert.Equal(t, inner.result, got)
	assert.Equal(t, 1, inner.calls, "provider called after a Get error")
}

func TestPropose_PutErrorStillReturnsResult(t *testing.T) {
	inner := &fakeProvider{result: ports.ProposeResult{ProposedSQL: "provider", Model: "m1"}}
	cache := newFakeCache()
	cache.putErr = errors.New("redis write failed")

	p := New(inner, cache, "m1", testLogger())
	req := ports.ProposeRequest{System: "s", User: "u"}
	got, err := p.Propose(context.Background(), req)

	require.NoError(t, err, "cache Put error is never surfaced")
	assert.Equal(t, inner.result, got)
	assert.Equal(t, 1, cache.puts, "a Put was attempted")
}

func TestPropose_ProviderErrorPropagatedNoPut(t *testing.T) {
	provErr := errors.New("provider blew up")
	inner := &fakeProvider{err: provErr}
	cache := newFakeCache()

	p := New(inner, cache, "m1", testLogger())
	req := ports.ProposeRequest{System: "s", User: "u"}
	_, err := p.Propose(context.Background(), req)

	require.ErrorIs(t, err, provErr, "provider error propagates")
	assert.Equal(t, 0, cache.puts, "no Put when the provider fails")
}

func TestCacheKey_StableAndSensitive(t *testing.T) {
	req := ports.ProposeRequest{System: "s", User: "u", ToolName: "propose_fix"}

	k1, err := CacheKey("m1", req)
	require.NoError(t, err)
	k2, err := CacheKey("m1", req)
	require.NoError(t, err)
	assert.Equal(t, k1, k2, "identical requests produce the same key")
	assert.Contains(t, k1, "llmcache:", "key is namespaced")

	// A changed request field changes the key.
	req2 := req
	req2.User = "different user prompt"
	k3, err := CacheKey("m1", req2)
	require.NoError(t, err)
	assert.NotEqual(t, k1, k3, "a changed request field changes the key")

	// A different model id changes the key.
	k4, err := CacheKey("m2", req)
	require.NoError(t, err)
	assert.NotEqual(t, k1, k4, "a different model id changes the key")
}
