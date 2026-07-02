// Package llmcache provides a best-effort, idempotency-keyed caching decorator
// over ports.LLMProvider. The remediation.requested consumer is at-least-once:
// if the terminal DB commit fails the message redelivers and the whole handler
// re-runs, which would re-pay the expensive LLM call. Wrapping the provider with
// this decorator lets an identical request — including a redelivery's identical
// request — reuse the prior completion.
//
// The decorator composes two ports (the real LLMProvider and an
// LLMResponseCache) and imports no adapter, so it lives in the application layer.
package llmcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// keyPrefix namespaces every cache key so LLM entries never collide with other
// keys in the shared Redis instance.
const keyPrefix = "llmcache:"

// CachingLLMProvider decorates a real ports.LLMProvider with a best-effort
// response cache. A cache hit skips the wrapped provider; a miss calls it and
// stores the result. Any cache error is logged and treated as a miss/no-op and
// is never surfaced to the caller — only the wrapped provider's own error
// propagates.
type CachingLLMProvider struct {
	inner   ports.LLMProvider
	cache   ports.LLMResponseCache
	modelID string
	logger  *slog.Logger
}

var _ ports.LLMProvider = (*CachingLLMProvider)(nil)

// New builds a caching decorator around inner. modelID is folded into the cache
// key so results from different models never collide. logger must be non-nil.
func New(inner ports.LLMProvider, cache ports.LLMResponseCache, modelID string, logger *slog.Logger) *CachingLLMProvider {
	return &CachingLLMProvider{inner: inner, cache: cache, modelID: modelID, logger: logger}
}

// Propose returns a cached result for an identical request when one exists,
// otherwise calls the wrapped provider and best-effort stores the result.
func (p *CachingLLMProvider) Propose(ctx context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	key, err := CacheKey(p.modelID, req)
	if err != nil {
		// Keying should never fail for a JSON-serializable request; if it does,
		// bypass the cache entirely rather than break the happy path.
		p.logger.Warn("llm cache key computation failed; bypassing cache", "error", err)
		return p.inner.Propose(ctx, req)
	}

	if res, ok, gerr := p.cache.Get(ctx, key); gerr != nil {
		p.logger.Warn("llm cache get failed; treating as miss", "error", gerr)
	} else if ok {
		p.logger.Debug("llm cache hit", "key", key)
		return res, nil
	}

	res, err := p.inner.Propose(ctx, req)
	if err != nil {
		return ports.ProposeResult{}, err
	}

	if perr := p.cache.Put(ctx, key, res); perr != nil {
		p.logger.Warn("llm cache put failed; result not cached", "error", perr)
	}
	return res, nil
}

// CacheKey derives the idempotency key for a request. It is the SHA-256 of the
// model id concatenated with the canonical JSON encoding of the request, hex
// encoded and namespaced. Go's json.Marshal emits struct fields in declaration
// order, so identical requests yield identical bytes and thus identical keys.
func CacheKey(modelID string, req ports.ProposeRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(modelID))
	h.Write([]byte{0}) // separator so modelID and body cannot run together
	h.Write(body)
	return keyPrefix + hex.EncodeToString(h.Sum(nil)), nil
}
