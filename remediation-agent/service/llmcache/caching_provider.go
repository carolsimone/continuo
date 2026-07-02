// Package llmcache provides a best-effort, idempotency-keyed caching decorator
// over the LLMProvider port. remediation-agent consumes remediation.requested at
// least once; if the terminal DB commit fails the trigger is redelivered and the
// whole handler re-runs, re-paying the expensive LLM call. This decorator makes
// that call effectively-once: an identical request (including a redelivery's
// identical request) reuses the prior completion.
//
// The cache is strictly best-effort: any cache Get/Put error is logged and
// treated as a miss/no-op — it is never surfaced. Only the wrapped provider's
// error propagates. The decorator depends only on ports, so application code
// keeps calling the LLMProvider port unchanged and imports no adapter.
package llmcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// keyPrefix namespaces cache keys within the shared Redis instance.
const keyPrefix = "llmcache:"

// CachingLLMProvider decorates an LLMProvider with a best-effort response cache
// keyed by the content-addressed request (model id ‖ canonical request JSON).
type CachingLLMProvider struct {
	inner   ports.LLMProvider
	cache   ports.LLMResponseCache
	modelID string
	logger  *slog.Logger
}

var _ ports.LLMProvider = (*CachingLLMProvider)(nil)

// New wraps inner with a cache. modelID is folded into the cache key so results
// from different models never collide.
func New(inner ports.LLMProvider, cache ports.LLMResponseCache, modelID string, logger *slog.Logger) *CachingLLMProvider {
	return &CachingLLMProvider{inner: inner, cache: cache, modelID: modelID, logger: logger}
}

// Propose returns a cached completion for an identical prior request, otherwise
// calls the wrapped provider and best-effort caches the result. A cache failure
// degrades to calling the provider; it never breaks the happy path.
func (p *CachingLLMProvider) Propose(ctx context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	key, err := Key(p.modelID, req)
	if err != nil {
		// A request that cannot be canonicalised simply is not cached.
		p.logger.Warn("llm cache: key derivation failed; bypassing cache", "error", err)
		return p.inner.Propose(ctx, req)
	}

	if res, ok, err := p.cache.Get(ctx, key); err != nil {
		p.logger.Warn("llm cache: get failed; treating as miss", "error", err)
	} else if ok {
		return res, nil
	}

	res, err := p.inner.Propose(ctx, req)
	if err != nil {
		return ports.ProposeResult{}, err
	}

	if err := p.cache.Put(ctx, key, res); err != nil {
		p.logger.Warn("llm cache: put failed; result not cached", "error", err)
	}
	return res, nil
}

// Key derives the content-addressed cache key for a request under a given model
// id: sha256(modelID ‖ 0x00 ‖ canonical-JSON(req)), hex-encoded and prefixed.
// The NUL separator prevents any modelID/JSON boundary ambiguity.
func Key(modelID string, req ports.ProposeRequest) (string, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(modelID))
	h.Write([]byte{0})
	h.Write(b)
	return keyPrefix + hex.EncodeToString(h.Sum(nil)), nil
}
