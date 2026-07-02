// Package llmcache provides a best-effort, idempotency-keyed caching decorator
// over the LLMProvider port. remediation-agent consumes remediation.requested at
// least once; if the terminal DB commit fails the same trigger is redelivered
// and the whole handler re-runs, re-paying the expensive LLM call. This decorator
// makes that call effectively-once per trigger: a redelivery of the same trigger
// reuses the prior completion, while a genuinely new trigger (e.g. a later
// remediation attempt for the same failure) always gets a fresh provider call.
//
// The cache key is therefore scoped to the inbound trigger's idempotency key —
// supplied by the driver via ContextWithIdempotencyKey — not just the request
// content. Keying by content alone would let two distinct triggers that happen
// to build a byte-identical request collide, replaying an earlier (possibly
// failed) result and burning the attempt cap without asking the model again.
//
// The cache is strictly best-effort: any cache Get/Put error is logged and
// treated as a miss/no-op — it is never surfaced. Only the wrapped provider's
// error propagates. When no idempotency key is present on the context the
// decorator cannot tell a redelivery from a new trigger, so it bypasses the
// cache and calls the provider directly. The decorator depends only on ports, so
// application code keeps calling the LLMProvider port unchanged and imports no
// adapter.
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

// idempotencyKeyCtx is the unexported context key under which the driver stores
// the inbound trigger's idempotency key.
type idempotencyKeyCtx struct{}

// ContextWithIdempotencyKey returns a context carrying the inbound trigger's
// idempotency key. The driver sets it before dispatching to a Fixer so the LLM
// call made by that Fixer is cached per trigger. An empty key is treated as
// absent (the decorator bypasses the cache).
func ContextWithIdempotencyKey(ctx context.Context, key string) context.Context {
	if key == "" {
		return ctx
	}
	return context.WithValue(ctx, idempotencyKeyCtx{}, key)
}

// idempotencyKeyFromContext returns the inbound trigger's idempotency key and
// whether one was present.
func idempotencyKeyFromContext(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(idempotencyKeyCtx{}).(string)
	return v, ok && v != ""
}

// CachingLLMProvider decorates an LLMProvider with a best-effort response cache
// keyed by the inbound trigger's idempotency key plus the model id and request.
type CachingLLMProvider struct {
	provider ports.LLMProvider
	cache    ports.LLMResponseCache
	modelID  string
	logger   *slog.Logger
}

var _ ports.LLMProvider = (*CachingLLMProvider)(nil)

// New wraps the provider with a cache. modelID is folded into the cache key so
// results from different models never collide.
func New(provider ports.LLMProvider, cache ports.LLMResponseCache, modelID string, logger *slog.Logger) *CachingLLMProvider {
	return &CachingLLMProvider{provider: provider, cache: cache, modelID: modelID, logger: logger}
}

// Propose returns a cached completion when the same trigger already produced one
// for an identical request, otherwise calls the wrapped provider and best-effort
// caches the result. Without an idempotency key on the context, or on any cache
// failure, it degrades to calling the provider; it never breaks the happy path.
func (p *CachingLLMProvider) Propose(ctx context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	idemKey, ok := idempotencyKeyFromContext(ctx)
	if !ok {
		// No trigger identity: cannot distinguish a redelivery from a new
		// trigger, so caching would risk replaying a result across distinct
		// triggers. Bypass the cache.
		return p.provider.Propose(ctx, req)
	}

	key, err := Key(p.modelID, idemKey, req)
	if err != nil {
		// A request that cannot be canonicalised simply is not cached.
		p.logger.Warn("llm cache: key derivation failed; bypassing cache", "error", err)
		return p.provider.Propose(ctx, req)
	}

	if res, ok, err := p.cache.Get(ctx, key); err != nil {
		p.logger.Warn("llm cache: get failed; treating as miss", "error", err)
	} else if ok {
		return res, nil
	}

	res, err := p.provider.Propose(ctx, req)
	if err != nil {
		return ports.ProposeResult{}, err
	}

	if err := p.cache.Put(ctx, key, res); err != nil {
		p.logger.Warn("llm cache: put failed; result not cached", "error", err)
	}
	return res, nil
}

// Key derives the cache key for a request under a given model id and inbound
// trigger idempotency key: sha256(modelID ‖ 0x00 ‖ idempotencyKey ‖ 0x00 ‖
// canonical-JSON(req)), hex-encoded and prefixed. The NUL separators prevent any
// boundary ambiguity between the three components.
func Key(modelID, idempotencyKey string, req ports.ProposeRequest) (string, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(modelID))
	h.Write([]byte{0})
	h.Write([]byte(idempotencyKey))
	h.Write([]byte{0})
	h.Write(b)
	return keyPrefix + hex.EncodeToString(h.Sum(nil)), nil
}
