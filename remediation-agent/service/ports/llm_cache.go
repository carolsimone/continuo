package ports

import "context"

// LLMResponseCache is a best-effort, idempotency-keyed cache for LLM propose
// results. It lets an identical ProposeRequest (including a redelivered
// message's identical request) reuse a prior completion instead of re-paying
// the LLM call. Implementations are best-effort: callers treat any error as a
// cache miss / no-op and must never let a cache failure break the happy path.
type LLMResponseCache interface {
	// Get returns the cached result for key. The boolean is false on a miss.
	// An error indicates the cache backend failed; callers treat it as a miss.
	Get(ctx context.Context, key string) (ProposeResult, bool, error)
	// Put stores result under key with the implementation's configured TTL.
	Put(ctx context.Context, key string, result ProposeResult) error
}
