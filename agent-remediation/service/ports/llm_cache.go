package ports

import "context"

// LLMResponseCache is a best-effort, idempotency-keyed store for LLM propose
// results. It lets a redelivered remediation.requested trigger reuse the prior
// completion instead of re-paying the expensive LLM call. It is a technical
// collaborator (not a domain concept), so it lives in service/ports and is
// implemented by an adapter.
//
// Every method is best-effort from the caller's perspective: a store that is
// unavailable or errors must degrade to a cache miss, never break the happy
// path. Get returns (result, found, error); found is false on a miss.
type LLMResponseCache interface {
	Get(ctx context.Context, key string) (ProposeResult, bool, error)
	Put(ctx context.Context, key string, result ProposeResult) error
}
