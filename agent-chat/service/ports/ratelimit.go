package ports

import "context"

// RateLimiter caps how many user messages a single user may submit per minute.
// Implementations may be process-local (single replica) or backed by a shared
// store (global across replicas). Allow reports whether this attempt is within
// the quota; the error is non-nil only when the limiter backend fails, letting
// callers decide their fail-open / fail-closed policy.
type RateLimiter interface {
	Allow(ctx context.Context, userID string) (allowed bool, err error)
}
