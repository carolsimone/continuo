// Package ports holds executor-controller's technical (non-domain) collaborator
// interfaces. Adapters implement them; the application depends on them.
package ports

import "context"

// CandidateSchemaCleaner drops a release's candidate schema from the dbt
// warehouse once validation is terminal. Validation runs are --empty, so the
// candidate schema is always disposable regardless of promote/reject outcome.
// Implementations must be idempotent (dropping a missing schema is not an error).
type CandidateSchemaCleaner interface {
	DropCandidateSchema(ctx context.Context, schema string) error
}
