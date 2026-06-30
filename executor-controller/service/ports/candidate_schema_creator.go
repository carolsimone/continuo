// Package ports holds executor-controller's technical (non-domain) collaborator
// interfaces. Adapters implement them; the application depends on them.
package ports

import "context"

// CandidateSchemaCreator creates a release's candidate schema in the dbt
// warehouse once, before any validation job for that release is enqueued.
//
// Root validation nodes have no gating upstreams and their jobs run in parallel.
// Each validation job would otherwise create the shared candidate schema on its own;
// `CREATE SCHEMA IF NOT EXISTS` is not atomic under concurrency, so two pods can
// race and raise a unique-violation on pg_namespace. Creating the schema exactly
// once up front removes that race for every validation node.
//
// Implementations must be idempotent (creating an existing schema is not an
// error) and race-safe against concurrent callers for the same schema name.
type CandidateSchemaCreator interface {
	EnsureCandidateSchema(ctx context.Context, schema string) error
}
