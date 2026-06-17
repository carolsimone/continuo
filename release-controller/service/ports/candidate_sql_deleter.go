package ports

import "context"

// CandidateSQLDeleter removes all candidate-SQL objects stored under an S3
// prefix. Implemented by an S3 adapter; used at prune time to reclaim the
// candidate SQL of a release being deleted.
type CandidateSQLDeleter interface {
	// DeletePrefix removes every object whose key begins with prefix.
	// A no-op (and nil error) when nothing matches.
	DeletePrefix(ctx context.Context, prefix string) error
}
