package s3

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// CandidateSQLDeleter implements ports.CandidateSQLDeleter over an
// object-store prefix deleter. The prune that calls it soft-fails and
// discards the error, so a failed delete is logged here as a warning before
// it is returned; the bucket's lifecycle rules reclaim what is left.
type CandidateSQLDeleter struct {
	objects objectstore.PrefixDeleter
	logger  *slog.Logger
}

var _ ports.CandidateSQLDeleter = (*CandidateSQLDeleter)(nil)

// NewCandidateSQLDeleter builds a CandidateSQLDeleter over objects, which
// deletes in the store's default bucket.
func NewCandidateSQLDeleter(objects objectstore.PrefixDeleter, logger *slog.Logger) *CandidateSQLDeleter {
	return &CandidateSQLDeleter{objects: objects, logger: logger}
}

// DeletePrefix removes every object whose key begins with prefix. Nothing
// matching is a no-op and returns nil.
func (d *CandidateSQLDeleter) DeletePrefix(ctx context.Context, prefix string) error {
	if err := d.objects.DeletePrefix(ctx, prefix); err != nil {
		d.logger.Warn("object-storage prefix delete failed", "prefix", prefix, "error", err)
		return err
	}
	return nil
}
