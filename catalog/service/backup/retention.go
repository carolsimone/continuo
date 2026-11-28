package backup

import (
	"context"
)

// prune deletes the tenant's backups beyond the newest keep, after stored was
// just stored. stored always counts as one of the kept, so retention never
// deletes the backup it follows, even when the store holds backups dated
// after it; the other keep-1 kept are the newest of the rest. A failure to
// list or to delete is logged and leaves those backups in place for the next
// backup's retention, which deletes everything beyond the newest keep again.
func (e *Taker) prune(ctx context.Context, tenantID, stored string) {
	objects, err := e.store.List(ctx, tenantID)
	if err != nil {
		e.logger.Error("catalog backup retention could not list the tenant's backups; none were deleted",
			"tenant_id", tenantID, "error", err)
		return
	}
	kept, deleted, failed := 1, 0, 0
	for _, o := range objects {
		if o.Key == stored {
			continue
		}
		if kept < e.keep {
			kept++
			continue
		}
		if err := e.store.Delete(ctx, o.Key); err != nil {
			failed++
			e.logger.Error("catalog backup retention could not delete a backup", "tenant_id", tenantID, "object_key", o.Key, "error", err)
			continue
		}
		deleted++
	}
	if deleted > 0 || failed > 0 {
		e.logger.Info("catalog backup retention", "tenant_id", tenantID, "keep", e.keep, "deleted", deleted, "failed", failed)
	}
}
