package backup

import (
	"context"
)

// prune deletes the tenant's exports beyond the newest keep, after stored was
// just stored. stored always counts as one of the kept, so retention never
// deletes the export it follows, even when the store holds exports dated
// after it; the other keep-1 kept are the newest of the rest. A failure to
// list or to delete is logged and leaves those exports in place for the next
// export's retention, which deletes everything beyond the newest keep again.
func (e *Exporter) prune(ctx context.Context, tenantID, stored string) {
	objects, err := e.store.List(ctx, tenantID)
	if err != nil {
		e.logger.Error("catalog export retention could not list the tenant's exports; none were deleted",
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
			e.logger.Error("catalog export retention could not delete an export", "tenant_id", tenantID, "object_key", o.Key, "error", err)
			continue
		}
		deleted++
	}
	if deleted > 0 || failed > 0 {
		e.logger.Info("catalog export retention", "tenant_id", tenantID, "keep", e.keep, "deleted", deleted, "failed", failed)
	}
}
