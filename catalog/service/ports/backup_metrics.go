package ports

import "time"

// The outcomes of one export of a tenant's graph.
const (
	// ExportSuccess is an export whose object was stored.
	ExportSuccess = "success"
	// ExportFailure is an export that stored nothing: the store refused it,
	// the graph could not be read, the lease was lost, or the process stopped.
	ExportFailure = "failure"
	// ExportSkipped is an export that did not start because another export or
	// a restore held the tenant's lease.
	ExportSkipped = "skipped"
)

// BackupMetrics observes the exports this process runs.
type BackupMetrics interface {
	// ExportObserved records one export's outcome (ExportSuccess,
	// ExportFailure or ExportSkipped). For a success, d is how long the
	// export held the tenant's lease (the time its writes were paused),
	// bytes the size of the stored object, and nodes and rels the records it
	// holds; the other outcomes pass zeros.
	ExportObserved(outcome string, d time.Duration, bytes int64, nodes, rels int64)
}
