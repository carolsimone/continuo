package ports

import "time"

// The outcomes of one backup of a tenant's graph.
const (
	// BackupSuccess is a backup whose object was stored.
	BackupSuccess = "success"
	// BackupFailure is a backup that stored nothing: the store refused it,
	// the graph could not be read, the lease was lost, or the process stopped.
	BackupFailure = "failure"
	// BackupSkipped is a backup that did not start because another backup or
	// a restore held the tenant's lease.
	BackupSkipped = "skipped"
)

// BackupMetrics observes the backups this process runs.
type BackupMetrics interface {
	// BackupObserved records one backup's outcome (BackupSuccess,
	// BackupFailure or BackupSkipped). For a success, d is how long the
	// backup held the tenant's lease (the time its writes were paused),
	// bytes the size of the stored object, and nodes and rels the records it
	// holds; the other outcomes pass zeros.
	BackupObserved(outcome string, d time.Duration, bytes int64, nodes, rels int64)
}
