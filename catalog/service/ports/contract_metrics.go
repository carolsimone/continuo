package ports

import "time"

// The writes of the live contracts folder, by what they write.
const (
	// LiveWritePut is the put of one node's published contract file.
	LiveWritePut = "put"
	// LiveWriteDelete is the delete of the file of a node no longer live.
	LiveWriteDelete = "delete"
	// LiveWriteIndex is the put of the folder's index: the interim index a
	// rewrite that puts files writes first, or the final one, written last.
	LiveWriteIndex = "index"
)

// The outcomes of one write of the live contracts folder.
const (
	// LiveWriteOK is a write object storage accepted.
	LiveWriteOK = "ok"
	// LiveWriteFailed is a write object storage refused or never answered.
	LiveWriteFailed = "failed"
)

// The timed steps of one promotion on the contracts group.
const (
	// ContractStepIngest reads the promotion's contract bundle and records its
	// contract versions in the graph.
	ContractStepIngest = "ingest"
	// ContractStepLiveRewrite rewrites the tenant's live contracts folder.
	ContractStepLiveRewrite = "live_rewrite"
)

// ContractMetrics observes the contracts group's work. A counter moves only
// once the transaction that did the counted work has committed.
type ContractMetrics interface {
	// VersionsWritten counts n stored contracts a promotion created.
	VersionsWritten(n int)
	// LiveWrite counts one write of the live contracts folder: op is
	// LiveWritePut, LiveWriteDelete or LiveWriteIndex, and outcome LiveWriteOK
	// or LiveWriteFailed.
	LiveWrite(op, outcome string)
	// ObserveStep records how long one step (ContractStepIngest or
	// ContractStepLiveRewrite) of a promotion took.
	ObserveStep(step string, d time.Duration)
}
