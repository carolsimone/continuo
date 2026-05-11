package snapshot

import "github.com/google/uuid"

// SourceTaskRow captures the per-task data the selectors need from a source
// :Run's :EXECUTES set. Loaded by TopologyReader.LoadSourceTasks.
type SourceTaskRow struct {
	TaskID            uuid.UUID
	ScheduleName      string
	NodeType          string
	Status            string // uppercase: "PENDING" | "SUCCEEDED" | "FAILED" | "SKIPPED"
	ImageTag          string
	ManifestVersion   string
	InheritedFromRoot *uuid.UUID // non-nil iff source row was itself inherited
}

// LatestTableRow captures the per-table metadata pinned at snapshot time from
// the latest :Table topology. Loaded by TopologyReader.LoadLatestSourceDAG and
// TopologyReader.LoadSingleLatestTable.
type LatestTableRow struct {
	ScheduleName    string
	NodeType        string
	ImageTag        string
	ManifestVersion string
}
