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
	// ContentHash is the code fingerprint the SOURCE run actually executed, read
	// off its :EXECUTES edge. A derived run reuses it alongside ImageTag so the
	// new run records the code it repeats, not whatever the topology holds now.
	ContentHash       string
	InheritedFromRoot *uuid.UUID // non-nil iff source row was itself inherited
}

// LatestTableRow captures the per-table metadata pinned at snapshot time from
// the latest :Table topology. Loaded by TopologyReader.LoadLatestSourceDAG and
// TopologyReader.LoadSingleLatestTable.
//
// LoadSingleTableFromSourceRun reuses this shape for a snapshot_of_run task, and
// fills ImageTag/ManifestVersion/ContentHash from the source run's :EXECUTES
// edge rather than from the :Table, so a stale-mode run repeats exactly what the
// source executed.
type LatestTableRow struct {
	ScheduleName    string
	NodeType        string
	TestCount       int
	TestCountKnown  bool // true iff the :Table had a test_count property (false for pre-capture topology)
	ImageTag        string
	ManifestVersion string
	ContentHash     string
}
