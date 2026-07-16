package snapshot

import (
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// SourceTaskRow captures the per-task data the selectors need from a source
// :Run's :EXECUTES set. Loaded by TopologyReader.LoadSourceTasks.
type SourceTaskRow struct {
	TaskID          uuid.UUID
	ScheduleName    string
	NodeType        string
	Status          string // uppercase: "PENDING" | "SUCCEEDED" | "FAILED" | "SKIPPED"
	ImageTag        string
	ManifestVersion string
	// DBTUniqueID is the node's dbt identity ("model.finance.orders"), used to
	// select the model inside a hydrated manifest. Distinct from the graph's
	// unique_id, which is "schema.table".
	DBTUniqueID string
	// RuntimeManifestRef is the artifact the source task ran against, read from
	// its :EXECUTES edge. Empty for a run snapshotted before the source's
	// release pinned one.
	pkgModel.RuntimeManifestRef
	InheritedFromRoot *uuid.UUID // non-nil iff source row was itself inherited
}

// LatestTableRow captures the per-table metadata pinned at snapshot time from
// the latest :Table topology. Loaded by TopologyReader.LoadLatestSourceDAG and
// TopologyReader.LoadSingleLatestTable.
type LatestTableRow struct {
	ScheduleName    string
	NodeType        string
	TestCount       int
	TestCountKnown  bool // true iff the :Table had a test_count property (false for pre-capture topology)
	ImageTag        string
	ManifestVersion string
	// DBTUniqueID is the node's dbt identity ("model.finance.orders"), used to
	// select the model inside a hydrated manifest. Distinct from the graph's
	// unique_id, which is "schema.table".
	DBTUniqueID string
	// RuntimeManifestRef is the artifact the node's release pinned. Empty for a
	// topology promoted before runtime manifests existed.
	pkgModel.RuntimeManifestRef
}
