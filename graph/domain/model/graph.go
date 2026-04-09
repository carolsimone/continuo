package model

import "time"

// GraphEdge represents a directed dependency edge between two nodes.
// Node IDs use the format "{service_name}.{schema_name}.{table_name}".
type GraphEdge struct {
	FromNodeID string
	ToNodeID   string
}

// ScheduleGraph holds all nodes and edges for a schedule's DAG,
// including cross-boundary upstream dependencies (e.g. seed nodes).
type ScheduleGraph struct {
	Nodes []*TableNode
	Edges []*GraphEdge
}

// ScheduleInitNodes holds the three node sets returned by GetScheduleInitNodes.
type ScheduleInitNodes struct {
	AllNodes  []*TableNode
	RootNodes []*TableNode
	SeedNodes []*TableNode
}

// DownstreamNode represents a downstream table node that is ready for execution.
// Used by GetReadyDownstream to return candidates to dependency-controller.
type DownstreamNode struct {
	ServiceName string
	SchemaName  string
	TableName   string
	NodeType    string
}

// RunSummary is returned by ListRuns for the UI run history timeline.
type RunSummary struct {
	RunID          string
	ScheduleName   string
	TerminalStatus string    // "SUCCEEDED" | "FAILED"
	CreatedAt      time.Time
	CompletedAt    time.Time
}
