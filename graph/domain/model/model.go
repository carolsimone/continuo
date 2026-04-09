package model

import "time"

// Criticality represents the criticality level of a table node
type Criticality string

const (
	CriticalityUnspecified Criticality = "UNSPECIFIED"
	CriticalityRegulatory  Criticality = "REGULATORY"
	CriticalityCore        Criticality = "CORE"
	CriticalitySecondary   Criticality = "SECONDARY"
)

// String returns the string representation of Criticality
func (c Criticality) String() string {
	return string(c)
}

// TableNode represents a table in the dependency graph
type TableNode struct {
	TableName     string
	SchemaName    string
	ServiceName   string
	Owner         string
	ScheduleName  string
	Criticality   Criticality
	LastUpdatedAt time.Time
	CreatedAt     time.Time
	NodeType      string // wire value: dbt-model | dbt-seed | dbt-snapshot
	Status        string // runtime graph status: PENDING, RUNNING, SUCCEEDED, FAILED, or empty
}

// Dependency represents a table node with its depth in the dependency tree
type Dependency struct {
	Node  *TableNode
	Depth int
}

// StaleUpstream represents an upstream dependency that is stale
type StaleUpstream struct {
	Node               *TableNode
	MinutesSinceUpdate int
}

// UpstreamDependency represents an upstream dependency reference
type UpstreamDependency struct {
	TableName   string
	SchemaName  string
	ServiceName string
}
