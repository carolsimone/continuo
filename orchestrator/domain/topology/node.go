package topology

import (
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
)

type Node struct {
	TableName       string
	SchemaName      string
	ServiceName     string
	Owner           string
	ScheduleName    string
	Criticality     domain.Criticality
	NodeType        string // dbt-model | dbt-seed | dbt-snapshot
	ManifestVersion string
	LastUpdatedAt   time.Time
	CreatedAt       time.Time
}

type UpstreamDependency struct {
	TableName   string
	SchemaName  string
	ServiceName string
}

type TopologyPayload struct {
	Nodes []TopologyNode
}

type TopologyNode struct {
	ServiceName     string
	SchemaName      string
	TableName       string
	Owner           string
	ScheduleName    string
	Criticality     string
	NodeType        string
	ManifestVersion string
	Dependencies    []UpstreamDependency
}
