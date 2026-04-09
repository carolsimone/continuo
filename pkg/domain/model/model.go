package model

import (
	"fmt"
)

type ExecutionStatus string

const (
	Queue      ExecutionStatus = "queue"
	Running    ExecutionStatus = "running"
	Successful ExecutionStatus = "successful"
	Failed     ExecutionStatus = "failed"
)

// FQN represents a Fully Qualified Name (service.schema.table)
type FQN struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
}

// ToString converts FQN to string format: service.schema.table
func (f FQN) ToString() string {
	return fmt.Sprintf("%s.%s.%s", f.ServiceName, f.SchemaName, f.TableName)
}

// DbtFQN represents a dbt-specific Fully Qualified Name
type DbtFQN struct {
	FQN
}

// SourceFQN represents a source-specific Fully Qualified Name
type SourceFQN struct {
	FQN
}

// NodeType represents the dbt resource type for a graph node.
type NodeType string

const (
	NodeTypeDbtModel    NodeType = "dbt-model"
	NodeTypeDbtSeed     NodeType = "dbt-seed"
	NodeTypeDbtSnapshot NodeType = "dbt-snapshot"
)

// ParseNodeType converts a raw string to NodeType.
// Returns an error for empty or unrecognised values.
func ParseNodeType(s string) (NodeType, error) {
	switch NodeType(s) {
	case NodeTypeDbtModel, NodeTypeDbtSeed, NodeTypeDbtSnapshot:
		return NodeType(s), nil
	default:
		return "", fmt.Errorf("unknown node_type %q", s)
	}
}

// Command returns the container command slice for this NodeType.
// This is the single source of truth for the dbt CLI mapping.
func (t NodeType) Command(tableName string) []string {
	switch t {
	case NodeTypeDbtSeed:
		return []string{"dbt", "seed", "--select", tableName}
	case NodeTypeDbtSnapshot:
		return []string{"dbt", "snapshot", "--select", tableName}
	default: // NodeTypeDbtModel
		return []string{"dbt", "run", "--select", tableName}
	}
}
