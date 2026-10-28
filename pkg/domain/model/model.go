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

// NodeType, its values, NodeTypes(), IsValid() and Runtime() are generated from
// the node_type vocabulary in pkg/streams/contract.yaml (vocabulary.gen.go).

// IsPython reports whether this node type runs on the python runtime image
// rather than the dbt toolchain. Family-branching call sites use it, never a
// direct equality against one python kind.
func (t NodeType) IsPython() bool {
	return t.Runtime() == NodeRuntimePython
}

// ParseNodeType converts a raw string to NodeType. It returns an error naming
// the value and every valid node type for an empty or undeclared value.
func ParseNodeType(s string) (NodeType, error) {
	t := NodeType(s)
	if !t.IsValid() {
		return "", fmt.Errorf("unknown node_type %q (valid: %v)", s, NodeTypes())
	}
	return t, nil
}

// Command returns the container command slice for this NodeType.
// This is the single source of truth for the dbt CLI mapping. A production
// seed load is a normal, non-destructive `dbt seed`: it inserts new/changed
// rows without dropping the table. It is NOT a full refresh — rebuilding a
// seed from scratch (`dbt seed --full-refresh`) is a separate, explicit
// operation, never issued by an automatic or scheduled load.
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
