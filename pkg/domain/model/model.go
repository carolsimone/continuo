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
	ServiceName string
	SchemaName  string
	TableName   string
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

// Annotation keys carrying the RAW (unsanitized) release/node identity on a
// validation Job. The dispatcher stamps them, and the job-status handler reads
// them into the outcomes.NodeOutcome it records. Labels are sanitized for K8s
// (charset + 63-char limit) and serve only routing/selection; these
// annotations preserve the exact values so the outcome lookup matches the
// unmodified deployments key.
const (
	AnnotationReleaseID = "continuo.dev/release-id"
	AnnotationNodeID    = "continuo.dev/node-id"
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

// Operation is the dbt verb a run applies to its nodes. It is orthogonal to
// NodeType: NodeType is what a node IS; Operation is which verb runs against it.
type Operation string

const (
	OperationRun         Operation = ""             // default: dbt run/seed/snapshot by NodeType
	OperationTest        Operation = "test"         // dbt test --select <node>
	OperationBuild       Operation = "build"        // dbt build --select <node>: materializes and tests the node in one invocation
	OperationFullRefresh Operation = "full_refresh" // rebuilds one model or seed from scratch (dbt --full-refresh)
)

// ParseOperation normalizes a raw operation string. Empty ⇒ run.
func ParseOperation(s string) (Operation, error) {
	switch Operation(s) {
	case OperationRun, "run":
		return OperationRun, nil
	case OperationTest:
		return OperationTest, nil
	case OperationBuild:
		return OperationBuild, nil
	case OperationFullRefresh:
		return OperationFullRefresh, nil
	default:
		return "", fmt.Errorf("unknown operation %q", s)
	}
}

// IsSingleNodeOnly reports whether o may only target one node. A full refresh
// drops and rebuilds a table, so it is never fanned out across a schedule.
func (o Operation) IsSingleNodeOnly() bool {
	return o == OperationFullRefresh
}
