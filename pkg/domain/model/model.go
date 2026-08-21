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

// Annotation keys carrying the RAW (unsanitized) release/node identity on a
// validation Job. They are the cross-service contract between executor-controller
// (which stamps them) and k8s-controller (which reads them into
// validation.node.completed:v1). Labels are sanitized for K8s (charset + 63-char
// limit) and serve only routing/selection; these annotations preserve the exact
// values so the executor's outcome lookup matches the unmodified
// executor_deployments key.
const (
	AnnotationReleaseID = "continuo.dev/release-id"
	AnnotationNodeID    = "continuo.dev/node-id"
)

// NodeType represents the resource type for a graph node (dbt-model/seed/snapshot,
// or python-model).
type NodeType string

const (
	NodeTypeDbtModel    NodeType = "dbt-model"
	NodeTypeDbtSeed     NodeType = "dbt-seed"
	NodeTypeDbtSnapshot NodeType = "dbt-snapshot"
	// NodeTypePythonModel is a Continuo-native python node (contract.yaml +
	// user image). Validation routes it to build_from_columns; the executor
	// runs the node's own image (built FROM continuo-python-runtime-<engine>)
	// via buildPythonPodSpec.
	NodeTypePythonModel NodeType = "python-model"
	// NodeTypePythonCsv is a contract-only python node: no script — the
	// runtime's csv loader materializes the declared table from the csv uri
	// in the contract. Validation routes it to build_from_columns with a
	// header check; the executor runs it like python-model plus S3
	// credentials for the source fetch.
	NodeTypePythonCsv NodeType = "python-csv"
)

// AllNodeTypes enumerates every declared NodeType. The exhaustiveness guard
// test pins its length; family-branching call sites use IsPython, never a
// direct equality against one python kind.
var AllNodeTypes = []NodeType{
	NodeTypeDbtModel, NodeTypeDbtSeed, NodeTypeDbtSnapshot,
	NodeTypePythonModel, NodeTypePythonCsv,
}

// IsPython reports whether this node type runs on the python runtime image
// rather than the dbt toolchain.
func (t NodeType) IsPython() bool {
	return t == NodeTypePythonModel || t == NodeTypePythonCsv
}

// ParseNodeType converts a raw string to NodeType.
// Returns an error for empty or unrecognised values.
func ParseNodeType(s string) (NodeType, error) {
	switch NodeType(s) {
	case NodeTypeDbtModel, NodeTypeDbtSeed, NodeTypeDbtSnapshot, NodeTypePythonModel, NodeTypePythonCsv:
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

// Operation is the dbt verb a run applies to its nodes. It is orthogonal to
// NodeType: NodeType is what a node IS; Operation is which verb runs against it.
type Operation string

const (
	OperationRun   Operation = ""      // default: dbt run/seed/snapshot by NodeType
	OperationTest  Operation = "test"  // dbt test --select <node>
	OperationBuild Operation = "build" // dbt build --select <node>: materializes and tests the node in one invocation
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
	default:
		return "", fmt.Errorf("unknown operation %q", s)
	}
}
