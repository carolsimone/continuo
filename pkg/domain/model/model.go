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

// IsExternal reports whether this node type only marks a table another system
// loads: no continuo toolchain builds it, so it is never run, built, validated
// or fixed.
func (t NodeType) IsExternal() bool {
	return t.Runtime() == NodeRuntimeExternal
}

// IsBuilt reports whether a release builds a node of this type: it runs on the
// dbt or the python runtime and is not a dbt test, which only validates.
func (t NodeType) IsBuilt() bool {
	r := t.Runtime()
	return (r == NodeRuntimeDbt || r == NodeRuntimePython) && t != NodeTypeDbtTest
}

// BuiltNodeTypes returns every node type for which IsBuilt holds, in
// declaration order. A query that must leave out a table no node builds lists
// these canonical names rather than excluding one it knows.
func BuiltNodeTypes() []NodeType {
	var built []NodeType
	for _, t := range NodeTypes() {
		if t.IsBuilt() {
			built = append(built, t)
		}
	}
	return built
}

// IsDormant reports whether a node of type t with this schedule is dormant: a
// release builds it and no schedule tag puts it on a cron. A run started by
// hand executes it, and a schedule's run includes it only to rebuild it as a
// view or materialized view below a seed whose load drops the seed's table. A
// dbt test, a table no node builds and an undeclared type are never dormant.
// Go services ask this function rather than restate the rule;
// topology-controller's is_dormant is its Python twin, and
// testdata/dormant_cases.json pins the two to each other.
func IsDormant(t NodeType, schedule string) bool {
	return schedule == "" && t.IsBuilt()
}

// legacyIngested is the name under which stored rows and artifacts written
// earlier still record NodeTypeUnknownExternal.
const legacyIngested = "ingested"

// CanonicalNodeType converts a node type read from storage or from an artifact
// into the current NodeType: the legacy name of unknown-external becomes
// NodeTypeUnknownExternal, and every other string is returned as it is, valid
// or not, for ParseNodeType or IsValid to judge.
func CanonicalNodeType(s string) NodeType {
	if s == legacyIngested {
		return NodeTypeUnknownExternal
	}
	return NodeType(s)
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
