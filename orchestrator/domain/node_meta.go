package domain

// NodeMeta is per-node topology metadata for a single node addressed by its
// (service, schema, table) identity. TestCountKnown is false when the node was
// recorded before its test count was captured; callers must treat that as
// "unknown", never as zero. Inactive is true when a later release removed the
// node from the current topology.
type NodeMeta struct {
	NodeType       string
	TestCount      int
	TestCountKnown bool
	Inactive       bool
}

// NodeScope selects which nodes a node read may match.
type NodeScope int

const (
	// ActiveNodesOnly matches only nodes in the current topology.
	ActiveNodesOnly NodeScope = iota
	// IncludeInactiveNodes also matches nodes a later release removed from the
	// topology; when both an active and an inactive node match, the active one
	// wins.
	IncludeInactiveNodes
)
