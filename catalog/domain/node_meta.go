package domain

// NodeMeta is per-node topology metadata for a single node of a tenant,
// addressed by its (service, schema, table) identity. TestCountKnown is false
// when the node was recorded before its test count was captured; callers must
// treat that as "unknown", never as zero. Inactive is true for a retired node:
// a later release dropped it from the current topology, and the graph keeps it
// with its history.
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
	// IncludeInactiveNodes also matches retired nodes, which a later release
	// dropped from the topology; when both a live and a retired node match, the
	// live one wins.
	IncludeInactiveNodes
)
