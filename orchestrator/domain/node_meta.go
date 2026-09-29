package domain

// NodeMeta is per-node topology metadata read for a single :Table addressed by
// its (service, schema, table) identity. TestCountKnown is false when the node
// predates test_count capture (the Neo4j property is unset); callers must treat
// that as "unknown", never as zero. Inactive is true when the :Table is no
// longer active in the topology; such a node is only returned by a lookup that
// asks for inactive nodes.
type NodeMeta struct {
	NodeType       string
	TestCount      int
	TestCountKnown bool
	Inactive       bool
}
