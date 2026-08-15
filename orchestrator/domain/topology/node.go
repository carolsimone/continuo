package topology

// ReleasePromotedTopologyNode is the domain representation of a node in a
// release.promoted:v1 payload. Used by ReleasePromotionRepository.
// Nodes are keyed by unique_id; upstream relationships are expressed as a
// list of unique_id strings rather than (schema_name, table_name) tuples.
//
// The wire event's changed flag is not carried here: ReleasePromotionRepository
// (the only consumer of this type) refreshes every node's properties on every
// promotion regardless of whether it changed. Callers that do need the flag
// (writeSeedsPending, planVersionWrite) read it directly off the wire
// []domainEvent.ReleasePromotedNode instead.
type ReleasePromotedTopologyNode struct {
	UniqueID    string
	SchemaName  string
	TableName   string
	ServiceName string
	NodeType    string
	// ContentHash is stored on :Table so a single query can detect a node whose
	// recorded code version no longer matches the code the topology says it runs.
	ContentHash       string
	TestCount         int
	ImageTag          string
	Schedule          string
	UpstreamUniqueIDs []string
	OriginalFilePath  string
}
