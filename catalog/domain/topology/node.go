package topology

// ReleasePromotedTopologyNode is one node of a promoted release's topology, as
// ReleasePromotionRepository writes it into the live graph. Nodes are keyed by
// unique_id; upstream relationships are a list of unique_id strings rather
// than (schema_name, table_name) tuples. Whether a node changed is not carried
// here: the swap refreshes every node's properties on every promotion, and the
// callers that need it read the promotion's changed_node_ids.
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
	SecretRef         string
	Schedule          string
	UpstreamUniqueIDs []string
	OriginalFilePath  string
}
