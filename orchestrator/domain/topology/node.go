package topology

// ReleasePromotedTopologyNode is the domain representation of a node in a
// release.promoted:v1 payload. Used by ReleasePromotionRepository.
// Nodes are keyed by unique_id; upstream relationships are expressed as a
// list of unique_id strings rather than (schema_name, table_name) tuples.
type ReleasePromotedTopologyNode struct {
	UniqueID          string
	SchemaName        string
	TableName         string
	ServiceName       string
	NodeType          string
	ImageTag          string
	Schedule          string
	UpstreamUniqueIDs []string
}
