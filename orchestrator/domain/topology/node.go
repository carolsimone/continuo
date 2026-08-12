package topology

import "time"

// ReleasePromotedTopologyNode is the domain representation of a node in a
// release.promoted:v1 payload. Used by ReleasePromotionRepository.
// Nodes are keyed by unique_id; upstream relationships are expressed as a
// list of unique_id strings rather than (schema_name, table_name) tuples.
//
// Changed marks a node whose dbt content_hash differs from the prior prod.
// LastCommitSHA/LastRepo/LastChangedAt carry the release's provenance,
// denormalized onto each node so the repository stamps only changed nodes
// without a separate scalar parameter.
type ReleasePromotedTopologyNode struct {
	UniqueID          string
	SchemaName        string
	TableName         string
	ServiceName       string
	NodeType          string
	// ContentHash is stored on :Table so a single query can detect a node whose
	// recorded code version no longer matches the code the topology says it runs.
	ContentHash       string
	TestCount         int
	ImageTag          string
	Schedule          string
	UpstreamUniqueIDs []string
	Changed           bool
	LastCommitSHA     string
	LastRepo          string
	LastChangedAt     time.Time
	OriginalFilePath  string
}
