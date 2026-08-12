package events

// SeedNode is one seed a promoted release changed, carrying the metadata that
// release pinned to it.
//
// NodeType and ImageTag travel on the event rather than being re-read from the
// topology later: a promotion can be overtaken by a newer one, and re-reading
// would then build this release's seeds with a different release's image.
type SeedNode struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	NodeType    string `json:"node_type"`
	ImageTag    string `json:"image_tag"`
}

// ReleaseSeedsPending is the release.seeds.pending:v1 payload: the seeds a
// promoted release changed, published by orchestrator in the same transaction
// that swaps the topology.
//
// Because it is written inside that transaction, every node named here is
// already present in the topology by the time this event is visible.
type ReleaseSeedsPending struct {
	ReleaseID string     `json:"release_id"`
	Nodes     []SeedNode `json:"nodes"`
}
