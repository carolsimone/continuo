package event

// ReleasePromotedNode is the wire-format representation of a single node in a
// release.promoted:v1 payload's topology array. Distinct from
// ManifestLoadedNode because the release pipeline keys nodes by unique_id and
// carries upstream relationships as a string-id list (vs. the manifest path's
// service/schema/table tuple).
type ReleasePromotedNode struct {
	UniqueID          string   `json:"unique_id"`
	SchemaName        string   `json:"schema_name"`
	TableName         string   `json:"table_name"`
	ServiceName       string   `json:"service_name"`
	NodeType          string   `json:"node_type"`
	ImageTag          string   `json:"image_tag"`
	Schedule          string   `json:"schedule"`
	UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
}

// ReleasePromoted is the full release.promoted:v1 payload as published by
// release-controller's outbox processor.
type ReleasePromoted struct {
	ReleaseID string                `json:"release_id"`
	Topology  []ReleasePromotedNode `json:"topology"`
	ImageTags map[string]string     `json:"image_tags"`
}
