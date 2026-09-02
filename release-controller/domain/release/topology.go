package release

type Topology []Node

type Node struct {
	UniqueID   string `json:"unique_id"`
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	// ResolvedRelationID is "<schema>.<resolved name>", lowercased: the
	// physical relation this node's build actually writes. UniqueID is keyed
	// on the DECLARED name; this is keyed on the RESOLVED one — a dbt node's
	// alias when it overrides one, else the same declared name. Two nodes
	// with different declared names but the same alias write the same
	// warehouse table, a collision UniqueID alone cannot see. Empty on a
	// payload from before this field existed; DuplicateClaims falls back to
	// UniqueID in that case.
	ResolvedRelationID string   `json:"resolved_relation_id"`
	ServiceName        string   `json:"service_name"`
	NodeType           string   `json:"node_type"`
	ContentHash        string   `json:"content_hash"`
	TestCount          int      `json:"test_count"`
	ImageTag           string   `json:"image_tag"`
	UpstreamUniqueIDs  []string `json:"upstream_unique_ids"`
	Schedule           string   `json:"schedule"`
	OriginalFilePath   string   `json:"original_file_path"`
	// CandidateArtifactURI is an S3 URI pointing to the object the node's
	// validation Job must fetch to build the node as an empty table in the
	// candidate schema: for a dbt node the compiled SQL with schema-qualified
	// references rewritten to the candidate schema, for a python node the
	// validation spec (declared reads plus output columns). Which shape it is
	// follows from NodeType. This is transient validation data and must not be
	// persisted to current_prod or published in the promoted topology.
	CandidateArtifactURI string `json:"candidate_artifact_uri,omitempty"`
}

// WithoutCandidateArtifactURI returns a copy of the topology with per-node
// CandidateArtifactURI cleared. The URI is release-specific transient
// validation data — it must not be persisted to current_prod or published in
// the promoted topology.
func (t Topology) WithoutCandidateArtifactURI() Topology {
	out := make(Topology, len(t))
	for i, n := range t {
		n.CandidateArtifactURI = ""
		out[i] = n
	}
	return out
}
