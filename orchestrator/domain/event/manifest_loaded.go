package event

// ManifestLoadedNode is the wire-format representation of a single topology
// node carried in the manifest.loaded:v1 stream payload. The orchestrator's
// consumer json.Unmarshals the raw stream bytes directly into a
// []ManifestLoadedNode slice.
type ManifestLoadedNode struct {
	ServiceName     string                     `json:"service_name"`
	SchemaName      string                     `json:"schema_name"`
	TableName       string                     `json:"table_name"`
	Owner           string                     `json:"owner"`
	ScheduleName    string                     `json:"schedule_name"`
	Criticality     string                     `json:"criticality"`
	NodeType        string                     `json:"node_type"`
	ManifestVersion string                     `json:"manifest_version"`
	ImageTag        string                     `json:"image_tag"`
	Dependencies    []ManifestLoadedDependency `json:"dependencies"`
}

// ManifestLoadedDependency is the wire-format representation of an upstream
// dependency edge carried as a sub-payload inside ManifestLoadedNode.
type ManifestLoadedDependency struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
}
