package release

// ManifestKey identifies one project's release artifact. A slice of ManifestKey
// is assembled in memory for a release; a boundary DTO maps it onto the
// release.requested:v1 payload, so this domain type carries no serialization
// tags. Kind says how the artifact at S3URI is parsed (dbt manifest.json,
// Python project's odcs-bundle.json or the ontology project's ontology.yaml).
type ManifestKey struct {
	Project string
	S3URI   string
	Kind    ManifestKind
}
