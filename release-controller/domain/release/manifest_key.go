package release

// ManifestKey identifies one service's release artifact. A slice of ManifestKey
// is assembled in memory for a release; a boundary DTO maps it onto the
// release.requested:v1 payload, so this domain type carries no serialization
// tags. Kind says how the artifact at S3URI is parsed (dbt manifest.json,
// python-odcs odcs-bundle.json or the ontology service's ontology.yaml).
type ManifestKey struct {
	Service string
	S3URI   string
	Kind    ManifestKind
}
