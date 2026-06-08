package release

// ManifestKey identifies one service's manifest file. A slice of ManifestKey is
// assembled in memory for a release; the AdvanceQueue handler maps it onto a
// boundary DTO to serialize the release.requested:v1 payload, so this domain
// type carries no serialization tags.
type ManifestKey struct {
	Service string
	S3URI   string
}
