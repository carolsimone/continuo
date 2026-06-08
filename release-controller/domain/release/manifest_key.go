package release

// ManifestKey identifies one service's manifest file. A slice of ManifestKey
// is carried by release.requested:v1 so manifest-controller knows exactly which
// S3 objects to fetch when parsing the assembled topology for a release.
type ManifestKey struct {
	Service string `json:"service"`
	S3URI   string `json:"s3_uri"`
}
