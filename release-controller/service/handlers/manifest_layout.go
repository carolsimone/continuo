package handlers

import "fmt"

// CanonicalManifestKey is the single source of the manifest layout convention,
// shared by contract with dbt_upload's writer:
// s3://<bucket>/<service>/<release_id>/manifest.json
func CanonicalManifestKey(bucket, service, releaseID string) string {
	return fmt.Sprintf("s3://%s/%s/%s/manifest.json", bucket, service, releaseID)
}
