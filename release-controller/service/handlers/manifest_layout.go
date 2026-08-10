package handlers

import (
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// CanonicalManifestKey is the single source of the per-release artifact layout
// convention: s3://<bucket>/<service>/<release_id>/manifest.json for a dbt
// service (uploaded by the compile leg) and .../contract.yaml for a python
// service (uploaded by the domain repo's CI before POST /releases).
func CanonicalManifestKey(bucket, service, releaseID string, kind release.ManifestKind) string {
	name := "manifest.json"
	if kind == release.ManifestKindPython {
		name = "contract.yaml"
	}
	return fmt.Sprintf("s3://%s/%s/%s/%s", bucket, service, releaseID, name)
}
