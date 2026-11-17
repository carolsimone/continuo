package handlers

import (
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// odcsBundleName is the object name of a python-odcs service's artifact: the
// bundle of its ODCS contracts, one per node.
const odcsBundleName = "odcs-bundle.json"

// CanonicalManifestKey is the single source of the per-release artifact layout
// convention: s3://<bucket>/<service>/<release_id>/manifest.json for a dbt
// service (uploaded by the compile leg) and .../odcs-bundle.json for a
// python-odcs service (uploaded by the domain repo's CI before POST /releases).
func CanonicalManifestKey(bucket, service, releaseID string, kind release.ManifestKind) string {
	name := "manifest.json"
	if kind == release.ManifestKindPythonODCS {
		name = odcsBundleName
	}
	return fmt.Sprintf("s3://%s/%s/%s/%s", bucket, service, releaseID, name)
}
