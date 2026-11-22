package handlers

import (
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// odcsBundleName is the object name of a python-odcs service's artifact: the
// bundle of its ODCS contracts, one per node.
const odcsBundleName = "odcs-bundle.json"

// ontologyFileName is the object name of the ontology service's artifact: the
// install's company ontology file.
const ontologyFileName = "ontology.yaml"

// CanonicalManifestKey is the single source of the per-release artifact layout
// convention: s3://<bucket>/<service>/<release_id>/manifest.json for a dbt
// service (uploaded by the compile leg), .../odcs-bundle.json for a
// python-odcs service and .../ontology.yaml for the ontology service (both
// uploaded by the owning repo's CI before POST /releases).
func CanonicalManifestKey(bucket, service, releaseID string, kind release.ManifestKind) string {
	name := "manifest.json"
	switch kind {
	case release.ManifestKindPythonODCS:
		name = odcsBundleName
	case release.ManifestKindOntology:
		name = ontologyFileName
	}
	return fmt.Sprintf("s3://%s/%s/%s/%s", bucket, service, releaseID, name)
}
