package handlers

import (
	"fmt"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// odcsBundleName is the object name of a python-odcs project's artifact: the
// bundle of its ODCS contracts, one per node.
const odcsBundleName = "odcs-bundle.json"

// ontologyFileName is the object name of the ontology project's artifact: the
// install's company ontology file.
const ontologyFileName = "ontology.yaml"

// CanonicalManifestKey is the single source of the per-release artifact layout
// convention: s3://<bucket>/<project>/<release_id>/manifest.json for a dbt
// project (uploaded by the compile leg), .../odcs-bundle.json for a
// python-odcs project and .../ontology.yaml for the ontology project (both
// uploaded by the owning repo's CI before POST /releases).
func CanonicalManifestKey(bucket, project, releaseID string, kind release.ManifestKind) string {
	name := "manifest.json"
	switch kind {
	case release.ManifestKindPythonODCS:
		name = odcsBundleName
	case release.ManifestKindOntology:
		name = ontologyFileName
	}
	return fmt.Sprintf("s3://%s/%s/%s/%s", bucket, project, releaseID, name)
}
