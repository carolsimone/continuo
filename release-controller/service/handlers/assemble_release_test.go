package handlers_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssembleManifestSet_ReplacesChangedServiceAndMergesOthers(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	existing := []*release.ServiceProd{
		release.NewServiceProd("service-1", "rOLD1", "k1", "t1", release.ManifestKindDbt, now),
		release.NewServiceProd("service-2", "rOLD2", "kOLD", "tOLD", release.ManifestKindDbt, now),
	}

	result := handlers.AssembleManifestSet(
		existing,
		"my-bucket", "service-2", "rNEW", "tNEW", release.ManifestKindDbt,
	)

	// Exactly two entries: one per service, no duplicates.
	assert.Len(t, result.ManifestKeys, 2, "changed service's OLD pointer must not be duplicated")

	// The changed service gets the fresh canonical key; the other keeps its stored key.
	require.ElementsMatch(t, []release.ManifestKey{
		{Service: "service-1", S3URI: "k1", Kind: release.ManifestKindDbt},
		{Service: "service-2", S3URI: "s3://my-bucket/service-2/rNEW/manifest.json", Kind: release.ManifestKindDbt},
	}, result.ManifestKeys)

	// Image tags are updated correctly.
	assert.Equal(t, map[string]string{
		"service-1": "t1",
		"service-2": "tNEW",
	}, result.ImageTags)
}

func TestAssembleManifestSet_NoExistingServices(t *testing.T) {
	result := handlers.AssembleManifestSet(
		nil,
		"bucket", "svc-a", "r1", "img-1", release.ManifestKindDbt,
	)
	assert.Equal(t, []release.ManifestKey{
		{Service: "svc-a", S3URI: "s3://bucket/svc-a/r1/manifest.json", Kind: release.ManifestKindDbt},
	}, result.ManifestKeys)
	assert.Equal(t, map[string]string{"svc-a": "img-1"}, result.ImageTags)
}

func TestAssembleManifestSet_ThreadsKinds(t *testing.T) {
	existing := []*release.ServiceProd{
		release.NewServiceProd("svc-dbt", "rOld", "s3://b/svc-dbt/rOld/manifest.json", "t1", release.ManifestKindDbt, time.Unix(0, 0)),
		release.NewServiceProd("svc-py-old", "rOld2", "s3://b/svc-py-old/rOld2/odcs-bundle.json", "t2", release.ManifestKindPythonODCS, time.Unix(0, 0)),
	}
	set := handlers.AssembleManifestSet(existing, "b", "svc-py", "rNew", "t3", release.ManifestKindPythonODCS)

	require.Len(t, set.ManifestKeys, 3)
	assert.Equal(t, release.ManifestKey{Service: "svc-py", S3URI: "s3://b/svc-py/rNew/odcs-bundle.json", Kind: release.ManifestKindPythonODCS}, set.ManifestKeys[0])
	assert.Contains(t, set.ManifestKeys, release.ManifestKey{Service: "svc-dbt", S3URI: "s3://b/svc-dbt/rOld/manifest.json", Kind: release.ManifestKindDbt})
	assert.Contains(t, set.ManifestKeys, release.ManifestKey{Service: "svc-py-old", S3URI: "s3://b/svc-py-old/rOld2/odcs-bundle.json", Kind: release.ManifestKindPythonODCS})
}

// The ontology service's pointer rides on every release like any other
// service's, with its kind, so topology-controller reads the live ontology
// beside the changed service's artifact. The ontology service's own release
// replaces it.
func TestAssembleManifestSet_CarriesTheOntologyPointerOnEveryRelease(t *testing.T) {
	existing := []*release.ServiceProd{
		release.NewServiceProd("company-ontology", "rOnt", "s3://b/company-ontology/rOnt/ontology.yaml", "t1", release.ManifestKindOntology, time.Unix(0, 0)),
	}

	set := handlers.AssembleManifestSet(existing, "b", "svc-dbt", "rNew", "t2", release.ManifestKindDbt)
	assert.Equal(t, []release.ManifestKey{
		{Service: "svc-dbt", S3URI: "s3://b/svc-dbt/rNew/manifest.json", Kind: release.ManifestKindDbt},
		{Service: "company-ontology", S3URI: "s3://b/company-ontology/rOnt/ontology.yaml", Kind: release.ManifestKindOntology},
	}, set.ManifestKeys)

	set = handlers.AssembleManifestSet(existing, "b", "company-ontology", "rOnt2", "t3", release.ManifestKindOntology)
	assert.Equal(t, []release.ManifestKey{
		{Service: "company-ontology", S3URI: "s3://b/company-ontology/rOnt2/ontology.yaml", Kind: release.ManifestKindOntology},
	}, set.ManifestKeys)
}
