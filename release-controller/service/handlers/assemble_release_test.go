package handlers_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssembleManifestSet_ReplacesChangedProjectAndMergesOthers(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	existing := []*release.ProductionPointer{
		release.NewProductionPointer("service-1", "rOLD1", "k1", "t1", release.ManifestKindDbt, now),
		release.NewProductionPointer("service-2", "rOLD2", "kOLD", "tOLD", release.ManifestKindDbt, now),
	}

	result := handlers.AssembleManifestSet(
		existing,
		"my-bucket", "service-2", "rNEW", "tNEW", release.ManifestKindDbt,
	)

	// Exactly two entries: one per project, no duplicates.
	assert.Len(t, result.ManifestKeys, 2, "changed project's OLD pointer must not be duplicated")

	// The changed project gets the fresh canonical key; the other keeps its stored key.
	require.ElementsMatch(t, []release.ManifestKey{
		{Project: "service-1", S3URI: "k1", Kind: release.ManifestKindDbt},
		{Project: "service-2", S3URI: "s3://my-bucket/service-2/rNEW/manifest.json", Kind: release.ManifestKindDbt},
	}, result.ManifestKeys)

	// Image tags are updated correctly.
	assert.Equal(t, map[string]string{
		"service-1": "t1",
		"service-2": "tNEW",
	}, result.ImageTags)
}

func TestAssembleManifestSet_NoExistingProjects(t *testing.T) {
	result := handlers.AssembleManifestSet(
		nil,
		"bucket", "svc-a", "r1", "img-1", release.ManifestKindDbt,
	)
	assert.Equal(t, []release.ManifestKey{
		{Project: "svc-a", S3URI: "s3://bucket/svc-a/r1/manifest.json", Kind: release.ManifestKindDbt},
	}, result.ManifestKeys)
	assert.Equal(t, map[string]string{"svc-a": "img-1"}, result.ImageTags)
}

func TestAssembleManifestSet_ThreadsKinds(t *testing.T) {
	existing := []*release.ProductionPointer{
		release.NewProductionPointer("svc-dbt", "rOld", "s3://b/svc-dbt/rOld/manifest.json", "t1", release.ManifestKindDbt, time.Unix(0, 0)),
		release.NewProductionPointer("svc-py-old", "rOld2", "s3://b/svc-py-old/rOld2/odcs-bundle.json", "t2", release.ManifestKindPython, time.Unix(0, 0)),
	}
	set := handlers.AssembleManifestSet(existing, "b", "svc-py", "rNew", "t3", release.ManifestKindPython)

	require.Len(t, set.ManifestKeys, 3)
	assert.Equal(t, release.ManifestKey{Project: "svc-py", S3URI: "s3://b/svc-py/rNew/odcs-bundle.json", Kind: release.ManifestKindPython}, set.ManifestKeys[0])
	assert.Contains(t, set.ManifestKeys, release.ManifestKey{Project: "svc-dbt", S3URI: "s3://b/svc-dbt/rOld/manifest.json", Kind: release.ManifestKindDbt})
	assert.Contains(t, set.ManifestKeys, release.ManifestKey{Project: "svc-py-old", S3URI: "s3://b/svc-py-old/rOld2/odcs-bundle.json", Kind: release.ManifestKindPython})
}

// The ontology project's pointer rides on every release like any other
// project's, with its kind, so topology-controller reads the live ontology
// beside the changed project's artifact. The ontology project's own release
// replaces it.
func TestAssembleManifestSet_CarriesTheOntologyPointerOnEveryRelease(t *testing.T) {
	existing := []*release.ProductionPointer{
		release.NewProductionPointer("company-ontology", "rOnt", "s3://b/company-ontology/rOnt/ontology.yaml", "t1", release.ManifestKindOntology, time.Unix(0, 0)),
	}

	set := handlers.AssembleManifestSet(existing, "b", "svc-dbt", "rNew", "t2", release.ManifestKindDbt)
	assert.Equal(t, []release.ManifestKey{
		{Project: "svc-dbt", S3URI: "s3://b/svc-dbt/rNew/manifest.json", Kind: release.ManifestKindDbt},
		{Project: "company-ontology", S3URI: "s3://b/company-ontology/rOnt/ontology.yaml", Kind: release.ManifestKindOntology},
	}, set.ManifestKeys)

	set = handlers.AssembleManifestSet(existing, "b", "company-ontology", "rOnt2", "t3", release.ManifestKindOntology)
	assert.Equal(t, []release.ManifestKey{
		{Project: "company-ontology", S3URI: "s3://b/company-ontology/rOnt2/ontology.yaml", Kind: release.ManifestKindOntology},
	}, set.ManifestKeys)
}
