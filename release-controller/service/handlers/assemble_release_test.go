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
		"my-bucket", "service-2", "rNEW", "tNEW",
	)

	// Exactly two entries: one per service, no duplicates.
	assert.Len(t, result.ManifestKeys, 2, "changed service's OLD pointer must not be duplicated")

	// The changed service gets the fresh canonical key; the other keeps its stored key.
	require.ElementsMatch(t, []release.ManifestKey{
		{Service: "service-1", S3URI: "k1"},
		{Service: "service-2", S3URI: "s3://my-bucket/service-2/rNEW/manifest.json"},
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
		"bucket", "svc-a", "r1", "img-1",
	)
	assert.Equal(t, []release.ManifestKey{
		{Service: "svc-a", S3URI: "s3://bucket/svc-a/r1/manifest.json"},
	}, result.ManifestKeys)
	assert.Equal(t, map[string]string{"svc-a": "img-1"}, result.ImageTags)
}
