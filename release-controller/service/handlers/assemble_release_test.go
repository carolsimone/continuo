package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubServiceProdRepo is an in-test stub that returns a fixed list of ServiceProd records.
type stubServiceProdRepo struct {
	rows []*release.ServiceProd
}

func (s *stubServiceProdRepo) List(_ context.Context) ([]*release.ServiceProd, error) {
	return s.rows, nil
}

func (s *stubServiceProdRepo) Get(_ context.Context, serviceName string) (*release.ServiceProd, error) {
	for _, sp := range s.rows {
		if sp.ServiceName() == serviceName {
			return sp, nil
		}
	}
	return nil, nil
}

func (s *stubServiceProdRepo) Upsert(_ context.Context, sp *release.ServiceProd) error {
	s.rows = append(s.rows, sp)
	return nil
}

var _ repository.ServiceProdRepository = (*stubServiceProdRepo)(nil)

func TestAssembleManifestSet_ReplacesChangedServiceAndMergesOthers(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	repo := &stubServiceProdRepo{
		rows: []*release.ServiceProd{
			release.NewServiceProd("service-1", "rOLD1", "k1", "t1", now),
			release.NewServiceProd("service-2", "rOLD2", "kOLD", "tOLD", now),
		},
	}

	result, err := handlers.AssembleManifestSet(
		context.Background(), repo,
		"my-bucket", "service-2", "rNEW", "tNEW",
	)
	require.NoError(t, err)

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
	repo := &stubServiceProdRepo{rows: nil}

	result, err := handlers.AssembleManifestSet(
		context.Background(), repo,
		"bucket", "svc-a", "r1", "img-1",
	)
	require.NoError(t, err)
	assert.Equal(t, []release.ManifestKey{
		{Service: "svc-a", S3URI: "s3://bucket/svc-a/r1/manifest.json"},
	}, result.ManifestKeys)
	assert.Equal(t, map[string]string{"svc-a": "img-1"}, result.ImageTags)
}
