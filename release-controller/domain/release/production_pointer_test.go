package release_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
)

func TestNewProductionPointer_AccessorsRoundTrip(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()
	pointer := release.NewProductionPointer("svc-a", "sha-abc", "s3://bucket/svc-a/manifest.json", "sha-abc:latest", release.ManifestKindDbt, now)

	assert.Equal(t, "svc-a", pointer.ProjectName())
	assert.Equal(t, "sha-abc", pointer.ReleaseID())
	assert.Equal(t, "s3://bucket/svc-a/manifest.json", pointer.ManifestS3Key())
	assert.Equal(t, "sha-abc:latest", pointer.ImageTag())
	assert.Equal(t, release.ManifestKindDbt, pointer.ManifestKind())
	assert.Equal(t, now, pointer.UpdatedAt())
}
