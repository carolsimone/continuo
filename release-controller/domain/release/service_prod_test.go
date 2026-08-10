package release_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
)

func TestNewServiceProd_AccessorsRoundTrip(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()
	sp := release.NewServiceProd("svc-a", "sha-abc", "s3://bucket/svc-a/manifest.json", "sha-abc:latest", release.ManifestKindDbt, now)

	assert.Equal(t, "svc-a", sp.ServiceName())
	assert.Equal(t, "sha-abc", sp.ReleaseID())
	assert.Equal(t, "s3://bucket/svc-a/manifest.json", sp.ManifestS3Key())
	assert.Equal(t, "sha-abc:latest", sp.ImageTag())
	assert.Equal(t, release.ManifestKindDbt, sp.ManifestKind())
	assert.Equal(t, now, sp.UpdatedAt())
}
