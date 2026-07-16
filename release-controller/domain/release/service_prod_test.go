package release_test

import (
	"strings"
	"testing"
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
)

func TestNewServiceProd_AccessorsRoundTrip(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()
	sp := release.NewServiceProd("svc-a", "sha-abc", "s3://bucket/svc-a/manifest.json", "sha-abc:latest", now)

	assert.Equal(t, "svc-a", sp.ServiceName())
	assert.Equal(t, "sha-abc", sp.ReleaseID())
	assert.Equal(t, "s3://bucket/svc-a/manifest.json", sp.ManifestS3Key())
	assert.Equal(t, "sha-abc:latest", sp.ImageTag())
	assert.Equal(t, now, sp.UpdatedAt())
}

func TestNewServiceProd_HasNoRuntimeManifest(t *testing.T) {
	// NewServiceProd is the legacy/seed constructor: a pointer built through it
	// pins no runtime manifest, which is the state of every pre-existing row.
	sp := release.NewServiceProd("svc-a", "sha-abc", "s3://bucket/svc-a/manifest.json", "sha-abc:latest",
		time.Unix(1_000_000, 0).UTC())

	assert.Equal(t, pkgmodel.RuntimeManifestRef{}, sp.RuntimeManifest())
	assert.False(t, sp.RuntimeManifest().Complete())
}

func TestNewServiceProdWithRuntime_RoundTripsTheRef(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()
	ref := pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://bucket/svc-a/sha-abc/manifest.msgpack",
		RuntimeManifestSHA256:             "aa" + strings.Repeat("0", 62),
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "bb" + strings.Repeat("0", 62),
	}
	sp := release.NewServiceProdWithRuntime("svc-a", "sha-abc", "s3://bucket/svc-a/manifest.json",
		"sha-abc:latest", ref, now)

	assert.Equal(t, "svc-a", sp.ServiceName())
	assert.Equal(t, ref, sp.RuntimeManifest())
	assert.True(t, sp.RuntimeManifest().Complete())
}
