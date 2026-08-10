package handlers_test

import (
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
)

func TestCanonicalManifestKey_PerKindArtifactName(t *testing.T) {
	assert.Equal(t, "s3://b/svc/r1/manifest.json",
		handlers.CanonicalManifestKey("b", "svc", "r1", release.ManifestKindDbt))
	assert.Equal(t, "s3://b/svc-py/r1/contract.yaml",
		handlers.CanonicalManifestKey("b", "svc-py", "r1", release.ManifestKindPython))
}
