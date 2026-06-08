package handlers_test

import (
	"testing"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
)

func TestCanonicalManifestKey(t *testing.T) {
	assert.Equal(t,
		"s3://my-bucket/service-2/rel-abc/manifest.json",
		handlers.CanonicalManifestKey("my-bucket", "service-2", "rel-abc"),
	)
}
