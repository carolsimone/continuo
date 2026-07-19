package artifacts_test

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/service/artifacts"
	"github.com/stretchr/testify/assert"
)

func TestParseCacheURIs(t *testing.T) {
	assert.Equal(t, "s3://b/svc/parse-cache/tag1/partial_parse.msgpack",
		artifacts.ParseCacheProdURI("b", "svc", "tag1"))
	assert.Equal(t, "s3://b/svc/rel-1/partial_parse.candidate.msgpack",
		artifacts.ParseCacheCandidateURI("b", "svc", "rel-1"))
}

func TestManifestURI(t *testing.T) {
	assert.Equal(t, "s3://b/svc/rel-1/manifest.json",
		artifacts.ManifestURI("b", "svc", "rel-1"))
}
