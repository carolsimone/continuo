package deploy_test

import (
	"testing"

	"github.com/carolsimone/continuo/executor-controller/domain/deploy"
	"github.com/stretchr/testify/assert"
)

func TestParseCacheURIs(t *testing.T) {
	assert.Equal(t, "s3://b/svc/parse-cache/tag1/partial_parse.msgpack",
		deploy.ParseCacheProdURI("b", "svc", "tag1"))
	assert.Equal(t, "s3://b/svc/rel-1/partial_parse.candidate.msgpack",
		deploy.ParseCacheCandidateURI("b", "svc", "rel-1"))
}
